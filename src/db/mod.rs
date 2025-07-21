mod backend;
mod builder;

pub use backend::Backend;
pub use builder::DatabaseBuilder;
use builder::TABLE;
#[cfg(feature = "sqlite")]
use r2d2::Pool;
#[cfg(feature = "sqlite")]
use r2d2_sqlite::SqliteConnectionManager;
use sketches_ddsketch::DDSketch;
use std::{
    ops::Bound,
    sync::{atomic::AtomicU64, Arc, Mutex},
    time::Instant,
};

#[derive(Clone)]
pub enum GenericDatabase {
    Fjall {
        keyspace: fjall::TxKeyspace,
        db: fjall::TxPartition,
    },

    #[cfg(feature = "fjall_nightly")]
    FjallNightly {
        keyspace: fjall_nightly::TxKeyspace,
        db: fjall_nightly::TxPartition,
    },

    Sled(sled::Db),

    Redb(Arc<redb::Database>),

    Canopydb(Arc<canopydb::Database>),

    #[cfg(feature = "heed")]
    Heed {
        db: heed::Database<heed::types::Bytes, heed::types::Bytes>,
        env: heed::Env,
    },

    #[cfg(feature = "rocksdb")]
    RocksDb(Arc<rocksdb::OptimisticTransactionDB>),

    #[cfg(feature = "sqlite")]
    Sqlite(Pool<SqliteConnectionManager>),
}

#[derive(Clone)]
pub struct DatabaseWrapper {
    pub inner: GenericDatabase,
    /// Current size of the workload (key + value length) in bytes
    /// To get an accurate value, updates should be distinguished from inserts
    /// and deletes must know the size of the deleted value.
    pub workload_real_bytes: Arc<AtomicU64>,

    pub write_ops: Arc<AtomicU64>,
    pub write_latency: Arc<AtomicU64>,
    pub written_bytes: Arc<AtomicU64>,

    pub point_read_ops: Arc<AtomicU64>,
    pub point_read_latency: Arc<AtomicU64>,
    /// Number of bytes read in point reads (key + value length)
    pub point_read_bytes: Arc<AtomicU64>,

    pub range_ops: Arc<AtomicU64>,
    pub range_latency: Arc<AtomicU64>,
    /// Number of bytes read in range reads (key + value length)
    pub range_read_bytes: Arc<AtomicU64>,

    pub delete_ops: Arc<AtomicU64>,
    pub delete_latency: Arc<AtomicU64>,

    pub write_latency_histogram: Arc<Mutex<DDSketch>>,
    pub point_read_latency_histogram: Arc<Mutex<DDSketch>>,
    pub range_latency_histogram: Arc<Mutex<DDSketch>>,
}

impl std::ops::Deref for DatabaseWrapper {
    type Target = GenericDatabase;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DatabaseWrapper {
    fn report_scan(&self, total_bytes: u64, start: Instant) {
        let latency = start.elapsed().as_nanos() as u64;

        self.range_read_bytes
            .fetch_add(total_bytes, std::sync::atomic::Ordering::Relaxed);

        self.range_latency
            .fetch_add(latency, std::sync::atomic::Ordering::Relaxed);

        self.range_ops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        self.range_latency_histogram
            .lock()
            .unwrap()
            .add(latency as f64);
    }

    pub fn range_first(&self, range: (Bound<&[u8]>, Bound<&[u8]>)) -> Option<(Vec<u8>, Vec<u8>)> {
        let start = Instant::now();

        let v: Option<(Vec<u8>, Vec<u8>)> = match &self.inner {
            GenericDatabase::Fjall { db, keyspace } => {
                let read_tx = keyspace.read_tx();
                let mut iter = read_tx.range::<&[u8], _>(db, range);

                iter.next()
                    .transpose()
                    .unwrap()
                    .map(|(k, v)| (k.to_vec(), v.to_vec()))
            }

            #[cfg(feature = "fjall_nightly")]
            GenericDatabase::FjallNightly { db, keyspace } => {
                let read_tx = keyspace.read_tx();
                let mut iter = read_tx.range::<&[u8], _>(db, range);

                iter.next()
                    .transpose()
                    .unwrap()
                    .map(|(k, v)| (k.to_vec(), v.to_vec()))
            }
            GenericDatabase::Canopydb(db) => {
                let tx = db.begin_read().unwrap();
                let tree = tx.get_tree(b"default").unwrap().unwrap();

                let mut iter = tree.range::<&[u8]>(range).unwrap();

                iter.next()
                    .transpose()
                    .unwrap()
                    .map(|(k, v)| (k.to_vec(), v.to_vec()))
            }
            #[cfg(feature = "sqlite")]
            GenericDatabase::Sqlite(db) => {
                let (stmt, params) = sqlite_range(range, false);
                db.get()
                    .unwrap()
                    .prepare_cached(&stmt)
                    .unwrap()
                    .query_map(rusqlite::params_from_iter(params), |row| {
                        let k = row.get_ref(0).unwrap();
                        let v = row.get_ref(1).unwrap();
                        use rusqlite::types::ValueRef;
                        if let (ValueRef::Blob(k), ValueRef::Blob(v)) = (k, v) {
                            Ok((k.to_vec(), v.to_vec()))
                        } else {
                            unreachable!()
                        }
                    })
                    .unwrap()
                    .next()
                    .transpose()
                    .unwrap()
            }
            GenericDatabase::Redb(db) => {
                let tx = db.begin_read().unwrap();
                let tree = tx.open_table(TABLE).unwrap();
                let mut iter = tree.range::<&[u8]>(range).unwrap();
                iter.next()
                    .transpose()
                    .unwrap()
                    .map(|(k, v)| (k.value().to_vec(), v.value().to_vec()))
            }
            GenericDatabase::Sled(db) => {
                let mut iter = db.range::<&[u8], _>(range);

                iter.next()
                    .transpose()
                    .unwrap()
                    .map(|(k, v)| (k.to_vec(), v.to_vec()))
            }
            #[cfg(feature = "heed")]
            GenericDatabase::Heed { db, env } => {
                let tx = env.read_txn().unwrap();
                let mut iter = db.range(&tx, &range).unwrap();

                iter.next()
                    .transpose()
                    .unwrap()
                    .map(|(k, v)| (k.to_vec(), v.to_vec()))
            }
            #[cfg(feature = "rocksdb")]
            GenericDatabase::RocksDb(db) => rocksdb_range(range, false, db)
                .next()
                .map(|(k, v)| (k.into(), v.into())),
        };

        self.report_scan(
            v.as_ref().map(|(k, v)| k.len() + v.len()).unwrap_or(0) as u64,
            start,
        );

        v
    }

    pub fn prefix(&self, prefix: &[u8], rev: bool, take: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        let start = Instant::now();
        let mut sum_bytes = 0;

        let v = match &self.inner {
            #[cfg(feature = "fjall_nightly")]
            GenericDatabase::FjallNightly { db, keyspace } => {
                let read_tx = keyspace.read_tx();
                let iter = read_tx.prefix(db, prefix);

                if rev {
                    iter.rev()
                        .take(take)
                        .map(|kv| kv.unwrap())
                        .map(|(k, v)| (k.to_vec(), v.to_vec()))
                        .inspect(|(k, v)| {
                            sum_bytes += k.len() + v.len();
                        })
                        .collect()
                } else {
                    iter.take(take)
                        .map(|kv| kv.unwrap())
                        .map(|(k, v)| (k.to_vec(), v.to_vec()))
                        .inspect(|(k, v)| {
                            sum_bytes += k.len() + v.len();
                        })
                        .collect()
                }
            }

            GenericDatabase::Fjall { db, keyspace } => {
                let read_tx = keyspace.read_tx();
                let iter = read_tx.prefix(db, prefix);

                if rev {
                    iter.rev()
                        .take(take)
                        .map(|kv| kv.unwrap())
                        .map(|(k, v)| (k.to_vec(), v.to_vec()))
                        .inspect(|(k, v)| {
                            sum_bytes += k.len() + v.len();
                        })
                        .collect()
                } else {
                    iter.take(take)
                        .map(|kv| kv.unwrap())
                        .map(|(k, v)| (k.to_vec(), v.to_vec()))
                        .inspect(|(k, v)| {
                            sum_bytes += k.len() + v.len();
                        })
                        .collect()
                }
            }

            GenericDatabase::Sled(db) => {
                let iter = db.scan_prefix(prefix);

                if rev {
                    iter.rev()
                        .take(take)
                        .map(|kv| kv.unwrap())
                        .map(|(k, v)| (k.to_vec(), v.to_vec()))
                        .inspect(|(k, v)| {
                            sum_bytes += k.len() + v.len();
                        })
                        .collect()
                } else {
                    iter.take(take)
                        .map(|kv| kv.unwrap())
                        .map(|(k, v)| (k.to_vec(), v.to_vec()))
                        .inspect(|(k, v)| {
                            sum_bytes += k.len() + v.len();
                        })
                        .collect()
                }
            }

            #[cfg(feature = "rocksdb")]
            GenericDatabase::RocksDb(db) => {
                let upper_bound = get_upper_bound(prefix);
                let upper_bound = upper_bound
                    .as_ref()
                    .map_or(Bound::Unbounded, |b| Bound::Excluded(b.as_slice()));

                /* let range = db
                .range::<&[u8]>((Bound::Included(prefix), upper_bound))
                .unwrap(); */

                let range = rocksdb_range((Bound::Included(prefix), upper_bound), rev, &db);

                range
                    .take(take)
                    .map(|(k, v)| (k.to_vec(), v.to_vec()))
                    .inspect(|(k, v)| {
                        sum_bytes += k.len() + v.len();
                    })
                    .collect()
            }

            GenericDatabase::Redb(db) => {
                let upper_bound = get_upper_bound(prefix).unwrap();

                let tx = db.begin_read().unwrap();
                let table = tx.open_table(TABLE).unwrap();
                let iter = table.range(prefix..&upper_bound).unwrap();

                if rev {
                    iter.map(|guard| guard.unwrap())
                        .map(|(k, v)| (k.value().to_vec(), v.value().to_vec()))
                        .inspect(|(k, v)| {
                            sum_bytes += k.len() + v.len();
                        })
                        .collect()
                } else {
                    iter.rev()
                        .map(|guard| guard.unwrap())
                        .map(|(k, v)| (k.value().to_vec(), v.value().to_vec()))
                        .inspect(|(k, v)| {
                            sum_bytes += k.len() + v.len();
                        })
                        .collect()
                }
            }

            GenericDatabase::Heed { db, env } => {
                let upper_bound = get_upper_bound(prefix).unwrap();
                let range: (Bound<&[u8]>, Bound<&[u8]>) =
                    (Bound::Included(prefix), Bound::Excluded(&*upper_bound));

                let tx = env.read_txn().unwrap();

                if rev {
                    db.rev_range(&tx, &range)
                        .unwrap()
                        .map(|x| x.unwrap())
                        .map(|(k, v)| (k.to_vec(), v.to_vec()))
                        .inspect(|(k, v)| {
                            sum_bytes += k.len() + v.len();
                        })
                        .collect()
                } else {
                    db.range(&tx, &range)
                        .unwrap()
                        .map(|x| x.unwrap())
                        .map(|(k, v)| (k.to_vec(), v.to_vec()))
                        .inspect(|(k, v)| {
                            sum_bytes += k.len() + v.len();
                        })
                        .collect()
                }
            }

            _ => unimplemented!(),
        };

        self.report_scan(sum_bytes as u64, start);

        v
    }

    pub fn prefix_len(&self, prefix: &[u8], rev: bool, take: usize) -> usize {
        let start = Instant::now();
        let mut sum_bytes = 0;

        let v = match &self.inner {
            #[cfg(feature = "sqlite")]
            GenericDatabase::Sqlite(_db) => {
                let upper_bound = get_upper_bound(prefix);
                let upper_bound = upper_bound
                    .as_ref()
                    .map_or(Bound::Unbounded, |b| Bound::Excluded(b.as_slice()));
                return self.range_len((Bound::Included(prefix), upper_bound), rev, take);
            }

            #[cfg(feature = "fjall_nightly")]
            GenericDatabase::FjallNightly { db, keyspace } => {
                let read_tx = keyspace.read_tx();
                let iter = read_tx.prefix(db, prefix);

                if rev {
                    iter.rev()
                        .take(take)
                        .map(|kv| kv.unwrap())
                        .map(|(k, v)| {
                            sum_bytes += k.len() + v.len();
                        })
                        .count()
                } else {
                    iter.take(take)
                        .map(|kv| kv.unwrap())
                        .map(|(k, v)| {
                            sum_bytes += k.len() + v.len();
                        })
                        .count()
                }
            }
            GenericDatabase::Fjall { db, keyspace } => {
                let read_tx = keyspace.read_tx();
                let iter = read_tx.prefix(db, prefix);

                if rev {
                    iter.rev()
                        .take(take)
                        .map(|kv| kv.unwrap())
                        .map(|(k, v)| {
                            sum_bytes += k.len() + v.len();
                        })
                        .count()
                } else {
                    iter.take(take)
                        .map(|kv| kv.unwrap())
                        .map(|(k, v)| {
                            sum_bytes += k.len() + v.len();
                        })
                        .count()
                }
            }
            GenericDatabase::Sled(db) => {
                let iter = db.scan_prefix(prefix);

                if rev {
                    iter.rev()
                        .take(take)
                        .map(|kv| kv.unwrap())
                        .map(|(k, v)| {
                            sum_bytes += k.len() + v.len();
                        })
                        .count()
                } else {
                    iter.take(take)
                        .map(|kv| kv.unwrap())
                        .map(|(k, v)| {
                            sum_bytes += k.len() + v.len();
                        })
                        .count()
                }
            }
            GenericDatabase::Redb(_) => {
                let upper_bound = get_upper_bound(prefix);
                let upper_bound = upper_bound
                    .as_ref()
                    .map_or(Bound::Unbounded, |b| Bound::Excluded(b.as_slice()));
                return self.range_len((Bound::Included(prefix), upper_bound), rev, take);
            }

            #[cfg(feature = "heed")]
            GenericDatabase::Heed { db, env } => {
                let tx = env.read_txn().unwrap();

                if rev {
                    let iter = db.rev_prefix_iter(&tx, prefix).unwrap();
                    iter.take(take)
                        .map(|kv| kv.unwrap())
                        .map(|(k, v)| {
                            sum_bytes += k.len() + v.len();
                        })
                        .count()
                } else {
                    let iter = db.prefix_iter(&tx, prefix).unwrap();
                    iter.take(take)
                        .map(|kv| kv.unwrap())
                        .map(|(k, v)| {
                            sum_bytes += k.len() + v.len();
                        })
                        .count()
                }
            }

            #[cfg(feature = "rocksdb")]
            GenericDatabase::RocksDb(_) => {
                let upper_bound = get_upper_bound(prefix);
                let upper_bound = upper_bound
                    .as_ref()
                    .map_or(Bound::Unbounded, |b| Bound::Excluded(b.as_slice()));
                return self.range_len((Bound::Included(prefix), upper_bound), rev, take);
            }
            GenericDatabase::Canopydb(db) => {
                let tx = db.begin_read().unwrap();
                let tree = tx.get_tree(b"default").unwrap().unwrap();

                let range = tree.prefix(&prefix).unwrap();

                if rev {
                    range
                        .rev()
                        .take(take)
                        .map(|kv| kv.unwrap())
                        .map(|(k, v)| {
                            sum_bytes += k.len() + v.len();
                        })
                        .count()
                } else {
                    range
                        .take(take)
                        .map(|kv| kv.unwrap())
                        .map(|(k, v)| {
                            sum_bytes += k.len() + v.len();
                        })
                        .count()
                }
            }
        };

        self.report_scan(sum_bytes as u64, start);

        v
    }

    pub fn range_len(&self, range: (Bound<&[u8]>, Bound<&[u8]>), rev: bool, take: usize) -> usize {
        let start = Instant::now();
        let mut sum_bytes = 0;

        let v = match &self.inner {
            #[cfg(feature = "sqlite")]
            GenericDatabase::Sqlite(db) => {
                let (stmt, params) = sqlite_range(range, rev);
                db.get()
                    .unwrap()
                    .prepare_cached(&stmt)
                    .unwrap()
                    .query_map(rusqlite::params_from_iter(params), |row| {
                        let k = row.get_ref(0).unwrap();
                        let v = row.get_ref(1).unwrap();
                        use rusqlite::types::ValueRef;
                        if let (ValueRef::Blob(k), ValueRef::Blob(v)) = (k, v) {
                            sum_bytes += k.len() + v.len();
                        } else {
                            unreachable!()
                        }
                        Ok(())
                    })
                    .unwrap()
                    .take(take)
                    .count()
            }

            #[cfg(feature = "fjall_nightly")]
            GenericDatabase::FjallNightly { db, keyspace } => {
                let read_tx = keyspace.read_tx();
                let iter = read_tx.range::<&[u8], _>(db, range);

                if rev {
                    iter.rev()
                        .take(take)
                        .map(|kv| kv.unwrap())
                        .map(|(k, v)| {
                            sum_bytes += k.len() + v.len();
                        })
                        .count()
                } else {
                    iter.take(take)
                        .map(|kv| kv.unwrap())
                        .map(|(k, v)| {
                            sum_bytes += k.len() + v.len();
                        })
                        .count()
                }
            }
            GenericDatabase::Fjall { db, keyspace } => {
                let read_tx = keyspace.read_tx();
                let iter = read_tx.range::<&[u8], _>(db, range);

                if rev {
                    iter.rev()
                        .take(take)
                        .map(|kv| kv.unwrap())
                        .map(|(k, v)| {
                            sum_bytes += k.len() + v.len();
                        })
                        .count()
                } else {
                    iter.take(take)
                        .map(|kv| kv.unwrap())
                        .map(|(k, v)| {
                            sum_bytes += k.len() + v.len();
                        })
                        .count()
                }
            }
            GenericDatabase::Sled(db) => {
                let iter = db.range::<&[u8], _>(range);

                if rev {
                    iter.rev()
                        .take(take)
                        .map(|kv| kv.unwrap())
                        .map(|(k, v)| {
                            sum_bytes += k.len() + v.len();
                        })
                        .count()
                } else {
                    iter.take(take)
                        .map(|kv| kv.unwrap())
                        .map(|(k, v)| {
                            sum_bytes += k.len() + v.len();
                        })
                        .count()
                }
            }
            GenericDatabase::Redb(db) => {
                let tx = db.begin_read().unwrap();

                let table = tx.open_table(TABLE).unwrap();

                let iter = table.range::<&[u8]>(range).unwrap();

                if rev {
                    iter.rev()
                        .map(|x| x.unwrap())
                        .take(take)
                        .map(|(k, v)| {
                            sum_bytes += k.value().len() + v.value().len();
                        })
                        .count()
                } else {
                    iter.map(|x| x.unwrap())
                        .take(take)
                        .map(|(k, v)| {
                            sum_bytes += k.value().len() + v.value().len();
                        })
                        .count()
                }
            }

            #[cfg(feature = "heed")]
            GenericDatabase::Heed { db, env } => {
                let tx = env.read_txn().unwrap();

                if rev {
                    db.rev_range(&tx, &range)
                        .unwrap()
                        .take(take)
                        .map(|kv| kv.unwrap())
                        .map(|(k, v)| {
                            sum_bytes += k.len() + v.len();
                        })
                        .count()
                } else {
                    db.range(&tx, &range)
                        .unwrap()
                        .take(take)
                        .map(|kv| kv.unwrap())
                        .map(|(k, v)| {
                            sum_bytes += k.len() + v.len();
                        })
                        .count()
                }
            }

            #[cfg(feature = "rocksdb")]
            GenericDatabase::RocksDb(db) => rocksdb_range(range, rev, db)
                .take(take)
                .map(|(k, v)| {
                    sum_bytes += k.len() + v.len();
                })
                .count(),

            GenericDatabase::Canopydb(db) => {
                let tx = db.begin_read().unwrap();
                let tree = tx.get_tree(b"default").unwrap().unwrap();

                let range = tree.range::<&[u8]>(range).unwrap();

                if rev {
                    range
                        .rev()
                        .take(take)
                        .map(|kv| kv.unwrap())
                        .map(|(k, v)| {
                            sum_bytes += k.len() + v.len();
                        })
                        .count()
                } else {
                    range
                        .take(take)
                        .map(|kv| kv.unwrap())
                        .map(|(k, v)| {
                            sum_bytes += k.len() + v.len();
                        })
                        .count()
                }
            }
        };

        self.report_scan(sum_bytes as u64, start);

        v
    }

    pub fn fragmented_bytes(&self) -> usize {
        /* match &self.inner {
            // TODO: expensive?!
            /* GenericDatabase::Redb(db) => {
                use redb::ReadableTableMetadata;

                let tx = db.begin_read().unwrap();
                let table = tx.open_table(TABLE).unwrap();
                table.stats().unwrap().fragmented_bytes() as usize
            } */
            _ => 0,
        } */
        0
    }

    pub fn tree_height(&self) -> usize {
        match &self.inner {
            // TODO: fjall: non-vacant levels
            GenericDatabase::Redb(db) => {
                use redb::ReadableTableMetadata;

                // TODO: too expensive!!! memoize??
                let tx = db.begin_read().unwrap();
                let table = tx.open_table(TABLE).unwrap();
                table.stats().unwrap().tree_height() as usize
            }

            #[cfg(feature = "heed")]
            GenericDatabase::Heed { db, env } => {
                let tx = env.read_txn().unwrap();
                db.stat(&tx).unwrap().depth as usize
            }
            GenericDatabase::Canopydb(db) => {
                let tx = db.begin_read().unwrap();
                let tree = tx.get_tree(b"default").unwrap().unwrap();
                tree.height()
            }
            _ => 0,
        }
    }

    // TODO: curr cache size

    pub fn write_buffer_size(&self) -> u64 {
        match &self.inner {
            GenericDatabase::Fjall { keyspace, .. } => keyspace.write_buffer_size(),

            #[cfg(feature = "fjall_nightly")]
            GenericDatabase::FjallNightly { keyspace, .. } => keyspace.write_buffer_size(),

            #[cfg(feature = "rocksdb")]
            GenericDatabase::RocksDb(db) => db
                .property_int_value("rocksdb.size-all-mem-tables")
                .unwrap_or_default()
                .unwrap_or_default(),

            _ => 0,
        }
    }

    pub fn bloom_filter_size(&self) -> usize {
        match &self.inner {
            GenericDatabase::Fjall { db, .. } => {
                use fjall::AbstractTree;

                db.inner().tree.bloom_filter_size()
            }

            #[cfg(feature = "fjall_nightly")]
            GenericDatabase::FjallNightly { db, .. } => {
                use fjall_nightly::AbstractTree;

                db.inner().tree.pinned_bloom_filter_size()
            }

            _ => 0,
        }
    }

    pub fn block_index_size(&self) -> usize {
        match &self.inner {
            #[cfg(feature = "fjall_nightly")]
            GenericDatabase::FjallNightly { db, .. } => {
                use fjall_nightly::AbstractTree;

                db.inner().tree.pinned_block_index_size()
            }

            _ => 0,
        }
    }

    pub fn l0_runs(&self) -> usize {
        match &self.inner {
            GenericDatabase::Fjall { db, .. } => {
                use fjall::AbstractTree;

                db.inner().tree.l0_run_count()
            }

            #[cfg(feature = "fjall_nightly")]
            GenericDatabase::FjallNightly { db, .. } => {
                use fjall_nightly::AbstractTree;

                db.inner().tree.l0_run_count()
            }

            _ => 0,
        }
    }

    pub fn avg_l0_segment_creation_date_us(&self) -> u128 {
        match &self.inner {
            GenericDatabase::Fjall { db, .. } => {
                let tree = match &db.inner().tree {
                    fjall::AnyTree::Blob(tree) => &tree.index.0,
                    fjall::AnyTree::Standard(tree) => tree,
                };

                let first_level = &tree.levels.read().unwrap();
                let first_level = &first_level.levels.first().unwrap().segments;
                let count = first_level.len() as u128;

                let sum: u128 = first_level.iter().map(|x| x.metadata.created_at).sum();

                sum.checked_div(count).unwrap_or_default()
            }

            /*   #[cfg(feature = "fjall_nightly")]
            GenericDatabase::FjallNightly { db, .. } => {
                let tree = match &db.inner().tree {
                    fjall_nightly::AnyTree::Blob(tree) => &tree.index.0,
                    fjall_nightly::AnyTree::Standard(tree) => tree,
                };

                let first_level = &tree.levels.read().unwrap();
                let first_level = &first_level
                    .levels
                    .first()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>();

                let count = first_level.len() as u128;

                let sum: u128 = first_level
                    .iter()
                    .map(|x| x.metadata.created_at as u128)
                    .sum();

                sum.checked_div(count).unwrap_or_default()
            } */
            _ => 0,
        }
    }

    pub fn time_compacting_us(&self) -> u64 {
        match &self.inner {
            GenericDatabase::Fjall { keyspace, .. } => {
                keyspace.inner().time_compacting().as_micros() as u64
            }
            #[cfg(feature = "fjall_nightly")]
            GenericDatabase::FjallNightly { keyspace, .. } => {
                keyspace.inner().time_compacting().as_micros() as u64
            }
            _ => 0,
        }
    }

    pub fn active_compactions(&self) -> usize {
        match &self.inner {
            GenericDatabase::Fjall { keyspace, .. } => keyspace.inner().active_compactions(),

            #[cfg(feature = "fjall_nightly")]
            GenericDatabase::FjallNightly { keyspace, .. } => keyspace.inner().active_compactions(),

            #[cfg(feature = "rocksdb")]
            GenericDatabase::RocksDb(db) => db
                .property_int_value("rocksdb.num-running-compactions")
                .unwrap_or_default()
                .unwrap_or_default() as usize,

            _ => 0,
        }
    }

    pub fn blob_file_count(&self) -> usize {
        match &self.inner {
            GenericDatabase::Fjall { db, .. } => {
                use fjall::AbstractTree;

                db.inner().tree.blob_file_count()
            }
            #[cfg(feature = "fjall_nightly")]
            GenericDatabase::FjallNightly { db, .. } => {
                use fjall_nightly::AbstractTree;

                db.inner().tree.blob_file_count()
            }

            #[cfg(feature = "rocksdb")]
            GenericDatabase::RocksDb(db) => db
                .property_int_value("rocksdb.num-blob-files")
                .unwrap()
                .unwrap() as usize,

            _ => 0,
        }
    }

    pub fn disk_segment_count(&self) -> usize {
        match &self.inner {
            GenericDatabase::Fjall { db, .. } => {
                use fjall::AbstractTree;

                db.inner().tree.segment_count()
            }

            #[cfg(feature = "fjall_nightly")]
            GenericDatabase::FjallNightly { db, .. } => {
                use fjall_nightly::AbstractTree;

                db.inner().tree.segment_count()
            }

            #[cfg(feature = "rocksdb")]
            GenericDatabase::RocksDb(db) => {
                let mut total_sst_files = 0;

                for level in 0..7 {
                    let prop = format!("rocksdb.num-files-at-level{}", level);

                    if let Ok(Some(val)) = db.property_int_value(&prop) {
                        total_sst_files += val as usize;
                    }
                }

                total_sst_files
            }
            _ => 0,
        }
    }

    pub fn journal_size(&self) -> u64 {
        match &self.inner {
            GenericDatabase::Fjall { keyspace, .. } => keyspace.inner().journal_disk_space(),

            #[cfg(feature = "fjall_nightly")]
            GenericDatabase::FjallNightly { keyspace, .. } => keyspace.inner().journal_disk_space(),

            _ => 0,
        }
    }

    pub fn journal_count(&self) -> usize {
        match &self.inner {
            GenericDatabase::Fjall { keyspace, .. } => keyspace.journal_count(),

            #[cfg(feature = "fjall_nightly")]
            GenericDatabase::FjallNightly { keyspace, .. } => keyspace.journal_count(),

            _ => 0,
        }
    }

    /* pub fn scan_all(&self) -> usize {
        let len = match &self.inner {
            GenericDatabase::Fjall { db,.. } => db.iter().count(),
            _ => unimplemented!(),
        };

        len
    } */

    pub fn len(&self) -> usize {
        match &self.inner {
            GenericDatabase::Fjall { db, .. } => db.inner().len().unwrap(),

            #[cfg(feature = "fjall_nightly")]
            GenericDatabase::FjallNightly { db, .. } => db.inner().len().unwrap(),

            GenericDatabase::Canopydb(db) => {
                let tx = db.begin_read().unwrap();
                let tree = tx.get_tree(b"default").unwrap().unwrap();
                tree.len() as usize
            }

            _ => unimplemented!(),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let start = Instant::now();

        let report_latency = || {
            let point_read_latency = start.elapsed().as_nanos() as u64;

            self.point_read_latency
                .fetch_add(point_read_latency, std::sync::atomic::Ordering::Relaxed);

            self.point_read_ops
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            self.point_read_latency_histogram
                .lock()
                .unwrap()
                .add(point_read_latency as f64);
        };

        let item = match &self.inner {
            #[cfg(feature = "sqlite")]
            GenericDatabase::Sqlite(db) => {
                let value = db
                    .get()
                    .unwrap()
                    .prepare_cached("SELECT value FROM data WHERE key = ?")
                    .unwrap()
                    .query_row([key], |row| Ok(row.get(0).unwrap()));

                report_latency();

                match value {
                    Ok(row) => Some(row),
                    Err(e) => {
                        if e == rusqlite::Error::QueryReturnedNoRows {
                            None
                        } else {
                            panic!("{e:?}");
                        }
                    }
                }
            }

            #[cfg(feature = "rocksdb")]
            GenericDatabase::RocksDb(db) => {
                let value = db
                    .get_opt(key, &{
                        // NOTE: For now, disable checksum checks
                        let mut opts = rocksdb::ReadOptions::default();
                        opts.set_verify_checksums(false);
                        opts
                    })
                    .unwrap();
                report_latency();
                value
            }

            GenericDatabase::Fjall { db, .. } => {
                let item = db.get(key).unwrap();
                report_latency();
                item.map(|x| x.to_vec())
            }

            #[cfg(feature = "fjall_nightly")]
            GenericDatabase::FjallNightly { db, .. } => {
                let item = db.get(key).unwrap();
                report_latency();
                item.map(|x| x.to_vec())
            }

            GenericDatabase::Sled(db) => {
                let item = db.get(key).unwrap();
                report_latency();
                item.map(|x| x.to_vec())
            }

            GenericDatabase::Redb(db) => {
                let read_txn = db.begin_read().unwrap();
                let table = read_txn.open_table(TABLE).unwrap();

                table.get(key).unwrap().map(|x| {
                    let value = x.value();
                    report_latency();
                    value.to_vec()
                })
            }

            #[cfg(feature = "heed")]
            GenericDatabase::Heed { db, env } => {
                let read_txn = env.read_txn().unwrap();
                let value = db.get(&read_txn, key).unwrap();
                report_latency();
                value.map(ToOwned::to_owned)
            }

            GenericDatabase::Canopydb(db) => {
                let tx = db.begin_read().unwrap();
                let tree = tx.get_tree(b"default").unwrap().unwrap();

                let value = tree.get(key).unwrap();
                report_latency();
                value.map(|x| x.to_vec())
            }
        };
        self.point_read_bytes.fetch_add(
            key.len() as u64 + item.as_ref().map_or(0, |v| v.len() as u64),
            std::sync::atomic::Ordering::Relaxed,
        );

        item
    }

    /// Ingest a batch of items into the database.
    /// Assumes that the keys provided are unique and are not in the database, for statistics purposes.
    /// Some databases (e.g., Fjall) may not support ingesting items out of order.
    pub fn ingest(&self, items: impl Iterator<Item = (Vec<u8>, Vec<u8>)>) {
        let start = Instant::now();
        let mut count = 0u64;
        let mut last_start = start;

        let mut on_bytes_written = |k: &[u8], v: &[u8]| {
            count += 1;
            let total_bytes = (k.len() + v.len()) as u64;
            let now = Instant::now();
            let elapsed = now.duration_since(last_start);
            last_start = now;
            self.write_latency.fetch_add(
                elapsed.as_nanos() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
            self.written_bytes
                .fetch_add(total_bytes, std::sync::atomic::Ordering::Relaxed);
            self.workload_real_bytes
                .fetch_add(total_bytes, std::sync::atomic::Ordering::Relaxed);
            self.write_ops
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        };

        match &self.inner {
            #[cfg(feature = "sqlite")]
            GenericDatabase::Sqlite(db) => {
                let db = db.get().unwrap();
                db.execute("BEGIN IMMEDIATE", []).unwrap();
                let mut stmt = db
                    .prepare_cached("INSERT INTO data (key, value) VALUES (?, ?)")
                    .unwrap();

                for (key, value) in items {
                    stmt.execute(rusqlite::params![key, value]).unwrap();
                    on_bytes_written(&key, &value);
                }
                db.execute("COMMIT", []).unwrap();

                // NOTE: Durability is controlled by pragma in load()
            }

            #[cfg(feature = "rocksdb")]
            GenericDatabase::RocksDb(db) => {
                for (key, value) in items {
                    db.put(&key, &value).unwrap();
                    on_bytes_written(&key, &value);
                }
                db.flush_wal(true).unwrap();
            }
            GenericDatabase::Fjall { db, .. } => {
                db.inner()
                    .ingest(items.map(|(k, v)| {
                        on_bytes_written(&k, &v);
                        (k, v)
                    }))
                    .unwrap();
            }
            #[cfg(feature = "fjall_nightly")]
            GenericDatabase::FjallNightly { db, .. } => {
                db.inner()
                    .ingest(items.map(|(k, v)| {
                        on_bytes_written(&k, &v);
                        (k, v)
                    }))
                    .unwrap();
            }
            GenericDatabase::Sled(db) => {
                for (key, value) in items {
                    db.insert(&key, &*value).unwrap();
                    on_bytes_written(&key, &value);
                }
                db.flush().unwrap();
            }
            GenericDatabase::Redb(db) => {
                let write_txn = db.begin_write().unwrap();
                {
                    let mut table = write_txn.open_table(TABLE).unwrap();

                    for (key, value) in items {
                        table.insert(&*key, &*value).unwrap();
                        on_bytes_written(&key, &value);
                    }
                }
                write_txn.commit().unwrap();
            }
            #[cfg(feature = "heed")]
            GenericDatabase::Heed { db, env } => {
                let mut write_txn = env.write_txn().unwrap();
                {
                    for (key, value) in items {
                        db.put_with_flags(&mut write_txn, heed::PutFlags::APPEND, &key, &value)
                            .unwrap();
                        on_bytes_written(&key, &value);
                    }
                }
                write_txn.commit().unwrap();
            }
            GenericDatabase::Canopydb(db) => {
                let write_txn = db.begin_write().unwrap();
                {
                    let mut tree = write_txn.get_tree(b"default").unwrap().unwrap();

                    for (key, value) in items {
                        tree.insert(&key, &value).unwrap();
                        on_bytes_written(&key, &value);
                    }
                }
                write_txn.commit().unwrap();
            }
        }

        log::info!("Ingested {count} initial items in {:?}", start.elapsed());
    }

    pub fn flush(&self) {
        match &self.inner {
            GenericDatabase::Fjall { keyspace, .. } => {
                keyspace.persist(fjall::PersistMode::SyncAll).unwrap();
            }

            #[cfg(feature = "fjall_nightly")]
            GenericDatabase::FjallNightly { keyspace, .. } => {
                keyspace
                    .persist(fjall_nightly::PersistMode::SyncAll)
                    .unwrap();
            }

            #[cfg(feature = "rocksdb")]
            GenericDatabase::RocksDb(db) => db.flush_wal(true).unwrap(),

            GenericDatabase::Sled(db) => {
                db.flush().unwrap();
            }

            GenericDatabase::Redb(_) => {}

            #[cfg(feature = "heed")]
            GenericDatabase::Heed { .. } => {}

            _ => unimplemented!(),
        }
    }

    pub fn insert(&self, key: &[u8], value: &[u8], durable: bool, increment_workload_size: bool) {
        let start = Instant::now();

        match &self.inner {
            #[cfg(feature = "sqlite")]
            GenericDatabase::Sqlite(db) => {
                db.get()
                    .unwrap()
                    .execute("INSERT INTO data (key, value) VALUES (?, ?)", (key, value))
                    .unwrap();

                // NOTE: Durability is controlled by pragma in load()
            }

            GenericDatabase::Fjall { keyspace, db } => {
                db.insert(key, value).unwrap();

                keyspace
                    .persist(if durable {
                        // NOTE: RocksDB uses fsyncdata by default, too
                        fjall::PersistMode::SyncData
                    } else {
                        fjall::PersistMode::Buffer
                    })
                    .unwrap();
            }
            #[cfg(feature = "fjall_nightly")]
            GenericDatabase::FjallNightly { keyspace, db } => {
                db.insert(key, value).unwrap();

                keyspace
                    .persist(if durable {
                        // NOTE: RocksDB uses fsyncdata by default, too
                        fjall_nightly::PersistMode::SyncData
                    } else {
                        fjall_nightly::PersistMode::Buffer
                    })
                    .unwrap();
            }
            GenericDatabase::Sled(db) => {
                db.insert(key, value).unwrap();

                if durable {
                    db.flush().unwrap();
                }
            }
            GenericDatabase::Redb(db) => {
                use redb::Durability::{Eventual, Immediate};

                let mut write_txn = db.begin_write().unwrap();

                write_txn.set_durability(if durable { Immediate } else { Eventual });

                {
                    let mut table = write_txn.open_table(TABLE).unwrap();
                    table.insert(key, value).unwrap();
                }
                write_txn.commit().unwrap();
            }
            #[cfg(feature = "heed")]
            GenericDatabase::Heed { env, db } => {
                let mut wtxn = env.write_txn().unwrap();
                db.put(&mut wtxn, key, value).unwrap();
                wtxn.commit().unwrap();
            }
            #[cfg(feature = "rocksdb")]
            GenericDatabase::RocksDb(db) => {
                // TODO: add option to write through transactions
                db.put(key, value).unwrap();
                db.flush_wal(durable).unwrap();
            }
            GenericDatabase::Canopydb(db) => {
                let write_txn = db.begin_write().unwrap();
                {
                    let mut tree = write_txn.get_tree(b"default").unwrap().unwrap();
                    tree.insert(key, value).unwrap();
                }
                write_txn.commit_with(durable).unwrap();
            }
        }

        let written_latency = start.elapsed().as_nanos() as u64;

        self.write_latency
            .fetch_add(written_latency, std::sync::atomic::Ordering::Relaxed);

        self.write_ops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        self.written_bytes.fetch_add(
            (key.len() + value.len()) as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        self.write_latency_histogram
            .lock()
            .unwrap()
            .add(written_latency as f64);

        if increment_workload_size {
            self.workload_real_bytes.fetch_add(
                (key.len() + value.len()) as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
        }
    }

    // TODO: this should probably be a configurable option `use_remove_unique` and then all workloads use
    // the plain remove function. This way workloads can test both kinds.
    pub fn remove_unique(&self, key: &[u8], durable: bool, decrement_workload_size: Option<u64>) {
        match &self.inner {
            /* #[cfg(feature = "fjall_nightly")]
            GenericDatabase::FjallNightly { keyspace, db } => {
                // TODO: remove_weak
                db.remove(key).unwrap();

                keyspace
                    .persist(if durable {
                        // NOTE: RocksDB uses fsyncdata by default, too
                        fjall_nightly::PersistMode::SyncData
                    } else {
                        fjall_nightly::PersistMode::Buffer
                    })
                    .unwrap();

                if let Some(decrement_workload_size) = decrement_workload_size {
                    self.workload_real_bytes.fetch_sub(
                        decrement_workload_size,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                }
            } */
            _ => {
                self.remove(key, durable, decrement_workload_size);
            }
        }
    }

    pub fn remove(&self, key: &[u8], durable: bool, decrement_workload_size: Option<u64>) {
        let start = Instant::now();

        match &self.inner {
            #[cfg(feature = "sqlite")]
            GenericDatabase::Sqlite(_db) => {
                unimplemented!()
            }

            GenericDatabase::Fjall { keyspace, db } => {
                // TODO: add option to remove through transactions
                db.remove(key).unwrap();

                keyspace
                    .persist(if durable {
                        // NOTE: RocksDB uses fsyncdata by default, too
                        fjall::PersistMode::SyncData
                    } else {
                        fjall::PersistMode::Buffer
                    })
                    .unwrap();
            }
            #[cfg(feature = "fjall_nightly")]
            GenericDatabase::FjallNightly { keyspace, db } => {
                db.remove(key).unwrap();

                keyspace
                    .persist(if durable {
                        // NOTE: RocksDB uses fsyncdata by default, too
                        fjall_nightly::PersistMode::SyncData
                    } else {
                        fjall_nightly::PersistMode::Buffer
                    })
                    .unwrap();
            }
            GenericDatabase::Sled(db) => {
                db.remove(key).unwrap();

                if durable {
                    db.flush().unwrap();
                }
            }
            GenericDatabase::Redb(db) => {
                use redb::Durability::{Eventual, Immediate};

                let mut write_txn = db.begin_write().unwrap();

                write_txn.set_durability(if durable { Immediate } else { Eventual });

                {
                    let mut table = write_txn.open_table(TABLE).unwrap();
                    table.remove(key).unwrap();
                }
                write_txn.commit().unwrap();
            }
            #[cfg(feature = "heed")]
            GenericDatabase::Heed { env, db } => {
                let mut wtxn = env.write_txn().unwrap();
                db.delete(&mut wtxn, key).unwrap();
                wtxn.commit().unwrap();
            }
            #[cfg(feature = "rocksdb")]
            GenericDatabase::RocksDb(db) => {
                // TODO: add option to write through transactions
                db.delete(key).unwrap();
                db.flush_wal(durable).unwrap();
            }
            GenericDatabase::Canopydb(db) => {
                let write_txn = db.begin_write().unwrap();
                {
                    let mut tree = write_txn.get_tree(b"default").unwrap().unwrap();
                    tree.delete(key).unwrap();
                }
                write_txn.commit_with(durable).unwrap();
            }
        }

        if let Some(decrement_workload_size) = decrement_workload_size {
            self.workload_real_bytes.fetch_sub(
                decrement_workload_size,
                std::sync::atomic::Ordering::Relaxed,
            );
        }

        let delete_latency = start.elapsed().as_nanos() as u64;

        self.delete_latency
            .fetch_add(delete_latency, std::sync::atomic::Ordering::Relaxed);

        self.delete_ops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(feature = "rocksdb")]
fn rocksdb_range<'a>(
    range: (Bound<&'a [u8]>, Bound<&'a [u8]>),
    rev: bool,
    db: &'a rocksdb::OptimisticTransactionDB,
) -> impl Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'a {
    let (start, end) = if rev {
        (range.1, range.0)
    } else {
        (range.0, range.1)
    };

    let it_mode = match start {
        Bound::Included(x) | Bound::Excluded(x) => rocksdb::IteratorMode::From(
            x,
            if rev {
                rocksdb::Direction::Reverse
            } else {
                rocksdb::Direction::Forward
            },
        ),
        Bound::Unbounded if rev => rocksdb::IteratorMode::End,
        Bound::Unbounded => rocksdb::IteratorMode::Start,
    };

    db.iterator_opt(it_mode, {
        // NOTE: For now, disable checksum checks
        let mut opts = rocksdb::ReadOptions::default();
        opts.set_verify_checksums(false);
        opts
    })
    .map(|kv| kv.unwrap())
    .enumerate()
    .filter(move |(i, (k, _v))| {
        if *i != 0 {
            return true;
        }
        // skip the first element if it's an excluded start
        if let Bound::Excluded(x) = start {
            if rev {
                &k[..] < x
            } else {
                &k[..] > x
            }
        } else {
            true
        }
    })
    .take_while(move |(_, (k, _v))| match end {
        Bound::Included(x) => {
            if rev {
                &k[..] >= x
            } else {
                &k[..] <= x
            }
        }
        Bound::Excluded(x) => {
            if rev {
                &k[..] > x
            } else {
                &k[..] < x
            }
        }
        Bound::Unbounded => true,
    })
    .map(|(_, (k, v))| (k, v))
}

#[cfg(feature = "sqlite")]
fn sqlite_range<'a>(
    range: (Bound<&'a [u8]>, Bound<&'a [u8]>),
    rev: bool,
) -> (String, Vec<&'a [u8]>) {
    let where_clause: &str = match range {
        (Bound::Included(_), Bound::Included(_)) => "WHERE key >= ?1 AND key <= ?2",
        (Bound::Included(_), Bound::Excluded(_)) => "WHERE key >= ?1 AND key < ?2",
        (Bound::Excluded(_), Bound::Included(_)) => "WHERE key > ?1 AND key <= ?2",
        (Bound::Excluded(_), Bound::Excluded(_)) => "WHERE key > ?1 AND key < ?2",
        (Bound::Unbounded, Bound::Included(_)) => "WHERE key <= ?1",
        (Bound::Included(_), Bound::Unbounded) => "WHERE key >= ?1",
        (Bound::Unbounded, Bound::Excluded(_)) => "WHERE key < ?1",
        (Bound::Excluded(_), Bound::Unbounded) => "WHERE key > ?1",
        (Bound::Unbounded, Bound::Unbounded) => "",
    };
    let stmt = if rev {
        format!("SELECT key, value FROM data {where_clause} ORDER BY key DESC")
    } else {
        format!("SELECT key, value FROM data {where_clause} ORDER BY key")
    };
    let mut params = Vec::with_capacity(2);
    if let Bound::Included(x) | Bound::Excluded(x) = range.0 {
        params.push(x);
    }
    if let Bound::Included(x) | Bound::Excluded(x) = range.1 {
        params.push(x);
    }
    (stmt, params)
}

/// Returns the upper bound of a prefix, or None
/// if the range must be scanned from prefix until the end.
fn get_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    let len = end.len();

    for (idx, byte) in end.iter_mut().rev().enumerate() {
        if *byte < 255 {
            *byte += 1;
            end.truncate(len - idx);
            return Some(end);
        }
    }

    None
}
