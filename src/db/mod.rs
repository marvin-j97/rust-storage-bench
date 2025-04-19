mod backend;

use crate::args::RunOptions;
pub use backend::Backend;
use sketches_ddsketch::DDSketch;
use std::{
    ops::Bound,
    path::Path,
    sync::{atomic::AtomicU64, Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Clone)]
pub enum GenericDatabase {
    Fjall {
        keyspace: fjall::TxKeyspace,
        db: fjall::TxPartition,
    },

    #[cfg(feature = "localfjall")]
    LocalFjall {
        keyspace: local_fjall::TxKeyspace,
        db: local_fjall::TxPartition,
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
    Sqlite(Arc<Mutex<rusqlite::Connection>>),
}

const TABLE: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("data");

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

            #[cfg(feature = "localfjall")]
            GenericDatabase::LocalFjall { db, keyspace } => {
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
                db.lock()
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

            #[cfg(feature = "localfjall")]
            GenericDatabase::LocalFjall { db, keyspace } => {
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
                db.lock()
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

            #[cfg(feature = "localfjall")]
            GenericDatabase::LocalFjall { db, keyspace } => {
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
        match &self.inner {
            // TODO: expensive?!
            /* GenericDatabase::Redb(db) => {
                use redb::ReadableTableMetadata;

                let tx = db.begin_read().unwrap();
                let table = tx.open_table(TABLE).unwrap();
                table.stats().unwrap().fragmented_bytes() as usize
            } */
            _ => 0,
        }
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

    pub fn write_buffer_size(&self) -> u64 {
        match &self.inner {
            GenericDatabase::Fjall { keyspace, .. } => keyspace.write_buffer_size(),

            #[cfg(feature = "localfjall")]
            GenericDatabase::LocalFjall { keyspace, .. } => keyspace.write_buffer_size(),

            _ => 0,
        }
    }

    pub fn bloom_filter_size(&self) -> usize {
        match &self.inner {
            GenericDatabase::Fjall { db, .. } => {
                use fjall::AbstractTree;

                db.inner().tree.bloom_filter_size()
            }

            #[cfg(feature = "localfjall")]
            GenericDatabase::LocalFjall { db, .. } => {
                use local_fjall::AbstractTree;

                db.inner().tree.bloom_filter_size()
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

            #[cfg(feature = "localfjall")]
            GenericDatabase::LocalFjall { db, .. } => {
                use local_fjall::AbstractTree;

                db.inner().tree.l0_run_count()
            }

            _ => 0,
        }
    }

    pub fn avg_l0_segment_creation_date_us(&self) -> u64 {
        match &self.inner {
            GenericDatabase::Fjall { db, .. } => {
                let tree = match &db.inner().tree {
                    fjall::AnyTree::Blob(tree) => &tree.index.0,
                    fjall::AnyTree::Standard(tree) => tree,
                };

                let first_level = &tree.levels.read().unwrap();
                let first_level = &first_level.levels.first().unwrap().segments;
                let count = first_level.len() as u64;

                let sum: u64 = first_level
                    .iter()
                    .map(|x| x.metadata.created_at as u64)
                    .sum();

                sum.checked_div(count).unwrap_or_default()
            }

            #[cfg(feature = "localfjall")]
            GenericDatabase::LocalFjall { db, .. } => {
                let tree = match &db.inner().tree {
                    local_fjall::AnyTree::Blob(tree) => &tree.index.0,
                    local_fjall::AnyTree::Standard(tree) => tree,
                };

                let first_level = &tree.levels.read().unwrap();
                let first_level = &first_level
                    .levels
                    .first()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>();

                let count = first_level.len() as u64;

                let sum: u64 = first_level
                    .iter()
                    .map(|x| x.metadata.created_at as u64)
                    .sum();

                sum.checked_div(count).unwrap_or_default()
            }
            _ => 0,
        }
    }

    pub fn time_compacting(&self) -> u64 {
        match &self.inner {
            GenericDatabase::Fjall { keyspace, .. } => keyspace.inner().time_compacting(),
            #[cfg(feature = "localfjall")]
            GenericDatabase::LocalFjall { keyspace, .. } => keyspace.inner().time_compacting(),
            _ => 0,
        }
    }

    pub fn active_compactions(&self) -> usize {
        match &self.inner {
            GenericDatabase::Fjall { keyspace, .. } => keyspace.inner().active_compactions(),
            #[cfg(feature = "localfjall")]
            GenericDatabase::LocalFjall { keyspace, .. } => keyspace.inner().active_compactions(),
            _ => 0,
        }
    }

    pub fn blob_file_count(&self) -> usize {
        match &self.inner {
            GenericDatabase::Fjall { db, .. } => {
                use fjall::AbstractTree;

                db.inner().tree.blob_file_count()
            }
            #[cfg(feature = "localfjall")]
            GenericDatabase::LocalFjall { db, .. } => {
                use local_fjall::AbstractTree;

                db.inner().tree.blob_file_count()
            }
            _ => 0,
        }
    }

    pub fn disk_segment_count(&self) -> usize {
        match &self.inner {
            GenericDatabase::Fjall { db, .. } => {
                use fjall::AbstractTree;

                db.inner().tree.segment_count()
            }

            #[cfg(feature = "localfjall")]
            GenericDatabase::LocalFjall { db, .. } => {
                use local_fjall::AbstractTree;

                db.inner().tree.segment_count()
            }
            _ => 0,
        }
    }

    pub fn journal_size(&self) -> u64 {
        match &self.inner {
            GenericDatabase::Fjall { keyspace, .. } => keyspace.inner().journal_disk_space(),

            #[cfg(feature = "localfjall")]
            GenericDatabase::LocalFjall { keyspace, .. } => keyspace.inner().journal_disk_space(),
            _ => 0,
        }
    }

    pub fn journal_count(&self) -> usize {
        match &self.inner {
            GenericDatabase::Fjall { keyspace, .. } => keyspace.journal_count(),

            #[cfg(feature = "localfjall")]
            GenericDatabase::LocalFjall { keyspace, .. } => keyspace.journal_count(),
            _ => 0,
        }
    }

    pub fn load<P: AsRef<Path>>(path: P, args: &RunOptions) -> Self {
        let db = match args.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite => {
                use rusqlite::Connection;

                std::fs::create_dir_all(&path).unwrap();

                let conn = Connection::open(path.as_ref().join("sqlite.db")).unwrap();

                conn.pragma_update(None, "journal_mode", "WAL").unwrap();
                conn.pragma_update(None, "cache_size", format!("-{}", args.cache_size / 1024))
                    .unwrap();

                if args.fsync {
                    conn.pragma_update(None, "synchronous", "FULL").unwrap();
                } else {
                    conn.pragma_update(None, "synchronous", "NORMAL").unwrap();
                }

                // TODO: test WITHOUT ROWID to get a clustered index
                conn.execute(
                    "CREATE TABLE data (key BLOB NOT NULL UNIQUE, value BLOB NOT NULL) STRICT",
                    (),
                )
                .unwrap();

                GenericDatabase::Sqlite(Arc::new(Mutex::new(conn)))
            }

            #[cfg(feature = "rocksdb")]
            Backend::RocksDb => {
                use rocksdb::BlockBasedOptions;

                std::fs::create_dir_all(&path).unwrap();

                let mut opts = rocksdb::Options::default();
                opts.create_if_missing(true);
                opts.set_enable_blob_files(args.value_size >= 1_024);
                opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
                opts.set_manual_wal_flush(true);
                opts.set_max_background_jobs(8);
                opts.set_level_zero_file_num_compaction_trigger(4);

                let mut bopts = BlockBasedOptions::default();

                let my_cache = rocksdb::Cache::new_lru_cache(args.cache_size as usize);
                bopts.set_block_cache(&my_cache);
                bopts.set_bloom_filter(10.0, false);
                bopts.set_block_size(4 * 1_024);
                bopts.set_index_type(rocksdb::BlockBasedIndexType::TwoLevelIndexSearch);
                bopts.set_pin_l0_filter_and_index_blocks_in_cache(true);

                opts.set_block_based_table_factory(&bopts);
                opts.set_blob_compression_type(rocksdb::DBCompressionType::Lz4);
                opts.set_blob_cache(&my_cache);
                opts.set_min_blob_size(1_024);

                let db = rocksdb::OptimisticTransactionDB::open(&opts, &path).unwrap();
                GenericDatabase::RocksDb(Arc::new(db))
            }

            #[cfg(feature = "heed")]
            Backend::Heed => {
                use heed::EnvFlags;

                std::fs::create_dir_all(&path).unwrap();

                let env = unsafe {
                    heed::EnvOpenOptions::new()
                        .map_size(128_000_000_000)
                        // TODO: make LMDB NO_SYNC a separate option
                        // as this isn't equivalent to fsync=false for the
                        // other databases which treat it like "no sync commit"
                        .flags(if args.fsync {
                            EnvFlags::NO_READ_AHEAD
                        } else {
                            EnvFlags::NO_SYNC | EnvFlags::NO_READ_AHEAD
                        })
                        .open(&path)
                        .unwrap()
                };

                let mut wtxn = env.write_txn().unwrap();
                let db = env.create_database(&mut wtxn, None).unwrap();
                wtxn.commit().unwrap();

                GenericDatabase::Heed { db, env }
            }

            Backend::Sled => GenericDatabase::Sled(
                sled::Config::new()
                    .path(path)
                    .cache_capacity(args.cache_size)
                    .open()
                    .unwrap(),
            ),

            Backend::Redb => {
                std::fs::create_dir_all(&path).unwrap();

                let db = redb::Builder::new()
                    .set_cache_size(args.cache_size as usize)
                    .create(path.as_ref().join("my_db.redb"))
                    .unwrap();

                {
                    let tx = db.begin_write().unwrap();
                    tx.open_table(TABLE).unwrap();
                    tx.commit().unwrap();
                }

                GenericDatabase::Redb(Arc::new(db))
            }

            Backend::Fjall => {
                let config = fjall::Config::new(path)
                    .cache_size(args.cache_size)
                    .compaction_workers(7)
                    .max_write_buffer_size(256 * 1_024 * 1_024)
                    .manual_journal_persist(true);

                let keyspace = config.open_transactional().unwrap();

                let mut create_opts = fjall::PartitionCreateOptions::default()
                    .max_memtable_size(64 * 1_024 * 1_024)
                    .block_size(4 * 1_024)
                    .compaction_strategy(match args.lsm_compaction {
                        crate::args::LsmCompaction::Leveled => {
                            fjall::compaction::Strategy::Leveled(
                                fjall::compaction::Leveled::default(),
                            )
                        }
                        crate::args::LsmCompaction::Tiered => {
                            fjall::compaction::Strategy::SizeTiered(
                                fjall::compaction::SizeTiered::default(),
                            )
                        }
                    });

                if args.value_size >= fjall::KvSeparationOptions::default().separation_threshold {
                    create_opts = create_opts.with_kv_separation(Default::default());
                }

                let db = keyspace.open_partition("data", create_opts).unwrap();

                if db.inner().is_kv_separated() {
                    use fjall::GarbageCollection;
                    let blobs = db.clone();

                    std::thread::spawn(move || loop {
                        blobs.gc_scan().unwrap();
                        blobs.gc_with_space_amp_target(3.0).unwrap();
                        blobs.gc_with_staleness_threshold(0.9).unwrap();
                        std::thread::sleep(Duration::from_secs(60));
                    });
                }

                GenericDatabase::Fjall { keyspace, db }
            }

            #[cfg(feature = "localfjall")]
            Backend::LocalFjall => {
                let config = local_fjall::Config::new(path)
                    .cache_size(args.cache_size)
                    .compaction_workers(7)
                    .max_write_buffer_size(256 * 1_024 * 1_024)
                    .manual_journal_persist(true);

                let keyspace = config.open_transactional().unwrap();

                let mut create_opts = local_fjall::PartitionCreateOptions::default()
                    .max_memtable_size(64 * 1_024 * 1_024)
                    .block_size(4 * 1_024)
                    .compaction_strategy(match args.lsm_compaction {
                        crate::args::LsmCompaction::Leveled => {
                            local_fjall::compaction::Strategy::Leveled(
                                local_fjall::compaction::Leveled::default(),
                            )
                        }
                        crate::args::LsmCompaction::Tiered => {
                            local_fjall::compaction::Strategy::SizeTiered(
                                local_fjall::compaction::SizeTiered::default(),
                            )
                        }
                    });

                if args.value_size
                    >= local_fjall::KvSeparationOptions::default().separation_threshold
                {
                    create_opts = create_opts.with_kv_separation(Default::default());
                }

                let db = keyspace.open_partition("data", create_opts).unwrap();

                if db.inner().is_kv_separated() {
                    use local_fjall::GarbageCollection;
                    let blobs = db.clone();

                    std::thread::spawn(move || loop {
                        blobs.gc_scan().unwrap();
                        blobs.gc_with_space_amp_target(3.0).unwrap();
                        blobs.gc_with_staleness_threshold(0.9).unwrap();
                        std::thread::sleep(Duration::from_secs(60));
                    });
                }

                GenericDatabase::LocalFjall { keyspace, db }
            }

            Backend::Canopydb => {
                std::fs::create_dir_all(&path).unwrap();

                let mut env_opts = canopydb::EnvOptions::new(&path);
                env_opts.page_cache_size = args.cache_size as usize;
                env_opts.wal_background_sync_interval = None;

                let env = canopydb::Environment::with_options(env_opts).unwrap();
                let db = env.get_or_create_database("default").unwrap();
                let tx = db.begin_write().unwrap();
                tx.get_or_create_tree(b"default").unwrap();
                tx.commit().unwrap();

                GenericDatabase::Canopydb(Arc::new(db))
            }
        };

        DatabaseWrapper {
            inner: db,
            workload_real_bytes: Default::default(),

            write_ops: Default::default(),
            write_latency: Default::default(),
            written_bytes: Default::default(),

            point_read_bytes: Default::default(),
            point_read_ops: Default::default(),
            point_read_latency: Default::default(),

            range_read_bytes: Default::default(),
            range_ops: Default::default(),
            range_latency: Default::default(),

            write_latency_histogram: Default::default(),
            point_read_latency_histogram: Default::default(),
            range_latency_histogram: Default::default(),
            /*
            delete_ops: Default::default(),
            deleted_bytes: Default::default(),
            delete_latency: Default::default(), */
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

            #[cfg(feature = "localfjall")]
            GenericDatabase::LocalFjall { db, .. } => db.inner().len().unwrap(),

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
                    .lock()
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

                // NOTE: Durability is controlled by pragma in load()
            }

            #[cfg(feature = "rocksdb")]
            GenericDatabase::RocksDb(db) => {
                let value = db.get(key).unwrap();
                report_latency();
                value
            }
            GenericDatabase::Fjall { db, .. } => {
                let item = db.get(key).unwrap();
                report_latency();
                item.map(|x| x.to_vec())
            }

            #[cfg(feature = "localfjall")]
            GenericDatabase::LocalFjall { db, .. } => {
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
                let db = db.lock().unwrap();
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
            #[cfg(feature = "localfjall")]
            GenericDatabase::LocalFjall { db, .. } => {
                db.inner()
                    .ingest(items.map(|(k, v)| {
                        on_bytes_written(key.len() + value.len());
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

    pub fn insert(&self, key: &[u8], value: &[u8], durable: bool, increment_workload_size: bool) {
        let start = Instant::now();

        match &self.inner {
            #[cfg(feature = "sqlite")]
            GenericDatabase::Sqlite(db) => {
                db.lock()
                    .unwrap()
                    .execute("INSERT INTO data (key, value) VALUES (?, ?)", (key, value))
                    .unwrap();

                // NOTE: Durability is controlled by pragma in load()
            }

            GenericDatabase::Fjall { keyspace, db } => {
                // TODO: add option to write through transactions
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
            #[cfg(feature = "localfjall")]
            GenericDatabase::LocalFjall { keyspace, db } => {
                db.insert(key, value).unwrap();

                keyspace
                    .persist(if durable {
                        // NOTE: RocksDB uses fsyncdata by default, too
                        local_fjall::PersistMode::SyncData
                    } else {
                        local_fjall::PersistMode::Buffer
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
            #[cfg(feature = "localfjall")]
            GenericDatabase::LocalFjall { keyspace, db } => {
                // TODO: remove_weak
                db.remove(key).unwrap();

                keyspace
                    .persist(if durable {
                        // NOTE: RocksDB uses fsyncdata by default, too
                        local_fjall::PersistMode::SyncData
                    } else {
                        local_fjall::PersistMode::Buffer
                    })
                    .unwrap();
                if let Some(decrement_workload_size) = decrement_workload_size {
                    self.workload_real_bytes.fetch_sub(
                        decrement_workload_size,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                }
            }
            _ => {
                self.remove(key, durable, decrement_workload_size);
            }
        }
    }

    pub fn remove(&self, key: &[u8], durable: bool, decrement_workload_size: Option<u64>) {
        let _start = Instant::now();

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
            #[cfg(feature = "localfjall")]
            GenericDatabase::LocalFjall { keyspace, db } => {
                db.remove(key).unwrap();

                keyspace
                    .persist(if durable {
                        // NOTE: RocksDB uses fsyncdata by default, too
                        local_fjall::PersistMode::SyncData
                    } else {
                        local_fjall::PersistMode::Buffer
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

        // TODO: latency
        // let written_latency = start.elapsed().as_nanos() as u64;

        // self.write_latency
        //     .fetch_add(written_latency, std::sync::atomic::Ordering::Relaxed);

        // self.write_ops
        //     .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // self.write_latency_histogram
        //     .lock()
        //     .unwrap()
        //     .record(written_latency / 10)
        //     .inspect_err(|_| {
        //         log::warn!("Write latency value too large for histogram");
        //     })
        //     .ok();
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
    db.iterator(it_mode)
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
