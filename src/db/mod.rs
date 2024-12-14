mod backend;

use crate::args::RunOptions;
pub use backend::Backend;
use hdrhistogram::Histogram;
use std::{
    path::Path,
    sync::{atomic::AtomicU64, Arc, Mutex},
    time::Instant,
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

    #[cfg(feature = "heed")]
    Heed {
        db: heed::Database<heed::types::Bytes, heed::types::Bytes>,
        env: heed::Env,
    },

    #[cfg(feature = "rocksdb")]
    RocksDb(Arc<rocksdb::DB>),

    #[cfg(feature = "sqlite")]
    Sqlite(Arc<Mutex<rusqlite::Connection>>),
}

const TABLE: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("data");

#[derive(Clone)]
pub struct DatabaseWrapper {
    pub inner: GenericDatabase,

    pub write_ops: Arc<AtomicU64>,
    pub write_latency: Arc<AtomicU64>,
    pub written_bytes: Arc<AtomicU64>,

    pub point_read_ops: Arc<AtomicU64>,
    pub point_read_latency: Arc<AtomicU64>,

    pub range_ops: Arc<AtomicU64>,
    pub range_latency: Arc<AtomicU64>,

    pub write_latency_histogram: Arc<Mutex<Histogram<u64>>>,
    pub point_read_latency_histogram: Arc<Mutex<Histogram<u64>>>,
}

impl std::ops::Deref for DatabaseWrapper {
    type Target = GenericDatabase;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DatabaseWrapper {
    pub fn prefix_len(&self, prefix: &[u8], rev: bool, take: usize) -> usize {
        let start = Instant::now();

        let v = match &self.inner {
            GenericDatabase::Sqlite(_db) => {
                unimplemented!();
            }

            #[cfg(feature = "localfjall")]
            GenericDatabase::LocalFjall { db, keyspace } => {
                let read_tx = keyspace.read_tx();
                let iter = read_tx.prefix(db, prefix);

                if rev {
                    iter.rev().take(take).map(|kv| kv.unwrap()).count()
                } else {
                    iter.take(take).count()
                }
            }
            GenericDatabase::Fjall { db, keyspace } => {
                let read_tx = keyspace.read_tx();
                let iter = read_tx.prefix(db, prefix);

                if rev {
                    iter.rev().take(take).map(|kv| kv.unwrap()).count()
                } else {
                    iter.take(take).count()
                }
            }
            GenericDatabase::Sled(db) => {
                let iter = db.scan_prefix(prefix);

                if rev {
                    iter.rev().take(take).map(|kv| kv.unwrap()).count()
                } else {
                    iter.take(take).map(|kv| kv.unwrap()).count()
                }
            }
            GenericDatabase::Redb(db) => {
                let tx = db.begin_read().unwrap();

                let table = tx.open_table(TABLE).unwrap();

                let iter = table.range(prefix..).unwrap();

                if rev {
                    iter.rev()
                        .map(|x| {
                            let (k, v) = x.unwrap();
                            let k: Vec<u8> = k.value().into();
                            let v: Vec<u8> = v.value().into();
                            (k, v)
                        })
                        .filter(|(k, _)| k.starts_with(prefix))
                        .take(take)
                        .count()
                } else {
                    iter.map(|x| {
                        let (k, v) = x.unwrap();
                        let k: Vec<u8> = k.value().into();
                        let v: Vec<u8> = v.value().into();
                        (k, v)
                    })
                    .take_while(|(k, _)| k.starts_with(prefix))
                    .take(take)
                    .count()
                }
            }

            #[cfg(feature = "heed")]
            GenericDatabase::Heed { db, env } => {
                let tx = env.read_txn().unwrap();

                if rev {
                    let iter = db.rev_prefix_iter(&tx, prefix).unwrap();

                    iter.take(take)
                        .map(|kv| {
                            let (k, v) = kv.unwrap();
                            (k.to_vec(), v.to_vec())
                        })
                        .count()
                } else {
                    let iter = db.prefix_iter(&tx, prefix).unwrap();

                    iter.take(take)
                        .map(|kv| {
                            let (k, v) = kv.unwrap();
                            (k.to_vec(), v.to_vec())
                        })
                        .count()
                }
            }

            #[cfg(feature = "rocksdb")]
            GenericDatabase::RocksDb(db) => {
                let dir = if rev {
                    rocksdb::IteratorMode::End
                } else {
                    rocksdb::IteratorMode::Start
                };
                let iter = db.iterator(dir);

                iter.take(take).map(|kv| kv.unwrap()).count()
            }
        };

        self.range_latency.fetch_add(
            start.elapsed().as_nanos() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        self.range_ops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        v
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

    pub fn load<P: AsRef<Path>>(path: P, args: &RunOptions) -> Self {
        let db = match args.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite => {
                use rusqlite::Connection;

                std::fs::create_dir_all(&path).unwrap();

                let conn = Connection::open(path.as_ref().join("sqlite.db")).unwrap();

                conn.pragma_update(None, "journal_mode", "WAL").unwrap();

                if args.fsync {
                    conn.pragma_update(None, "synchronous", "FULL").unwrap();
                } else {
                    conn.pragma_update(None, "synchronous", "NORMAL").unwrap();
                }

                conn.execute(
                    "CREATE TABLE data (key BLOB NOT NULL UNIQUE, value BLOB NOT NULL)",
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
                // opts.set_enable_blob_files(args.lsm_kv_separation);
                opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
                opts.set_manual_wal_flush(true);
                opts.set_max_background_jobs(6);

                let mut bopts = BlockBasedOptions::default();
                bopts.set_block_cache(&rocksdb::Cache::new_lru_cache(args.cache_size as usize));
                bopts.set_bloom_filter(10.0, false);
                bopts.set_block_size(4 * 1_024);
                bopts.set_index_type(rocksdb::BlockBasedIndexType::TwoLevelIndexSearch);
                bopts.set_pin_l0_filter_and_index_blocks_in_cache(true);

                opts.set_block_based_table_factory(&bopts);
                opts.set_blob_compression_type(rocksdb::DBCompressionType::Lz4);

                // TODO: how to set blob cache???

                let db = rocksdb::DB::open(&opts, &path).unwrap();
                GenericDatabase::RocksDb(Arc::new(db))
            }

            #[cfg(feature = "heed")]
            Backend::Heed => {
                use heed::EnvFlags;

                std::fs::create_dir_all(&path).unwrap();

                let env = unsafe {
                    heed::EnvOpenOptions::new()
                        .map_size(64_000_000_000)
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
                let mut config = fjall::Config::new(path)
                    .compaction_workers(6)
                    .max_write_buffer_size(256 * 1_024 * 1_024)
                    .manual_journal_persist(true);

                // TODO: fjall will unify caches... soon
                config = if args.value_size
                    >= fjall::KvSeparationOptions::default().separation_threshold
                {
                    config
                        .block_cache(
                            fjall::BlockCache::with_capacity_bytes(args.cache_size / 100 * 5)
                                .into(),
                        )
                        .blob_cache(
                            fjall::BlobCache::with_capacity_bytes(args.cache_size / 100 * 95)
                                .into(),
                        )
                } else {
                    config
                        .block_cache(fjall::BlockCache::with_capacity_bytes(args.cache_size).into())
                };

                let keyspace = config.open_transactional().unwrap();

                let mut create_opts = fjall::PartitionCreateOptions::default()
                    //  .max_memtable_size(16 * 1_024 * 1_024)
                    // .block_size(4 * 1_024)
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

                GenericDatabase::Fjall { keyspace, db }
            }
            #[cfg(feature = "localfjall")]
            Backend::LocalFjall => {
                let mut config = local_fjall::Config::new(path)
                    .compaction_workers(6)
                    .max_write_buffer_size(256 * 1_024 * 1_024)
                    .manual_journal_persist(true);

                // TODO: fjall will unify caches... soon
                config = if args.value_size
                    >= local_fjall::KvSeparationOptions::default().separation_threshold
                {
                    config
                        .block_cache(
                            local_fjall::BlockCache::with_capacity_bytes(args.cache_size / 100 * 5)
                                .into(),
                        )
                        .blob_cache(
                            local_fjall::BlobCache::with_capacity_bytes(args.cache_size / 100 * 95)
                                .into(),
                        )
                } else {
                    config.block_cache(
                        local_fjall::BlockCache::with_capacity_bytes(args.cache_size).into(),
                    )
                };

                let keyspace = config.open_transactional().unwrap();

                let mut create_opts = local_fjall::PartitionCreateOptions::default()
                    //  .max_memtable_size(64 * 1_024 * 1_024)
                    //  .block_size(4 * 1_024)
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

                GenericDatabase::LocalFjall { keyspace, db }
            }
        };

        DatabaseWrapper {
            inner: db,

            write_ops: Default::default(),
            write_latency: Default::default(),
            written_bytes: Default::default(),

            point_read_ops: Default::default(),
            point_read_latency: Default::default(),

            range_ops: Default::default(),
            range_latency: Default::default(),

            write_latency_histogram: Arc::new(Mutex::new(Histogram::new(5).unwrap())),
            point_read_latency_histogram: Arc::new(Mutex::new(Histogram::new(5).unwrap())),
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

    /// NOTE: Purposefully only returns the length to avoid heap allocation
    pub fn last_len(&self) -> Option<usize> {
        let start = Instant::now();

        let item = match &self.inner {
            GenericDatabase::Fjall { db, .. } => {
                unimplemented!("fjall 2.5.0")
                // let item = db.last_key_value().unwrap();
                // item.map(|(_, v)| v.len())
            }
            GenericDatabase::Sled(db) => {
                let item = db.last().unwrap();
                item.map(|(_, v)| v.len())
            }
            GenericDatabase::Redb(db) => {
                use redb::ReadableTable;

                let read_txn = db.begin_read().unwrap();
                let table = read_txn.open_table(TABLE).unwrap();
                table.last().unwrap().map(|(_, v)| v.value().len())
            }
            /* GenericDatabase::Bloodstone(db) => {
                let item = db.get(key).unwrap();
                item.map(|x| x.to_vec())
            } */
            _ => {
                unimplemented!()
            }
        };

        self.point_read_latency.fetch_add(
            start.elapsed().as_nanos() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        self.point_read_ops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        item
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let start = Instant::now();

        let item = match &self.inner {
            #[cfg(feature = "sqlite")]
            GenericDatabase::Sqlite(db) => {
                let value = db.lock().unwrap().query_row(
                    "SELECT value FROM data WHERE key = ?",
                    [key],
                    |row| Ok(row.get(0).unwrap()),
                );

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
            GenericDatabase::RocksDb(db) => db.get(key).unwrap(),
            GenericDatabase::Fjall { db, .. } => {
                let item = db.get(key).unwrap();
                item.map(|x| x.to_vec())
            }

            #[cfg(feature = "localfjall")]
            GenericDatabase::LocalFjall { db, .. } => {
                let item = db.get(key).unwrap();
                item.map(|x| x.to_vec())
            }

            GenericDatabase::Sled(db) => {
                let item = db.get(key).unwrap();
                item.map(|x| x.to_vec())
            }
            GenericDatabase::Redb(db) => {
                let read_txn = db.begin_read().unwrap();
                let table = read_txn.open_table(TABLE).unwrap();
                table.get(key).unwrap().map(|x| x.value().to_vec())
            }
            #[cfg(feature = "heed")]
            GenericDatabase::Heed { db, env } => {
                let read_txn = env.read_txn().unwrap();
                db.get(&read_txn, key).unwrap().map(|x| x.to_vec())
            }
            _ => {
                unimplemented!()
            }
        };

        let point_read_latency = start.elapsed().as_nanos() as u64;

        self.point_read_latency
            .fetch_add(point_read_latency, std::sync::atomic::Ordering::Relaxed);

        self.point_read_ops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        self.point_read_latency_histogram
            .lock()
            .unwrap()
            .record(point_read_latency / 10)
            .inspect_err(|_| {
                log::warn!("Point read latency value too large for histogram");
            })
            .ok();

        item
    }

    pub fn ingest(&self, items: impl Iterator<Item = (Vec<u8>, Vec<u8>)>) {
        let start = Instant::now();

        let mut count = 0;
        let mut bytes_written = 0;

        match &self.inner {
            #[cfg(feature = "sqlite")]
            GenericDatabase::Sqlite(_db) => {
                unimplemented!();
            }

            #[cfg(feature = "rocksdb")]
            GenericDatabase::RocksDb(db) => {
                for (key, value) in items {
                    db.put(&key, &value).unwrap();

                    count += 1;
                    bytes_written += key.len() + value.len();
                }
                db.flush_wal(true).unwrap();
            }
            GenericDatabase::Fjall { keyspace, db } => {
                for (key, value) in items {
                    db.insert(&key, &value).unwrap();

                    count += 1;
                    bytes_written += key.len() + value.len();
                }
                keyspace.persist(fjall::PersistMode::SyncAll).unwrap();
            }
            #[cfg(feature = "localfjall")]
            GenericDatabase::LocalFjall { keyspace, db } => {
                for (key, value) in items {
                    db.insert(&key, &value).unwrap();

                    count += 1;
                    bytes_written += key.len() + value.len();
                }
                keyspace.persist(local_fjall::PersistMode::SyncAll).unwrap();
            }
            GenericDatabase::Sled(db) => {
                for (key, value) in items {
                    db.insert(&key, &*value).unwrap();

                    count += 1;
                    bytes_written += key.len() + value.len();
                }
                db.flush().unwrap();
            }
            GenericDatabase::Redb(db) => {
                let write_txn = db.begin_write().unwrap();
                {
                    let mut table = write_txn.open_table(TABLE).unwrap();

                    for (key, value) in items {
                        table.insert(&*key, &*value).unwrap();

                        count += 1;
                        bytes_written += key.len() + value.len();
                    }
                }
                write_txn.commit().unwrap();
            }
            #[cfg(feature = "heed")]
            GenericDatabase::Heed { db, env } => {
                let mut write_txn = env.write_txn().unwrap();
                {
                    for (key, value) in items {
                        db.put(&mut write_txn, &key, &value).unwrap();

                        count += 1;
                        bytes_written += key.len() + value.len();
                    }
                }
                write_txn.commit().unwrap();
            }
        }

        self.write_latency.fetch_add(
            start.elapsed().as_nanos() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        self.write_ops
            .fetch_add(count, std::sync::atomic::Ordering::Relaxed);

        self.written_bytes
            .fetch_add(bytes_written as u64, std::sync::atomic::Ordering::Relaxed);

        log::info!("Ingested {count} initial items");
    }

    pub fn insert(&self, key: &[u8], value: &[u8], durable: bool) {
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
                db.put(key, value).unwrap();
                db.flush_wal(durable).unwrap();
            } /* GenericDatabase::Bloodstone(db) => {
                  db.insert(key, value).unwrap();

                  if durable {
                      db.flush().unwrap();
                  } /* else if args.sled_flush {
                        // NOTE: TODO: OOM Workaround
                        // Intermittenly flush sled to keep memory usage sane
                        // This is hopefully a temporary workaround
                        if self.write_ops.load(std::sync::atomic::Ordering::Relaxed) % 50_000 == 0 {
                            println!("\n\n\nanti OOM flush");
                            db.flush().unwrap();
                        }
                    } */
              } */
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
            .record(written_latency / 10)
            .inspect_err(|_| {
                log::warn!("Write latency value too large for histogram");
            })
            .ok();
    }
}
