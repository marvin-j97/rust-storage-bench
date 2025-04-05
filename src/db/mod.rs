mod backend;

use crate::args::RunOptions;
pub use backend::Backend;
use hdrhistogram::Histogram;
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

    #[cfg(feature = "canopydb")]
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

    pub write_ops: Arc<AtomicU64>,
    pub write_latency: Arc<AtomicU64>,
    pub written_bytes: Arc<AtomicU64>,

    pub point_read_ops: Arc<AtomicU64>,
    pub point_read_latency: Arc<AtomicU64>,

    pub range_ops: Arc<AtomicU64>,
    pub range_latency: Arc<AtomicU64>,

    pub write_latency_histogram: Arc<Mutex<Histogram<u64>>>,
    pub point_read_latency_histogram: Arc<Mutex<Histogram<u64>>>,
    pub range_latency_histogram: Arc<Mutex<Histogram<u64>>>,
}

impl std::ops::Deref for DatabaseWrapper {
    type Target = GenericDatabase;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DatabaseWrapper {
    fn report_scan(&self, start: Instant) {
        let latency = start.elapsed().as_nanos() as u64;

        self.range_latency
            .fetch_add(latency, std::sync::atomic::Ordering::Relaxed);

        self.range_ops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        self.range_latency_histogram
            .lock()
            .unwrap()
            .record(latency / 10)
            .inspect_err(|_| {
                log::warn!("Scan latency value too large for histogram");
            })
            .ok();
    }

    pub fn range_first(&self, range: (Bound<&[u8]>, Bound<&[u8]>)) -> Option<(Vec<u8>, Vec<u8>)> {
        let start = Instant::now();

        let v = match &self.inner {
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
            #[cfg(feature = "canopydb")]
            GenericDatabase::Canopydb(db) => {
                let tx = db.begin_read().unwrap();
                let tree = tx.get_tree(b"default").unwrap().unwrap();

                let mut iter = tree.range::<&[u8]>(range).unwrap();

                iter.next()
                    .transpose()
                    .unwrap()
                    .map(|(k, v)| (k.to_vec(), v.to_vec()))
            }
            _ => unimplemented!(),
        };

        self.report_scan(start);

        v
    }

    pub fn prefix_len(&self, prefix: &[u8], rev: bool, take: usize) -> usize {
        let start = Instant::now();

        let v = match &self.inner {
            #[cfg(feature = "sqlite")]
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

                let upper_bound = get_upper_bound(prefix);
                let iter = if let Some(upper_bound) = upper_bound {
                    table.range(prefix..&upper_bound[..]).unwrap()
                } else {
                    table.range(prefix..).unwrap()
                };

                if rev {
                    iter.rev()
                        .map(|x| {
                            let (k, v) = x.unwrap();
                            let k: Vec<u8> = k.value().into();
                            let v: Vec<u8> = v.value().into();
                            (k, v)
                        })
                        .take(take)
                        .count()
                } else {
                    iter.map(|x| {
                        let (k, v) = x.unwrap();
                        let k: Vec<u8> = k.value().into();
                        let v: Vec<u8> = v.value().into();
                        (k, v)
                    })
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
                let mut iter = db.iterator(rocksdb::IteratorMode::Start);
                iter.set_mode(rocksdb::IteratorMode::From(
                    prefix,
                    if rev {
                        rocksdb::Direction::Reverse
                    } else {
                        rocksdb::Direction::Forward
                    },
                ));
                iter.take(take).map(|kv| kv.unwrap()).count()
            }
            #[cfg(feature = "canopydb")]
            GenericDatabase::Canopydb(db) => {
                let tx = db.begin_read().unwrap();
                let tree = tx.get_tree(b"default").unwrap().unwrap();

                let range = tree.prefix(&prefix).unwrap();

                if rev {
                    range.rev().take(take).map(|kv| kv.unwrap()).count()
                } else {
                    range.take(take).map(|kv| kv.unwrap()).count()
                }
            }
        };

        self.report_scan(start);

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
            #[cfg(feature = "canopydb")]
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

            #[cfg(feature = "canopydb")]
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

            write_ops: Default::default(),
            write_latency: Default::default(),
            written_bytes: Default::default(),

            point_read_ops: Default::default(),
            point_read_latency: Default::default(),

            range_ops: Default::default(),
            range_latency: Default::default(),

            write_latency_histogram: Arc::new(Mutex::new(Histogram::new(5).unwrap())),
            point_read_latency_histogram: Arc::new(Mutex::new(Histogram::new(5).unwrap())),
            range_latency_histogram: Arc::new(Mutex::new(Histogram::new(5).unwrap())),
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

    pub fn first(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        let start = Instant::now();

        let item = match &self.inner {
            GenericDatabase::Fjall { db, .. } => db
                .first_key_value()
                .unwrap()
                .map(|(k, v)| (k.to_vec(), v.to_vec())),

            #[cfg(feature = "localfjall")]
            GenericDatabase::LocalFjall { db, .. } => db
                .first_key_value()
                .unwrap()
                .map(|(k, v)| (k.to_vec(), v.to_vec())),

            GenericDatabase::Sled(db) => db.first().unwrap().map(|(k, v)| (k.to_vec(), v.to_vec())),
            GenericDatabase::Redb(db) => {
                use redb::ReadableTable;

                let read_txn = db.begin_read().unwrap();
                let table = read_txn.open_table(TABLE).unwrap();
                table
                    .first()
                    .unwrap()
                    .map(|(k, v)| (k.value().to_vec(), v.value().to_vec()))
            }
            _ => self.range_first((Bound::Unbounded, Bound::Unbounded)),
        };

        self.report_scan(start);

        item
    }

    /// NOTE: Purposefully only returns the length to avoid heap allocation
    pub fn last_len(&self) -> Option<usize> {
        let start = Instant::now();

        let item = match &self.inner {
            GenericDatabase::Fjall { db, .. } => {
                let item = db.last_key_value().unwrap();
                item.map(|(_, v)| v.len())
            }

            #[cfg(feature = "localfjall")]
            GenericDatabase::LocalFjall { db, .. } => {
                let item = db.last_key_value().unwrap();
                item.map(|(_, v)| v.len())
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

            #[cfg(feature = "canopydb")]
            GenericDatabase::Canopydb(db) => {
                let tx = db.begin_read().unwrap();
                let tree = tx.get_tree(b"default").unwrap().unwrap();

                tree.iter()
                    .unwrap()
                    .next_back()
                    .transpose()
                    .unwrap()
                    .map(|(_, v)| v.len())
            }
        };

        self.report_scan(start);
        item
    }

    pub fn len(&self) -> usize {
        match &self.inner {
            GenericDatabase::Fjall { db, .. } => db.inner().len().unwrap(),

            #[cfg(feature = "localfjall")]
            GenericDatabase::LocalFjall { db, .. } => db.inner().len().unwrap(),

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
                .record(point_read_latency / 10)
                .inspect_err(|_| {
                    log::warn!("Point read latency value too large for histogram");
                })
                .ok();
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
            #[cfg(feature = "canopydb")]
            GenericDatabase::Canopydb(db) => {
                let tx = db.begin_read().unwrap();
                let tree = tx.get_tree(b"default").unwrap().unwrap();

                let value = tree.get(key).unwrap();
                report_latency();
                value.map(|x| x.to_vec())
            }
        };

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
            GenericDatabase::Fjall { db, .. } => {
                db.inner()
                    .ingest(items.map(|(k, v)| {
                        count += 1;
                        bytes_written += k.len() + v.len();
                        (k, v)
                    }))
                    .unwrap();
            }
            #[cfg(feature = "localfjall")]
            GenericDatabase::LocalFjall { db, .. } => {
                db.inner()
                    .ingest(items.map(|(k, v)| {
                        count += 1;
                        bytes_written += k.len() + v.len();
                        (k, v)
                    }))
                    .unwrap();
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
                        db.put_with_flags(&mut write_txn, heed::PutFlags::APPEND, &key, &value)
                            .unwrap();

                        count += 1;
                        bytes_written += key.len() + value.len();
                    }
                }
                write_txn.commit().unwrap();
            }
            #[cfg(feature = "canopydb")]
            GenericDatabase::Canopydb(db) => {
                let write_txn = db.begin_write().unwrap();
                {
                    let mut tree = write_txn.get_tree(b"default").unwrap().unwrap();

                    for (key, value) in items {
                        tree.insert(&key, &value).unwrap();

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

        log::info!("Ingested {count} initial items in {:?}", start.elapsed());
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
            }
            #[cfg(feature = "canopydb")]
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
            .record(written_latency / 10)
            .inspect_err(|_| {
                log::warn!("Write latency value too large for histogram");
            })
            .ok();
    }

    // TODO:
    pub fn remove_unique(&self, key: &[u8], durable: bool) {
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
            }
            _ => {
                self.remove(key, durable);
            }
        }
    }

    pub fn remove(&self, key: &[u8], durable: bool) {
        let _start = Instant::now();

        match &self.inner {
            #[cfg(feature = "sqlite")]
            GenericDatabase::Sqlite(db) => {
                unimplemented!()
            }

            GenericDatabase::Fjall { keyspace, db } => {
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
                db.delete(key).unwrap();
                db.flush_wal(durable).unwrap();
            }
            #[cfg(feature = "canopydb")]
            GenericDatabase::Canopydb(db) => {
                let write_txn = db.begin_write().unwrap();
                {
                    let mut tree = write_txn.get_tree(b"default").unwrap().unwrap();
                    tree.delete(key).unwrap();
                }
                write_txn.commit().unwrap();
            }
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

/// Returns the upper bound of a prefix, or None
/// if the range must be scanned from prefix until the end.
pub fn get_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
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
