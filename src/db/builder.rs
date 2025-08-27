use super::{Backend, DatabaseWrapper, GenericDatabase};
use crate::args::CommonRunOptions;
use std::{path::Path, sync::Arc, time::Duration};

pub const TABLE: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("data");

pub struct DatabaseBuilder;

impl DatabaseBuilder {
    pub fn build<P: AsRef<Path>>(path: P, args: &CommonRunOptions) -> DatabaseWrapper {
        let db = match args.backend {
            #[cfg(feature = "sqlite")]
            Backend::Sqlite => {
                use r2d2_sqlite::SqliteConnectionManager;
                use rusqlite::Connection;

                std::fs::create_dir_all(&path).unwrap();

                let path = path.as_ref().join("sqlite.db");

                {
                    let conn = Connection::open(&path).unwrap();

                    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
                    conn.pragma_update(None, "cache_size", format!("-{}", args.cache_size / 1_024))
                        .unwrap();

                    if args.fsync {
                        conn.pragma_update(None, "synchronous", "FULL").unwrap();
                    } else {
                        conn.pragma_update(None, "synchronous", "NORMAL").unwrap();
                    }

                    conn.execute(
                        "CREATE TABLE data (key BLOB PRIMARY KEY COLLATE BINARY, value BLOB NOT NULL) WITHOUT ROWID, STRICT",
                        (),
                    )
                    .unwrap();
                }

                let manager = SqliteConnectionManager::file(path);
                let pool = r2d2::Pool::new(manager).unwrap();

                GenericDatabase::Sqlite(pool)
            }

            #[cfg(feature = "rocksdb")]
            Backend::RocksDb => {
                use rocksdb::BlockBasedOptions;

                std::fs::create_dir_all(&path).unwrap();

                let mut opts = rocksdb::Options::default();
                opts.create_if_missing(true);
                opts.set_compression_type(match args.compression {
                    crate::args::Compression::None => rocksdb::DBCompressionType::None,
                    crate::args::Compression::Lz4 => rocksdb::DBCompressionType::Lz4,
                });
                opts.set_manual_wal_flush(true);
                opts.set_max_background_jobs(3);
                opts.set_level_zero_file_num_compaction_trigger(4);
                opts.set_write_buffer_size(args.lsm_write_buffer_bytes as usize);

                let mut bopts = BlockBasedOptions::default();
                bopts.set_bloom_filter(f64::from(args.lsm_bloom_bpk), false);
                bopts.set_block_size(args.lsm_block_size as usize);
                bopts.set_index_type(rocksdb::BlockBasedIndexType::BinarySearch);
                bopts.set_pin_l0_filter_and_index_blocks_in_cache(true);
                // bopts.set_pin_top_level_index_and_filter(true);
                bopts.set_data_block_index_type(rocksdb::DataBlockIndexType::BinarySearch);
                bopts.set_index_block_restart_interval(1);
                bopts.set_cache_index_and_filter_blocks(true);

                if args.lsm_data_block_hash_ratio > 0.0 {
                    bopts.set_data_block_index_type(rocksdb::DataBlockIndexType::BinaryAndHash);
                    bopts
                        .set_data_block_hash_ratio(f64::from(1.0 / args.lsm_data_block_hash_ratio));
                }

                let my_cache =
                    rocksdb::Cache::new_hyper_clock_cache(args.cache_size as usize, 100_000);

                bopts.set_block_cache(&my_cache);

                opts.set_block_based_table_factory(&bopts);

                match args.lsm_compaction {
                    crate::args::LsmCompaction::Fifo => {
                        opts.set_compaction_style(rocksdb::DBCompactionStyle::Fifo);
                        opts.set_fifo_compaction_options(&{
                            let mut opts = rocksdb::FifoCompactOptions::default();
                            opts.set_max_table_files_size(args.lsm_fifo_limit_bytes);
                            opts
                        });
                    }
                    crate::args::LsmCompaction::Tiered => {
                        unimplemented!()
                    }
                    crate::args::LsmCompaction::Leveled => {
                        // Default
                    }
                }

                // TODO: hmmm
                // opts.set_enable_blob_files(args.value_size >= 1_024);
                // opts.set_blob_compression_type(rocksdb::DBCompressionType::Lz4);
                // opts.set_blob_cache(&my_cache);
                // opts.set_min_blob_size(1_024);

                let db = rocksdb::OptimisticTransactionDB::open(&opts, &path).unwrap();
                GenericDatabase::RocksDb(Arc::new(db))
            }

            #[cfg(feature = "heed")]
            Backend::Heed => {
                use heed::EnvFlags;

                std::fs::create_dir_all(&path).unwrap();

                let env = unsafe {
                    heed::EnvOpenOptions::new()
                        .map_size(1_000_000_000_000)
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
                    .compaction_workers(2)
                    .max_write_buffer_size(256 * 1_024 * 1_024)
                    .manual_journal_persist(true);

                let keyspace = config.open_transactional().unwrap();

                let create_opts = fjall::PartitionCreateOptions::default()
                    .bloom_filter_bits(Some(args.lsm_bloom_bpk))
                    .max_memtable_size(64 * 1_024 * 1_024)
                    .block_size(args.lsm_block_size)
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
                        crate::args::LsmCompaction::Fifo => fjall::compaction::Strategy::Fifo(
                            fjall::compaction::Fifo::new(args.lsm_fifo_limit_bytes, None),
                        ),
                    })
                    .compression(match args.compression {
                        crate::args::Compression::None => fjall::CompressionType::None,
                        crate::args::Compression::Lz4 => fjall::CompressionType::Lz4,
                    });

                // TODO: hmmm
                /* if args.value_size >= fjall::KvSeparationOptions::default().separation_threshold {
                    create_opts = create_opts.with_kv_separation(Default::default());
                } */

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

            #[cfg(feature = "fjall_nightly")]
            Backend::FjallNightly => {
                let config = fjall_nightly::Config::new(path)
                    .cache_size(args.cache_size)
                    .compaction_workers(2)
                    .max_write_buffer_size(4 * 1_024 * 1_024 * 1_024)
                    .manual_journal_persist(true);

                let keyspace = config.open_transactional().unwrap();

                let mut create_opts = fjall_nightly::PartitionCreateOptions::default()
                    .data_block_hash_ratio(args.lsm_data_block_hash_ratio)
                    .bloom_filter_bits(Some(args.lsm_bloom_bpk))
                    .max_memtable_size(args.lsm_write_buffer_bytes)
                    .block_size(args.lsm_block_size)
                    .compaction_strategy(match args.lsm_compaction {
                        crate::args::LsmCompaction::Leveled => {
                            fjall_nightly::compaction::Strategy::Leveled(
                                fjall_nightly::compaction::Leveled::default(),
                            )
                        }
                        crate::args::LsmCompaction::Tiered => {
                            fjall_nightly::compaction::Strategy::SizeTiered(
                                fjall_nightly::compaction::SizeTiered::default(),
                            )
                        }
                        crate::args::LsmCompaction::Fifo => {
                            fjall_nightly::compaction::Strategy::Fifo(
                                fjall_nightly::compaction::Fifo::new(
                                    args.lsm_fifo_limit_bytes,
                                    None,
                                ),
                            )
                        }
                    })
                    .compression(match args.compression {
                        crate::args::Compression::None => fjall_nightly::CompressionType::None,
                        crate::args::Compression::Lz4 => fjall_nightly::CompressionType::Lz4,
                    });

                // if args.value_size
                //     >= fjall_nightly::KvSeparationOptions::default().separation_threshold
                // {
                //     create_opts = create_opts.with_kv_separation(Default::default());
                // }

                let db = keyspace.open_partition("data", create_opts).unwrap();

                if db.inner().is_kv_separated() {
                    use fjall_nightly::GarbageCollection;
                    let blobs = db.clone();

                    std::thread::spawn(move || loop {
                        blobs.gc_scan().unwrap();
                        blobs.gc_with_space_amp_target(3.0).unwrap();
                        blobs.gc_with_staleness_threshold(0.9).unwrap();
                        std::thread::sleep(Duration::from_secs(60));
                    });
                }

                GenericDatabase::FjallNightly { keyspace, db }
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

            delete_ops: Default::default(),
            delete_latency: Default::default(),
        }
    }
}
