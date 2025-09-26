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

                #[cfg(feature = "metrics")]
                use crate::db::RocksTickers;
                #[cfg(feature = "metrics")]
                use rocksdb::statistics::Ticker;

                std::fs::create_dir_all(&path).unwrap();

                let mut opts = rocksdb::Options::default();
                opts.create_if_missing(true);
                opts.set_compression_type(match args.compression {
                    crate::args::Compression::None => rocksdb::DBCompressionType::None,
                    crate::args::Compression::Lz4 => rocksdb::DBCompressionType::Lz4,
                });
                opts.set_min_level_to_compress(1);
                opts.set_manual_wal_flush(true);
                opts.set_max_background_jobs(3);
                opts.set_level_zero_file_num_compaction_trigger(4);
                opts.set_write_buffer_size(args.lsm_write_buffer_bytes as usize);
                opts.set_advise_random_on_open(false);

                opts.set_max_open_files(match args.lsm_compaction {
                    // Whyyyy RocksDB
                    crate::args::LsmCompaction::Fifo => -1,
                    _ => 512,
                });

                let mut bopts = BlockBasedOptions::default();

                if let Some(bpk) = args.lsm_bloom_bpk {
                    bopts.set_bloom_filter(f64::from(bpk), false);
                }

                if let Some(block_size) = args.lsm_block_size {
                    bopts.set_block_size(block_size as usize);
                }

                bopts.set_index_type(rocksdb::BlockBasedIndexType::BinarySearch);
                bopts.set_pin_l0_filter_and_index_blocks_in_cache(true);
                bopts.set_pin_top_level_index_and_filter(true);
                bopts.set_data_block_index_type(rocksdb::DataBlockIndexType::BinarySearch);
                bopts.set_index_block_restart_interval(1);
                bopts.set_cache_index_and_filter_blocks(true);
                // bopts.set_index_compression(false); // TODO: cannot set index compression=false
                bopts.set_checksum_type(rocksdb::ChecksumType::XXH3);

                if let Some(hash_ratio) = args.lsm_data_block_hash_ratio {
                    if hash_ratio > 0.0 {
                        bopts.set_data_block_index_type(rocksdb::DataBlockIndexType::BinaryAndHash);
                        bopts.set_data_block_hash_ratio(f64::from(1.0 / hash_ratio));
                    }
                }

                let cache = rocksdb::Cache::new_lru_cache(args.cache_size as usize);

                bopts.set_block_cache(&cache);

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

                #[cfg(feature = "metrics")]
                {
                    opts.enable_statistics();
                    opts.set_statistics_level(
                        rocksdb::statistics::StatsLevel::ExceptHistogramOrTimers,
                    );
                }

                // TODO: hmmm
                // opts.set_enable_blob_files(args.value_size >= 1_024);
                // opts.set_blob_compression_type(rocksdb::DBCompressionType::Lz4);
                // opts.set_blob_cache(&my_cache);
                // opts.set_min_blob_size(1_024);

                let db = rocksdb::OptimisticTransactionDB::open(&opts, &path).unwrap();

                GenericDatabase::RocksDb {
                    db: Arc::new(db),
                    cache,
                    opts,

                    #[cfg(feature = "metrics")]
                    tickers: Arc::new(RocksTickers {
                        block_io: Ticker::BlockCacheMiss,
                        block_cached: Ticker::BlockCacheHit,

                        data_block_io: Ticker::BlockCacheDataMiss,
                        data_block_cached: Ticker::BlockCacheDataHit,

                        filter_block_io: Ticker::BlockCacheFilterMiss,
                        filter_block_cached: Ticker::BlockCacheFilterHit,

                        index_block_io: Ticker::BlockCacheIndexMiss,
                        index_block_cached: Ticker::BlockCacheIndexHit,
                    }),
                }
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

            Backend::Fjall2 => {
                let config = fjall_2::Config::new(path)
                    .cache_size(args.cache_size)
                    .compaction_workers(2)
                    .max_write_buffer_size(256 * 1_024 * 1_024)
                    .manual_journal_persist(true);

                let keyspace = config.open_transactional().unwrap();

                let mut create_opts = fjall_2::PartitionCreateOptions::default()
                    .max_memtable_size(64 * 1_024 * 1_024)
                    .compaction_strategy(match args.lsm_compaction {
                        crate::args::LsmCompaction::Leveled => {
                            fjall_2::compaction::Strategy::Leveled(
                                fjall_2::compaction::Leveled::default(),
                            )
                        }
                        crate::args::LsmCompaction::Tiered => {
                            fjall_2::compaction::Strategy::SizeTiered(
                                fjall_2::compaction::SizeTiered::default(),
                            )
                        }
                        crate::args::LsmCompaction::Fifo => fjall_2::compaction::Strategy::Fifo(
                            fjall_2::compaction::Fifo::new(args.lsm_fifo_limit_bytes, None),
                        ),
                    })
                    .compression(match args.compression {
                        crate::args::Compression::None => fjall_2::CompressionType::None,
                        crate::args::Compression::Lz4 => fjall_2::CompressionType::Lz4,
                    });

                if let Some(bpk) = args.lsm_bloom_bpk {
                    create_opts = create_opts.bloom_filter_bits(Some(bpk));
                }

                if let Some(block_size) = args.lsm_block_size {
                    create_opts = create_opts.block_size(block_size);
                }

                // TODO: hmmm
                /* if args.value_size >= fjall_2::KvSeparationOptions::default().separation_threshold {
                    create_opts = create_opts.with_kv_separation(Default::default());
                } */

                let db = keyspace.open_partition("data", create_opts).unwrap();

                if db.inner().is_kv_separated() {
                    use fjall_2::GarbageCollection;
                    let blobs = db.clone();

                    std::thread::spawn(move || loop {
                        blobs.gc_scan().unwrap();
                        blobs.gc_with_space_amp_target(3.0).unwrap();
                        blobs.gc_with_staleness_threshold(0.9).unwrap();
                        std::thread::sleep(Duration::from_secs(60));
                    });
                }

                GenericDatabase::Fjall2 { keyspace, db }
            }

            #[cfg(feature = "fjall_3")]
            Backend::Fjall3 => {
                let builder = fjall_3::TxDatabase::builder(path)
                    .cache_size(args.cache_size)
                    .compaction_workers(2)
                    .max_write_buffer_size(4 * 1_024 * 1_024 * 1_024)
                    .manual_journal_persist(true);

                let db = builder.open().unwrap();

                let mut create_opts = fjall_3::KeyspaceCreateOptions::default()
                    .max_memtable_size(args.lsm_write_buffer_bytes)
                    .compaction_strategy(match args.lsm_compaction {
                        crate::args::LsmCompaction::Leveled => {
                            fjall_3::compaction::Strategy::Leveled(
                                fjall_3::compaction::Leveled::default(),
                            )
                        }
                        crate::args::LsmCompaction::Tiered => {
                            fjall_3::compaction::Strategy::SizeTiered(
                                fjall_3::compaction::SizeTiered::default(),
                            )
                        }
                        crate::args::LsmCompaction::Fifo => fjall_3::compaction::Strategy::Fifo(
                            fjall_3::compaction::Fifo::new(args.lsm_fifo_limit_bytes, None),
                        ),
                    })
                    .data_block_compression_policy(fjall_3::config::CompressionPolicy::new(&[
                        fjall_3::CompressionType::None,
                        match args.compression {
                            crate::args::Compression::None => fjall_3::CompressionType::None,
                            crate::args::Compression::Lz4 => fjall_3::CompressionType::Lz4,
                        },
                    ]));

                if let Some(hash_ratio) = args.lsm_data_block_hash_ratio {
                    create_opts = create_opts.data_block_hash_ratio_policy(
                        fjall_3::config::HashRatioPolicy::all(hash_ratio),
                    );
                }

                if let Some(bpk) = args.lsm_bloom_bpk {
                    create_opts = create_opts.filter_policy(fjall_3::config::FilterPolicy::all(
                        fjall_3::config::FilterPolicyEntry::Bloom(
                            fjall_3::config::BloomConstructionPolicy::BitsPerKey(bpk.into()),
                        ),
                    ));
                }

                if let Some(block_size) = args.lsm_block_size {
                    create_opts = create_opts
                        .data_block_size_policy(fjall_3::config::BlockSizePolicy::all(block_size));
                }

                // if args.value_size >= fjall_3::KV_SEPARATION_DEFAULT_THRESHOLD {
                //     create_opts = create_opts.with_kv_separation(Default::default());
                // }

                let tree = db.keyspace("data", create_opts).unwrap();

                GenericDatabase::Fjall3 { db, tree }
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
