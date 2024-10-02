mod backend;

pub use backend::Backend;
use std::{
    path::Path,
    sync::{atomic::AtomicU64, Arc},
    time::Instant,
};

use crate::Args;

#[derive(Clone)]
pub enum GenericDatabase {
    Fjall {
        keyspace: fjall::Keyspace,
        db: fjall::PartitionHandle,
    },
    Sled(sled::Db),
    Bloodstone(bloodstone::Db),
}

#[derive(Clone)]
pub struct DatabaseWrapper {
    pub inner: GenericDatabase,

    pub write_ops: Arc<AtomicU64>,
    pub write_latency: Arc<AtomicU64>,
    pub written_bytes: Arc<AtomicU64>,

    pub point_read_ops: Arc<AtomicU64>,
    pub point_read_latency: Arc<AtomicU64>,
}

impl std::ops::Deref for DatabaseWrapper {
    type Target = GenericDatabase;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DatabaseWrapper {
    pub fn load<P: AsRef<Path>>(path: P, args: &Args) -> Self {
        let db = match args.backend {
            Backend::Bloodstone => GenericDatabase::Bloodstone(
                bloodstone::Config::new()
                    // .cache_capacity_bytes(args.cache_size as usize)
                    .path(path)
                    .open()
                    .unwrap(),
            ),
            Backend::Sled => GenericDatabase::Sled(
                sled::Config::new()
                    .path(path)
                    // .flush_every_ms(if args.fsync { None } else { Some(1_000) })
                    // .cache_capacity(args.cache_size)
                    .open()
                    .unwrap(),
            ),
            Backend::Fjall => {
                use fjall::PartitionCreateOptions;

                let config = fjall::Config::new(path);
                let keyspace = config.open().unwrap();

                let create_opts = PartitionCreateOptions::default();
                let db = keyspace.open_partition("data", create_opts).unwrap();

                /* let compaction_strategy = match args.lsm_compaction {
                    rust_storage_bench::LsmCompaction::Leveled => Strategy::Leveled(Leveled {
                        level_ratio: 8,
                        ..Default::default()
                    }),
                    rust_storage_bench::LsmCompaction::Tiered => {
                        Strategy::SizeTiered(SizeTiered::default())
                    }
                };

                let config = fjall::Config::new(&data_dir)
                    .max_write_buffer_size(256_000_000)
                    .fsync_ms(if args.fsync { None } else { Some(1_000) })
                    .block_cache(BlockCache::with_capacity_bytes(args.cache_size).into())
                    .blob_cache(fjall::BlobCache::with_capacity_bytes(args.cache_size).into());

                let create_opts = PartitionCreateOptions::default()
                    .block_size(args.lsm_block_size.into())
                    .compression(match args.lsm_compression {
                        rust_storage_bench::Compression::None => fjall::CompressionType::None,
                        rust_storage_bench::Compression::Lz4 => fjall::CompressionType::Lz4,
                        rust_storage_bench::Compression::Miniz => {
                            unimplemented!()
                            // fjall::CompressionType::Miniz(6)
                        }
                    })
                    // .max_memtable_size(8_000_000)
                    .manual_journal_persist(true)
                    .compaction_strategy(compaction_strategy); */

                /* let keyspace = config.open().unwrap();
                let db = if args.lsm_kv_separation {
                    keyspace
                        .open_partition("data", create_opts.with_kv_separation(Default::default()))
                        .unwrap()
                } else {
                    keyspace.open_partition("data", create_opts).unwrap()
                }; */

                GenericDatabase::Fjall { keyspace, db }
            }
        };

        DatabaseWrapper {
            inner: db,

            write_ops: Default::default(),
            write_latency: Default::default(),
            written_bytes: Default::default(),

            point_read_ops: Default::default(),
            point_read_latency: Default::default(),
            /*  read_ops: Default::default(),
            delete_ops: Default::default(),
            scan_ops: Default::default(),
            read_latency: Default::default(),
            write_latency: Default::default(),
            scan_latency: Default::default(),
            written_bytes: Default::default(),
            deleted_bytes: Default::default(),
            delete_latency: Default::default(), */
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let start = Instant::now();

        let item = match &self.inner {
            GenericDatabase::Fjall { keyspace: _, db } => {
                let item = db.get(key).unwrap();
                item.map(|x| x.to_vec())
            }
            GenericDatabase::Sled(db) => {
                let item = db.get(key).unwrap();
                item.map(|x| x.to_vec())
            }
            GenericDatabase::Bloodstone(db) => {
                let item = db.get(key).unwrap();
                item.map(|x| x.to_vec())
            }
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

    pub fn insert(&self, key: &[u8], value: &[u8], durable: bool) {
        let start = Instant::now();

        match &self.inner {
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
            GenericDatabase::Sled(db) => {
                db.insert(key, value).unwrap();

                if durable {
                    db.flush().unwrap();
                }
            }
            GenericDatabase::Bloodstone(db) => {
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
            }
            _ => {
                unimplemented!()
            }
        }

        self.write_latency.fetch_add(
            start.elapsed().as_nanos() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        self.write_ops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        self.written_bytes.fetch_add(
            (key.len() + value.len()) as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
    }
}
