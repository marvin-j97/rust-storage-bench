use crate::Args;
use nebari::{io::fs::StdFile, tree::Unversioned};
use redb::TableDefinition;
use std::{
    sync::{atomic::AtomicU64, Arc},
    time::Instant,
};

#[derive(Clone)]
pub struct DatabaseWrapper {
    pub inner: GenericDatabase,
    pub write_ops: Arc<AtomicU64>,
    pub read_ops: Arc<AtomicU64>,
    pub delete_ops: Arc<AtomicU64>,
    pub scan_ops: Arc<AtomicU64>,

    pub write_latency: Arc<AtomicU64>,
    pub read_latency: Arc<AtomicU64>,
}

impl std::ops::Deref for DatabaseWrapper {
    type Target = GenericDatabase;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Clone)]
pub enum GenericDatabase {
    Fjall {
        keyspace: fjall::Keyspace,
        db: fjall::PartitionHandle,
    },
    Sled(sled::Db),
    // Bloodstone(bloodstone::Db),
    Jamm(jammdb::DB),
    Persy(persy::Persy),
    Redb(Arc<redb::Database>),
    Nebari {
        roots: nebari::Roots<StdFile>,
        tree: nebari::Tree<Unversioned, StdFile>,
    },

    #[cfg(feature = "heed")]
    Heed {
        db: heed::Database<heed::types::Bytes, heed::types::Bytes>,
        env: heed::Env,
    },

    #[cfg(feature = "rocksdb")]
    RocksDb(Arc<rocksdb::DB>),
}

const TABLE: TableDefinition<&[u8], Vec<u8>> = TableDefinition::new("data");

impl DatabaseWrapper {
    pub fn insert(&self, key: &[u8], value: &[u8], durable: bool, args: Arc<Args>) {
        match &self.inner {
            #[cfg(feature = "rocksdb")]
            GenericDatabase::RocksDb(db) => {
                let start = Instant::now();

                db.put(key, value).unwrap();

                if durable {
                    db.flush_wal(true).unwrap();
                }

                self.write_latency.fetch_add(
                    start.elapsed().as_micros() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }

            #[cfg(feature = "heed")]
            GenericDatabase::Heed { env, db } => {
                let start = Instant::now();

                let mut wtxn = env.write_txn().unwrap();
                db.put(&mut wtxn, key, value).unwrap();

                wtxn.commit().unwrap();

                self.write_latency.fetch_add(
                    start.elapsed().as_micros() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
            GenericDatabase::Nebari { roots: _, tree } => {
                if !durable {
                    log::warn!("WARNING: Nebari does not support eventual durability");
                }

                let key = key.to_vec();
                let value = key.to_vec();

                let start = Instant::now();

                tree.set(key, value).unwrap();

                self.write_latency.fetch_add(
                    start.elapsed().as_micros() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
            GenericDatabase::Fjall { keyspace, db } => {
                let start = Instant::now();

                db.insert(key, value).unwrap();

                if durable {
                    keyspace.persist(fjall::FlushMode::SyncAll).unwrap();
                }

                self.write_latency.fetch_add(
                    start.elapsed().as_micros() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
            GenericDatabase::Sled(db) => {
                let start = Instant::now();

                db.insert(key, value).unwrap();

                if durable {
                    db.flush().unwrap();
                }

                self.write_latency.fetch_add(
                    start.elapsed().as_micros() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
            // GenericDatabase::Bloodstone(db) => {
            //     let start = Instant::now();

            //     db.insert(key, value).unwrap();

            //     if durable {
            //         db.flush().unwrap();
            //     } else if args.sled_flush {
            //         // NOTE: TODO: OOM Workaround
            //         // Intermittenly flush sled to keep memory usage sane
            //         // This is hopefully a temporary workaround
            //         if self.write_ops.load(std::sync::atomic::Ordering::Relaxed) % 5_000_000 == 0 {
            //             db.flush().unwrap();
            //         }
            //     }

            //     self.write_latency.fetch_add(
            //         start.elapsed().as_micros() as u64,
            //         std::sync::atomic::Ordering::Relaxed,
            //     );
            // }
            GenericDatabase::Jamm(db) => {
                if !durable {
                    log::warn!("WARNING: JammDB does not support eventual durability",);
                }

                let start = Instant::now();

                let tx = db.tx(true).unwrap();
                let bucket = tx.get_bucket("data").unwrap();
                bucket.put(key, value).unwrap();
                tx.commit().unwrap();

                self.write_latency.fetch_add(
                    start.elapsed().as_micros() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
            GenericDatabase::Persy(db) => {
                use persy::{PersyId, TransactionConfig};

                let key = String::from_utf8_lossy(key);
                let key = key.to_string();

                let start = Instant::now();

                let mut tx = db
                    .begin_with(TransactionConfig::new().set_background_sync(!durable))
                    .unwrap();
                let id = tx.insert("data", value).unwrap();

                tx.put::<String, PersyId>("primary", key, id).unwrap();
                let prepared = tx.prepare().unwrap();

                prepared.commit().unwrap();

                self.write_latency.fetch_add(
                    start.elapsed().as_micros() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
            GenericDatabase::Redb(db) => {
                use redb::Durability::{Eventual, Immediate};

                let start = Instant::now();

                let mut write_txn = db.begin_write().unwrap();

                write_txn.set_durability(if durable { Immediate } else { Eventual });

                {
                    let mut table = write_txn.open_table(TABLE).unwrap();
                    table.insert(key, value.to_vec()).unwrap();
                }
                write_txn.commit().unwrap();

                self.write_latency.fetch_add(
                    start.elapsed().as_micros() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
        }

        self.write_ops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let start = Instant::now();

        let item = match &self.inner {
            #[cfg(feature = "rocksdb")]
            GenericDatabase::RocksDb(db) => db.get(key).unwrap().map(|x| x.to_vec()),

            #[cfg(feature = "heed")]
            GenericDatabase::Heed { db, env } => {
                let rtxn = env.read_txn().unwrap();
                let ret = db.get(&rtxn, key).unwrap();
                ret.map(|x| x.to_vec())
            }

            GenericDatabase::Nebari { roots: _, tree } => {
                let item = tree.get(key).unwrap();
                item.map(|x| x.to_vec())
            }
            GenericDatabase::Fjall { keyspace: _, db } => db.get(key).unwrap().map(|x| x.to_vec()),
            GenericDatabase::Sled(db) => db.get(key).unwrap().map(|x| x.to_vec()),
            // GenericDatabase::Bloodstone(db) => db.get(key).unwrap().map(|x| x.to_vec()),
            GenericDatabase::Jamm(db) => {
                let tx = db.tx(false).unwrap();
                let bucket = tx.get_bucket("data").unwrap();
                bucket.get(key).map(|item| item.kv().value().into())
            }
            GenericDatabase::Persy(db) => {
                let key = String::from_utf8_lossy(key);

                let mut read_id = db
                    .get::<String, persy::PersyId>("primary", &key.to_string())
                    .unwrap();
                if let Some(id) = read_id.next() {
                    db.read("data", &id).unwrap()
                } else {
                    None
                }
            }
            GenericDatabase::Redb(db) => {
                let read_txn = db.begin_read().unwrap();
                let table = read_txn.open_table(TABLE).unwrap();
                table.get(key).unwrap().map(|x| x.value())
            }
        };

        self.read_latency.fetch_add(
            start.elapsed().as_micros() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        self.read_ops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        item
    }
}
