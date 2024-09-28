use crate::Args;
use nebari::{io::fs::StdFile, tree::Unversioned};
use persy::TxIndexIter;
use redb::{ReadableTable, TableDefinition};
use std::{
    ops::RangeBounds,
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
    pub delete_latency: Arc<AtomicU64>,
    pub scan_latency: Arc<AtomicU64>,

    pub written_bytes: Arc<AtomicU64>,
    pub deleted_bytes: Arc<AtomicU64>,
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
    Bloodstone(bloodstone::Db),
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

pub const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("data");

impl DatabaseWrapper {
    pub fn last(&self) -> Option<Vec<u8>> {
        let start = Instant::now();

        let v = match &self.inner {
            GenericDatabase::Fjall { db, .. } => {
                db.last_key_value().unwrap().map(|(_, v)| v.to_vec())
            }
            GenericDatabase::Sled(db) => db.last().unwrap().map(|(_, v)| v.to_vec()),
            GenericDatabase::Redb(db) => {
                let tx = db.begin_read().unwrap();

                let table = tx.open_table(TABLE).unwrap();

                let item = table.last().unwrap();

                item.map(|(_, v)| v.value().to_vec())
            }
            GenericDatabase::Heed { db, env } => {
                /* let tx = db.begin_read().unwrap();

                let table = tx.open_table(TABLE).unwrap();

                let item = table.last().unwrap();

                item.map(|(_, v)| v.value().to_vec()) */

                let rtxn = env.read_txn().unwrap();

                let item = db.last(&rtxn);

                item.unwrap().map(|(_, v)| v.to_vec())
            }
            GenericDatabase::Persy(db) => {
                use persy::PersyId;

                let (_, mut nexty) = db
                    .range::<String, PersyId, _>("data", ..)
                    .unwrap()
                    .next_back()
                    .unwrap();

                //let mut iter: TxIndexIter<String, PersyId> = db.range("data", ..).unwrap();

                /* let (_, mut nexty) = iter.next_back().unwrap();
                let nexty = nexty.next();
                println!("{nexty:?}");

                db.read("data", &nexty.unwrap()).unwrap() */

                db.read("data", &nexty.next().unwrap()).unwrap()
            }
            _ => unimplemented!(),
        };

        self.scan_ops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        self.scan_latency.fetch_add(
            start.elapsed().as_nanos() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        v
    }

    // NOTE: Avoid memory allocations of range
    pub fn range_len<'a, R: RangeBounds<&'a [u8]>>(&'a self, range: R, rev: bool) -> usize {
        let start = Instant::now();

        let v = match &self.inner {
            GenericDatabase::Fjall { db, .. } => {
                let iter = db.range(range);

                if rev {
                    iter.rev()
                        .map(|kv| {
                            let (k, v) = kv.unwrap();
                            (k.to_vec(), v.to_vec())
                        })
                        .count()
                } else {
                    iter.map(|kv| {
                        let (k, v) = kv.unwrap();
                        (k.to_vec(), v.to_vec())
                    })
                    .count()
                }
            }
            GenericDatabase::Sled(db) => {
                let iter = db.range(range);

                if rev {
                    iter.rev()
                        .map(|kv| {
                            let (k, v) = kv.unwrap();
                            (k.to_vec(), v.to_vec())
                        })
                        .count()
                } else {
                    iter.map(|kv| {
                        let (k, v) = kv.unwrap();
                        (k.to_vec(), v.to_vec())
                    })
                    .count()
                }
            }
            GenericDatabase::Redb(db) => {
                let tx = db.begin_read().unwrap();

                let table = tx.open_table(TABLE).unwrap();

                let iter = table.range(range).unwrap();

                if rev {
                    iter.rev()
                        .map(|x| {
                            let (k, v) = x.unwrap();
                            let k: Vec<u8> = k.value().into();
                            let v: Vec<u8> = v.value().into();
                            (k, v)
                        })
                        .count()
                } else {
                    iter.map(|x| {
                        let (k, v) = x.unwrap();
                        let k: Vec<u8> = k.value().into();
                        let v: Vec<u8> = v.value().into();
                        (k, v)
                    })
                    .count()
                }
            }
            _ => unimplemented!(),
        };

        self.scan_ops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        self.scan_latency.fetch_add(
            start.elapsed().as_nanos() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        v
    }

    pub fn range<'a, R: RangeBounds<&'a [u8]>>(
        &'a self,
        range: R,
        rev: bool,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let start = Instant::now();

        let v = match &self.inner {
            GenericDatabase::Fjall { db, .. } => {
                let iter = db.range(range);

                if rev {
                    iter.rev()
                        .map(|kv| {
                            let (k, v) = kv.unwrap();
                            (k.to_vec(), v.to_vec())
                        })
                        .collect()
                } else {
                    iter.map(|kv| {
                        let (k, v) = kv.unwrap();
                        (k.to_vec(), v.to_vec())
                    })
                    .collect()
                }
            }
            GenericDatabase::Sled(db) => {
                let iter = db.range(range);

                if rev {
                    iter.rev()
                        .map(|kv| {
                            let (k, v) = kv.unwrap();
                            (k.to_vec(), v.to_vec())
                        })
                        .collect()
                } else {
                    iter.map(|kv| {
                        let (k, v) = kv.unwrap();
                        (k.to_vec(), v.to_vec())
                    })
                    .collect()
                }
            }
            GenericDatabase::Redb(db) => {
                let tx = db.begin_read().unwrap();

                let table = tx.open_table(TABLE).unwrap();

                let iter = table.range(range).unwrap();

                if rev {
                    iter.rev()
                        .map(|x| {
                            let (k, v) = x.unwrap();
                            let k: Vec<u8> = k.value().into();
                            let v: Vec<u8> = v.value().into();
                            (k, v)
                        })
                        .collect()
                } else {
                    iter.map(|x| {
                        let (k, v) = x.unwrap();
                        let k: Vec<u8> = k.value().into();
                        let v: Vec<u8> = v.value().into();
                        (k, v)
                    })
                    .collect()
                }
            }
            _ => unimplemented!(),
        };

        self.scan_ops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        self.scan_latency.fetch_add(
            start.elapsed().as_nanos() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        v
    }

    pub fn prefix<'a>(
        &'a self,
        prefix: &'a [u8],
        rev: bool,
        take: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let start = Instant::now();

        let v = match &self.inner {
            GenericDatabase::Fjall { db, .. } => {
                let iter = db.prefix(prefix);

                if rev {
                    iter.rev()
                        .take(take)
                        .map(|kv| {
                            let (k, v) = kv.unwrap();
                            (k.to_vec(), v.to_vec())
                        })
                        .collect()
                } else {
                    iter.take(take)
                        .map(|kv| {
                            let (k, v) = kv.unwrap();
                            (k.to_vec(), v.to_vec())
                        })
                        .collect()
                }
            }
            GenericDatabase::Sled(db) => {
                let iter = db.scan_prefix(prefix);

                if rev {
                    iter.rev()
                        .take(take)
                        .map(|kv| {
                            let (k, v) = kv.unwrap();
                            (k.to_vec(), v.to_vec())
                        })
                        .collect()
                } else {
                    iter.take(take)
                        .map(|kv| {
                            let (k, v) = kv.unwrap();
                            (k.to_vec(), v.to_vec())
                        })
                        .collect()
                }
            }
            GenericDatabase::Bloodstone(db) => {
                let iter = db.scan_prefix(prefix);

                if rev {
                    iter.rev()
                        .take(take)
                        .map(|kv| {
                            let (k, v) = kv.unwrap();
                            (k.to_vec(), v.to_vec())
                        })
                        .collect()
                } else {
                    iter.take(take)
                        .map(|kv| {
                            let (k, v) = kv.unwrap();
                            (k.to_vec(), v.to_vec())
                        })
                        .collect()
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
                        .collect()
                } else {
                    iter.map(|x| {
                        let (k, v) = x.unwrap();
                        let k: Vec<u8> = k.value().into();
                        let v: Vec<u8> = v.value().into();
                        (k, v)
                    })
                    .take_while(|(k, _)| k.starts_with(prefix))
                    .take(take)
                    .collect()
                }
            }

            #[cfg(feature = "heed")]
            GenericDatabase::Heed { db, env } => {
                let tx = env.read_txn().unwrap();

                if rev {
                    let iter = db.rev_range(&tx, &..).unwrap();

                    iter.take(take)
                        .map(|kv| {
                            let (k, v) = kv.unwrap();
                            (k.to_vec(), v.to_vec())
                        })
                        .collect()
                } else {
                    let iter = db.range(&tx, &..).unwrap();

                    iter.take(take)
                        .map(|kv| {
                            let (k, v) = kv.unwrap();
                            (k.to_vec(), v.to_vec())
                        })
                        .collect()
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

                iter.take(take)
                    .map(|kv| {
                        let (k, v) = kv.unwrap();
                        (k.to_vec(), v.to_vec())
                    })
                    .collect()
            }
            _ => unimplemented!(),
        };

        self.scan_ops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        self.scan_latency.fetch_add(
            start.elapsed().as_nanos() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        v
    }

    pub fn remove(&self, key: &[u8], val_len: u64, durable: bool) {
        let start = Instant::now();

        match &self.inner {
            GenericDatabase::Fjall { keyspace, db } => {
                db.remove(key).unwrap();

                if durable {
                    // NOTE: RocksDB also uses fsyncmetadata by default
                    keyspace.persist(fjall::PersistMode::SyncData).unwrap();
                }
            }
            GenericDatabase::Sled(db) => {
                db.remove(key).unwrap();

                if durable {
                    db.flush().unwrap();
                }
            }
            GenericDatabase::Redb(db) => {
                use redb::Durability::{Immediate, None};

                let mut write_txn = db.begin_write().unwrap();

                write_txn.set_durability(if durable { Immediate } else { None });

                {
                    let mut table = write_txn.open_table(TABLE).unwrap();
                    table.remove(key).unwrap();
                }

                write_txn.commit().unwrap();
            }
            GenericDatabase::Persy(db) => {
                use persy::{PersyId, TransactionConfig};

                let key = String::from_utf8_lossy(key);
                let key = key.to_string();

                let mut tx = db
                    .begin_with(TransactionConfig::new().set_background_sync(!durable))
                    .unwrap();

                // TODO: help
                tx.remove::<String, PersyId>("primary", key.clone(), None)
                    .unwrap();

                let prepared = tx.prepare().unwrap();

                prepared.commit().unwrap();
            }
            _ => unimplemented!(),
        }

        self.delete_latency.fetch_add(
            start.elapsed().as_nanos() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        self.deleted_bytes
            .fetch_add(val_len, std::sync::atomic::Ordering::Relaxed);

        self.delete_ops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn insert(&self, key: &[u8], value: &[u8], durable: bool, args: Arc<Args>) {
        match &self.inner {
            #[cfg(feature = "rocksdb")]
            GenericDatabase::RocksDb(db) => {
                let start = Instant::now();

                db.put(key, value).unwrap();

                db.flush_wal(durable).unwrap();

                self.write_latency.fetch_add(
                    start.elapsed().as_nanos() as u64,
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
                    start.elapsed().as_nanos() as u64,
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
                    start.elapsed().as_nanos() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
            GenericDatabase::Fjall { keyspace, db } => {
                let start = Instant::now();

                db.insert(key, value).unwrap();

                keyspace
                    .persist(if durable {
                        // NOTE: RocksDB uses fsyncdata by default, too
                        fjall::PersistMode::SyncData
                    } else {
                        fjall::PersistMode::Buffer
                    })
                    .unwrap();

                self.write_latency.fetch_add(
                    start.elapsed().as_nanos() as u64,
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
                    start.elapsed().as_nanos() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
            GenericDatabase::Bloodstone(db) => {
                let start = Instant::now();

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

                self.write_latency.fetch_add(
                    start.elapsed().as_nanos() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
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
                    start.elapsed().as_nanos() as u64,
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
                    start.elapsed().as_nanos() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
            GenericDatabase::Redb(db) => {
                use redb::Durability::{Immediate, None};

                let start = Instant::now();

                let mut write_txn = db.begin_write().unwrap();

                write_txn.set_durability(if durable { Immediate } else { None });

                {
                    let mut table = write_txn.open_table(TABLE).unwrap();
                    table.insert(key, value).unwrap();
                }
                write_txn.commit().unwrap();

                self.write_latency.fetch_add(
                    start.elapsed().as_nanos() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
        }

        self.write_ops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        self.written_bytes.fetch_add(
            (key.len() + value.len()) as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let start = Instant::now();

        let item = match &self.inner {
            #[cfg(feature = "rocksdb")]
            GenericDatabase::RocksDb(db) => {
                let item = db.get(key).unwrap();

                item.map(|x| x.to_vec())
            }

            #[cfg(feature = "heed")]
            GenericDatabase::Heed { db, env } => {
                let rtxn = env.read_txn().unwrap();
                let item = db.get(&rtxn, key).unwrap();

                item.map(|x| x.to_vec())
            }

            GenericDatabase::Nebari { roots: _, tree } => {
                let item = tree.get(key).unwrap();

                item.map(|x| x.to_vec())
            }
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
            GenericDatabase::Jamm(db) => {
                let tx = db.tx(false).unwrap();
                let bucket = tx.get_bucket("data").unwrap();

                let item = bucket.get(key);

                item.map(|item| item.kv().value().into())
            }
            GenericDatabase::Persy(db) => {
                let key = String::from_utf8_lossy(key);

                let mut read_id = db
                    .get::<String, persy::PersyId>("primary", &key.to_string())
                    .unwrap();

                let nexty = read_id.next();

                if let Some(id) = nexty {
                    db.read("data", &id).unwrap()
                } else {
                    None
                }
            }
            GenericDatabase::Redb(db) => {
                let read_txn = db.begin_read().unwrap();
                let table = read_txn.open_table(TABLE).unwrap();

                let item = table.get(key).unwrap();

                item.map(|x| x.value().into())
            }
        };

        self.read_latency.fetch_add(
            start.elapsed().as_nanos() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        self.read_ops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        item
    }

    // NOTE: Purposefully don't use DB implementation of contains_key
    // because that may be faster than loading the entire value
    // (e.g. in Fjall key-value separated mode)
    // We want to still get the whole value, but not have to convert it
    // to Vec because that is expensive
    pub fn len_of_value(&self, key: &[u8]) -> Option<usize> {
        let start = Instant::now();

        let len = match &self.inner {
            #[cfg(feature = "rocksdb")]
            GenericDatabase::RocksDb(db) => {
                let item = db.get(key).unwrap();
                item.map(|x| x.len())
            }

            #[cfg(feature = "heed")]
            GenericDatabase::Heed { db, env } => {
                let rtxn = env.read_txn().unwrap();
                let item = db.get(&rtxn, key);
                item.unwrap().map(|x| x.len())
            }

            GenericDatabase::Nebari { roots: _, tree } => {
                let item = tree.get(key).unwrap();
                item.map(|x| x.len())
            }
            GenericDatabase::Fjall { keyspace: _, db } => {
                let item = db.get(key).unwrap();
                item.map(|x| x.len())
            }
            GenericDatabase::Sled(db) => {
                let item = db.get(key).unwrap();
                item.map(|x| x.len())
            }
            GenericDatabase::Bloodstone(db) => {
                let item = db.get(key).unwrap();
                item.map(|x| x.len())
            }
            GenericDatabase::Jamm(db) => {
                let tx = db.tx(false).unwrap();
                let bucket = tx.get_bucket("data").unwrap();

                let item = bucket.get(key);

                item.map(|x| x.kv().value().len())
            }
            GenericDatabase::Persy(db) => {
                let key = String::from_utf8_lossy(key);

                let mut read_id = db
                    .get::<String, persy::PersyId>("primary", &key.to_string())
                    .unwrap();

                let nexty = read_id.next();

                if let Some(id) = nexty {
                    db.read("data", &id).unwrap().map(|x| x.len())
                } else {
                    None
                }
            }
            GenericDatabase::Redb(db) => {
                let read_txn = db.begin_read().unwrap();
                let table = read_txn.open_table(TABLE).unwrap();

                let item = table.get(key).unwrap();
                item.map(|x| x.value().len())
            }
        };

        self.read_latency.fetch_add(
            start.elapsed().as_nanos() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        self.read_ops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        len
    }
}
