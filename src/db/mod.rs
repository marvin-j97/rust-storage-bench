mod backend;

pub use backend::Backend;
use std::{
    sync::{atomic::AtomicU64, Arc},
    time::Instant,
};

#[derive(Clone)]
pub enum GenericDatabase {
    Fjall {
        keyspace: fjall::Keyspace,
        db: fjall::PartitionHandle,
    },
    Sled(sled::Db),
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
