use crate::Args;
use nebari::{io::fs::StdFile, tree::Unversioned};
use redb::{ReadableTable, TableDefinition};
use std::sync::{atomic::AtomicU64, Arc};

#[derive(Clone)]
pub struct DatabaseWrapper {
    pub inner: GenericDatabase,
    pub write_ops: Arc<AtomicU64>,
    pub read_ops: Arc<AtomicU64>,
    pub delete_ops: Arc<AtomicU64>,
    pub scan_ops: Arc<AtomicU64>,
}

impl std::ops::Deref for DatabaseWrapper {
    type Target = GenericDatabase;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Clone)]
pub enum GenericDatabase {
    MyLsmTree(lsm_tree::Tree),
    Sled(sled::Db),
    Bloodstone(bloodstone::Db),
    Jamm(jammdb::DB),
    Persy(persy::Persy),
    Redb(Arc<redb::Database>),
    Nebari {
        roots: nebari::Roots<StdFile>,
        tree: nebari::Tree<Unversioned, StdFile>,
    },
}

const TABLE: TableDefinition<&[u8], Vec<u8>> = TableDefinition::new("data");

impl DatabaseWrapper {
    pub fn insert(&self, key: &[u8], value: &[u8], args: Arc<Args>) {
        self.write_ops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        match &self.inner {
            GenericDatabase::Nebari { roots: _, tree } => {
                let key = key.to_vec();
                let value = key.to_vec();
                tree.set(key, value).unwrap()
            }
            GenericDatabase::MyLsmTree(db) => {
                db.insert(key, value).unwrap();

                if args.fsync {
                    db.flush().unwrap();
                }
            }
            GenericDatabase::Sled(db) => {
                db.insert(key, value).unwrap();

                if args.fsync {
                    db.flush().unwrap();
                }
            }
            GenericDatabase::Bloodstone(db) => {
                db.insert(key, value).unwrap();

                if args.fsync {
                    db.flush().unwrap();
                } else if args.sled_flush {
                    // NOTE: TODO: OOM Workaround
                    // Intermittenly flush sled to keep memory usage sane
                    // This is hopefully a temporary workaround
                    if self.write_ops.load(std::sync::atomic::Ordering::Relaxed) % 5_000_000 == 0 {
                        db.flush().unwrap();
                    }
                }
            }
            GenericDatabase::Jamm(db) => {
                // TODO: jammdb durability/fsync...?

                let tx = db.tx(true).unwrap();
                let bucket = tx.get_bucket("data").unwrap();
                bucket.put(key, value).unwrap();
                tx.commit().unwrap();
            }
            GenericDatabase::Persy(db) => {
                use persy::{PersyId, TransactionConfig};

                let mut tx = db
                    .begin_with(TransactionConfig::new().set_background_sync(!args.fsync))
                    .unwrap();
                let id = tx.insert("data", value).unwrap();

                let mut buf = [0; 8];
                buf.copy_from_slice(key);
                let k = u64::from_be_bytes(buf);

                tx.put::<u64, PersyId>("primary", k, id).unwrap();
                let prepared = tx.prepare().unwrap();

                prepared.commit().unwrap();
            }
            GenericDatabase::Redb(db) => {
                use redb::Durability::{Eventual, Immediate};

                let mut write_txn = db.begin_write().unwrap();

                write_txn.set_durability(if args.fsync { Immediate } else { Eventual });

                {
                    let mut table = write_txn.open_table(TABLE).unwrap();
                    table.insert(key, value.to_vec()).unwrap();
                }
                write_txn.commit().unwrap();
            }
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.read_ops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        match &self.inner {
            GenericDatabase::Nebari { roots: _, tree } => {
                let item = tree.get(key).unwrap();
                item.map(|x| x.to_vec())
            }
            GenericDatabase::MyLsmTree(db) => db.get(key).unwrap().map(|x| x.to_vec()),
            GenericDatabase::Sled(db) => db.get(key).unwrap().map(|x| x.to_vec()),
            GenericDatabase::Bloodstone(db) => db.get(key).unwrap().map(|x| x.to_vec()),
            GenericDatabase::Jamm(db) => {
                let tx = db.tx(false).unwrap();
                let bucket = tx.get_bucket("data").unwrap();
                Some(bucket.get(key).unwrap().kv().value().into())
            }
            GenericDatabase::Persy(db) => {
                let mut buf = [0; 8];
                buf.copy_from_slice(key);
                let k = u64::from_be_bytes(buf);

                let mut read_id = db.get::<u64, persy::PersyId>("primary", &k).unwrap();
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
        }
    }
}
