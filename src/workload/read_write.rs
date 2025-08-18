use super::{hash_key, start_killer};
use crate::args::RunOptions;
use crate::db::DatabaseWrapper;
use crate::workload::choose_zipf;
use rand::{Rng, RngCore};
use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
};

pub(crate) fn run(args: &RunOptions, db: &DatabaseWrapper, finish_signal: Arc<AtomicIsize>) {
    let item_count = args.item_count as u64;
    let value_size = args.value_size as usize;
    let exponent = args.zipf_exponent;
    let fsync = args.fsync;
    let random_key_distribution = args.write_random;
    let read_random = args.read_random;
    let key_mapper = move |k: u64| -> u64 {
        if random_key_distribution {
            k
        } else {
            hash_key(k)
        }
    };

    {
        let mut rng = crate::random::thread_rng();
        let mut buf = vec![0; value_size];
        for i in 0..item_count {
            let key = &key_mapper(i).to_be_bytes();
            rng.fill_bytes(&mut buf);

            db.insert(key, &buf, fsync, true);
        }
    }

    let written_count = Arc::new(AtomicU64::new(item_count));
    let disjoint = Arc::new(Mutex::new(BTreeSet::new()));
    let next_write = Arc::new(AtomicU64::new(item_count));
    for _ in 0..args.threads {
        std::thread::spawn({
            log::debug!("Starting writer");
            let mut buf = vec![0; value_size];
            let db = db.clone();
            let written_count = written_count.clone();
            let next_write = next_write.clone();
            let disjoint = disjoint.clone();

            move || {
                let mut rng = crate::random::thread_rng();
                loop {
                    if rng.gen_bool(0.5) {
                        let x = next_write.fetch_add(1, Ordering::SeqCst);
                        let key = &key_mapper(x).to_be_bytes();
                        rng.fill_bytes(&mut buf);
                        db.insert(key, &buf, fsync, true);
                        if let Err(count) = written_count.compare_exchange(
                            x,
                            x + 1,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        ) {
                            let mut updated_count = count;
                            let mut disjoint = disjoint.lock().unwrap();
                            disjoint.insert(x + 1);
                            while let Some(&next) = disjoint.first() {
                                if next == updated_count + 1 {
                                    updated_count = next;
                                    disjoint.pop_first();
                                } else {
                                    break;
                                }
                            }
                            drop(disjoint);
                            if updated_count != count {
                                written_count.store(updated_count, Ordering::Release);
                            }
                        }
                    } else {
                        let written_count = written_count.load(Ordering::Acquire);
                        let x = if read_random {
                            rng.gen_range(0..written_count)
                        } else {
                            choose_zipf(&mut rng, exponent, written_count)
                        };
                        let key = &key_mapper(x).to_be_bytes();
                        db.get(key);
                    }
                }
            }
        });
    }

    start_killer(args.seconds, finish_signal);
}

pub(crate) fn run_independent(
    args: &RunOptions,
    db: &DatabaseWrapper,
    finish_signal: Arc<AtomicIsize>,
) {
    let fsync = args.fsync;
    let item_count = args.item_count as u64;
    let value_size = args.value_size as usize;
    let random_key_distribution = args.write_random;
    let read_random = args.read_random;
    let exponent = args.zipf_exponent;
    let key_mapper = move |k: u64| -> u64 {
        if random_key_distribution {
            k
        } else {
            hash_key(k)
        }
    };

    {
        let mut rng = crate::random::thread_rng();
        let mut buf = vec![0; value_size];
        for i in 0..item_count {
            let key = &key_mapper(i).to_be_bytes();
            rng.fill_bytes(&mut buf);
            db.insert(key, &buf, fsync, true);
        }
    }

    let written_count = Arc::new(AtomicU64::new(item_count));
    std::thread::spawn({
        log::debug!("Starting writer");
        let db = db.clone();
        let written_count = written_count.clone();

        move || {
            let mut rng = crate::random::thread_rng();
            let mut buf = vec![0; value_size];

            for x in item_count.. {
                let key = &key_mapper(x).to_be_bytes();
                rng.fill_bytes(&mut buf);
                db.insert(key, &buf, fsync, true);
                written_count.fetch_add(1, Ordering::Relaxed);
            }
        }
    });

    std::thread::spawn({
        log::debug!("Starting reader");
        let db = db.clone();

        move || {
            let mut rng = crate::random::thread_rng();
            loop {
                let written_count = written_count.load(Ordering::Relaxed);
                let x = if read_random {
                    rng.gen_range(0..written_count)
                } else {
                    choose_zipf(&mut rng, exponent, written_count)
                };
                let key = &key_mapper(x).to_be_bytes();
                db.get(key);
            }
        }
    });

    start_killer(args.seconds, finish_signal);
}
