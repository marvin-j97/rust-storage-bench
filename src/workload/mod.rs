mod feed;
mod monotonic;
mod monotonic_fixed;
mod ycsb;

use crate::{args::RunOptions, db::DatabaseWrapper};
use clap::ValueEnum;
use rand::{Rng, RngCore};
use serde::Serialize;
use std::{
    hash::Hasher,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

fn start_killer(sec: u16, signal: Arc<AtomicBool>) {
    log::debug!("Started killer");
    std::thread::sleep(Duration::from_secs(sec as u64));
    signal.store(true, Ordering::Relaxed);
}

// TODO: add more workloads
#[derive(Copy, Debug, Clone, ValueEnum, Serialize, PartialEq, Eq)]
#[clap(rename_all = "kebab_case")]
pub enum Workload {
    /// YCSB A: 50% reads and 50% updates
    YcsbA,

    /// YCSB B: 95% reads and 5% updates
    YcsbB,

    /// YCSB C: 100% reads
    YcsbC,

    /// Writes monotonic items and point reads them (in parallel)
    Monotonic,

    /// Writes monotonic items, then point reads them
    MonotonicFixed,

    /// Time series (increasing integer key, small value); write-only
    MonotonicWrite,

    /// (The company formerly known as Twitter)-style feed
    ///
    /// 1000 virtual users that post data to their feed
    /// - each user's profile is stored as userID#p
    /// - each post's key is: userID#f#cuid
    ///
    /// 90% a random virtual user's feed is queried by the last 10 items
    ///
    /// 10% a random virtual user will create a new post
    Feed,

    FixedUpdate,

    // /// Writes time series data, then point reads it zipfian-ly
    // FixedZipfian,

    // /// Writes time series data, then point reads it randomly
    // FixedRandom,
    /// Time series (increasing integer key, small value), read first data point
    ///
    /// Useful for testing a perfectly cached point read
    TimeseriesFirst,

    /// Time series (increasing integer key, small value), read latest 1'000 data points
    TimeseriesLatest,

    /// Uses siphash as key which makes writes very random; write-only
    RandomWrite,

    /// Uses siphash as key which makes writes very random, reads
    /// a pre-selected subset of keys randomly
    Random,

    /// Queue using a single producer and single consumer
    Queue,
}

pub fn run_workload(db: DatabaseWrapper, args: &RunOptions, finish_signal: Arc<AtomicBool>) {
    log::info!("Starting workload {:?}", args.workload);

    let fsync = args.fsync;

    match args.workload {
        /* Workload::FullScan => {
            println!("Ingesting data");

            // TODO: use args.value_size
            let mut buf = vec![0; 16];
            let mut rng = rand::thread_rng();

            for x in 0u128..5_000_000 {
                let key = x.to_be_bytes();
                rng.fill_bytes(&mut buf);

                db.insert(&key, &buf, fsync);
            }

            std::thread::spawn({
                println!("Starting scanner");

                move || loop {
                    db.scan_all();
                }
            });

            start_killer(args.seconds, finish_signal);
        } */
        Workload::YcsbA => {
            ycsb::a::run(args, &db, finish_signal);
        }
        Workload::YcsbB => {
            ycsb::b::run(args, &db, finish_signal);
        }
        Workload::YcsbC => {
            ycsb::c::run(args, &db, finish_signal);
        }
        Workload::MonotonicFixed => {
            monotonic_fixed::run(args, &db, finish_signal);
        }
        Workload::Monotonic => {
            monotonic::run(args, &db, finish_signal);
        }
        Workload::Feed => {
            feed::run(args, &db, finish_signal);
        }
        Workload::FixedUpdate => {
            let seconds = 30;
            let iterations = 100_000_000 / args.item_count;

            println!("Doing {iterations} iterations");

            let mut written_count = 0;
            let mut buf = vec![0; args.value_size as usize];

            for _ in 0..iterations {
                println!("Ingesting {} items", args.item_count);
                let item_count = args.item_count as u128;

                let mut rng = rand::thread_rng();

                let iter = (written_count..(written_count + item_count)).map(|x| {
                    rng.fill_bytes(&mut buf);
                    (x.to_be_bytes().to_vec(), buf.to_vec())
                });

                db.ingest(iter);
                written_count += item_count;

                let stopped = Arc::new(AtomicBool::default());

                let reader_handle = std::thread::spawn({
                    println!("We are at {written_count} - starting reader for {seconds}s");
                    let db = db.clone();
                    let stopped = stopped.clone();

                    move || {
                        let mut rng = rand::thread_rng();

                        for x in 0.. {
                            #[allow(clippy::collapsible_if)]
                            if x % 1_000 == 0 {
                                if stopped.load(Ordering::Relaxed) {
                                    return;
                                }
                            }

                            // TODO: support Zipfian reads
                            let x = rng.gen_range(0..written_count);

                            let key = x.to_be_bytes();
                            let prev = db.get(&key).unwrap();
                            let prev = prev.into_iter().map(|x| !x).collect::<Vec<_>>();
                            db.insert(&key, &prev, fsync);
                        }
                    }
                });

                std::thread::sleep(Duration::from_secs(seconds));
                stopped.store(true, Ordering::Relaxed);
                reader_handle.join().unwrap();
                println!("Killed reader");
            }

            finish_signal.store(true, Ordering::Relaxed);
        }
        Workload::TimeseriesFirst => {
            std::thread::spawn({
                log::debug!("Starting writer");
                let db = db.clone();

                move || {
                    for x in 0u128.. {
                        let key = x.to_be_bytes();
                        db.insert(&key, &key, fsync);
                    }
                }
            });

            std::thread::spawn({
                log::debug!("Starting reader");
                let db = db.clone();

                move || loop {
                    db.get(&0_u128.to_be_bytes());
                }
            });

            start_killer(args.seconds, finish_signal);
        }
        Workload::TimeseriesLatest => {
            let written_count = Arc::new(AtomicU64::new(0));

            std::thread::spawn({
                log::debug!("Starting writer");
                let db = db.clone();
                let written_count = written_count.clone();

                move || {
                    for x in 0u128.. {
                        let key = x.to_be_bytes();
                        db.insert(&key, &key, fsync);
                        written_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });

            std::thread::spawn({
                log::debug!("Starting reader");
                let db = db.clone();
                let written_count = written_count.clone();

                move || loop {
                    let max_key = written_count.load(Ordering::Relaxed).saturating_sub(1);

                    // TODO: implement range read, not last
                    if max_key > 1 {
                        db.last_len().unwrap();
                    }
                }
            });

            start_killer(args.seconds, finish_signal);
        }
        Workload::Queue => {
            std::thread::spawn({
                log::debug!("Starting writer");
                let db = db.clone();
                let mut buf = vec![0; args.value_size as usize];

                move || {
                    let mut rng = rand::thread_rng();

                    for seqno in 0u128.. {
                        rng.fill_bytes(&mut buf);
                        db.insert(&seqno.to_be_bytes(), &buf, fsync);
                    }
                }
            });

            let fsync = args.fsync;

            std::thread::spawn({
                log::debug!("Starting reader");
                let db = db.clone();

                move || {
                    use std::ops::Bound::{Excluded, Included};

                    let mut last_key;

                    loop {
                        if let Some((key, _)) = db.first() {
                            db.remove_unique(&key, fsync);
                            last_key = Some(key);
                            break;
                        }
                    }

                    for _ in 0.. {
                        let mut key_bytes = [0; 16];
                        key_bytes.copy_from_slice(last_key.as_deref().unwrap());

                        // NOTE: Make the range very tight
                        let upper_key = u128::from_be_bytes(key_bytes) + 1;
                        let upper_key: &[u8] = &upper_key.to_be_bytes();

                        let range = (Excluded(last_key.as_deref().unwrap()), Included(upper_key));

                        if let Some((key, _)) = db.range_first(range) {
                            db.remove_unique(&key, fsync);
                            last_key = Some(key);
                        }
                    }
                }
            });

            start_killer(args.seconds, finish_signal);
        }
        Workload::MonotonicWrite => {
            let mut buf = vec![0; args.value_size as usize];

            std::thread::spawn({
                log::debug!("Starting writer");
                let db = db.clone();

                move || {
                    let mut rng = rand::thread_rng();

                    for x in 0u128.. {
                        let key = x.to_be_bytes();
                        rng.fill_bytes(&mut buf);
                        db.insert(&key, &key, fsync);
                    }
                }
            });

            start_killer(args.seconds, finish_signal);
        }
        Workload::RandomWrite => {
            std::thread::spawn({
                log::debug!("Starting writer");
                let db = db.clone();
                let value_size = args.value_size as usize;

                move || {
                    let mut buf = vec![0; value_size];
                    let mut rng = rand::thread_rng();

                    for x in 0u128.. {
                        let mut hash = std::hash::DefaultHasher::default();
                        hash.write_u128(x);
                        let key = hash.finish().to_be_bytes();

                        rng.fill_bytes(&mut buf);

                        db.insert(&key, &buf, fsync);
                    }
                }
            });

            start_killer(args.seconds, finish_signal);
        }
        Workload::Random => {
            let value_size = args.value_size as usize;
            let mut buf = vec![0; value_size];

            {
                for i in 0..args.item_count {
                    let key = &{
                        let mut hash = std::hash::DefaultHasher::default();
                        hash.write_usize(i);
                        hash.finish().to_be_bytes()
                    };

                    db.insert(key, &buf, fsync)
                }
            }

            std::thread::spawn({
                log::debug!("Starting writer");
                let db = db.clone();

                move || {
                    let mut rng = rand::thread_rng();

                    for x in 0u128.. {
                        let mut hash = std::hash::DefaultHasher::default();
                        hash.write_u128(x);
                        let key = hash.finish().to_be_bytes();

                        rng.fill_bytes(&mut buf);

                        db.insert(&key, &buf, fsync);
                    }
                }
            });

            std::thread::spawn({
                log::debug!("Starting reader");
                let db = db.clone();
                let mut i = args.item_count;

                move || loop {
                    let key = &{
                        let mut hash = std::hash::DefaultHasher::default();
                        hash.write_usize(i);
                        hash.finish().to_be_bytes()
                    };
                    i += 1;

                    db.get(key);
                }
            });

            start_killer(args.seconds, finish_signal);
        }
    };
}
