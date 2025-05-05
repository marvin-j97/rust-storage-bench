// mod feed;
// mod monotonic;
// mod monotonic_fixed;
// mod read_write;
mod queue;
mod ycsb;

use crate::{
    args::{RunArgs, Workload},
    db::DatabaseWrapper,
};
use rand::{prelude::Distribution, Rng};
use std::{
    hash::Hasher,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use zipf::ZipfDistribution;

fn start_killer(sec: u16, signal: Arc<AtomicBool>) {
    log::debug!("Started killer");
    std::thread::sleep(Duration::from_secs(sec as u64));
    signal.store(true, Ordering::Relaxed);
}

/* // TODO: add more workloads
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

    /// Writes data and then reads and update it for a fixed amount of time
    /// The write and read distribution can be configured.
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

    /// Random writes; write-only
    RandomWrite,

    /// Read Write workload with 2 independent read and writer threads running in parallel.
    /// The write and read distribution can be configured.
    ReadWriteIndependent,

    /// Read Write workload with multiple actor threads.
    /// The configured number of threads are spawned and enter a loop with equal chances of performing a read or write.
    /// The write and read distribution can be configured.
    ReadWrite,

    /// Queue workload with 2 independent producer and consumer threads running in parallel.
    /// The producer will throttle if the consumer is not consuming fast enough.
    Queue,

    /// Queue workload with 2 independent producer and consumer threads running in parallel.
    QueueIndependent,
} */

pub fn run_workload(db: DatabaseWrapper, cmd: &RunArgs, finish_signal: Arc<AtomicBool>) {
    let args = &cmd.args;

    log::info!("Starting workload {:?}", cmd.workload);

    match &cmd.workload {
        Workload::Ycsb(ycsb_opts) => {
            use crate::args::YcsbType::{A, B, C};

            match ycsb_opts.r#type {
                A => {
                    ycsb::a::run(args, ycsb_opts, &db, finish_signal);
                }
                B => {
                    ycsb::b::run(args, ycsb_opts, &db, finish_signal);
                }
                C => {
                    ycsb::c::run(args, ycsb_opts, &db, finish_signal);
                }
            }
        }
        Workload::Queue(opts) => queue::run(args, opts, &db, finish_signal),
    }

    /* match args.workload {
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
            let exponent = args.zipf_exponent;

            println!("Doing {iterations} iterations");

            let mut written_count = 0;
            let mut buf = vec![0; args.value_size as usize];
            let random_key_distribution = args.write_random;
            let key_mapper = move |k: u64| -> u64 {
                if random_key_distribution {
                    k
                } else {
                    hash_key(k)
                }
            };
            let read_random = args.read_random;

            for _ in 0..iterations {
                println!("Ingesting {} items", args.item_count);
                let item_count = args.item_count as u64;

                let mut rng = rand::thread_rng();

                let iter = (written_count..(written_count + item_count)).map(|x| {
                    rng.fill_bytes(&mut buf);
                    let x = key_mapper(x);
                    ((x as u128).to_be_bytes().to_vec(), buf.to_vec())
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

                        while !stopped.load(Ordering::Relaxed) {
                            let x = if read_random {
                                rng.gen_range(0..written_count)
                            } else {
                                choose_zipf(&mut rng, exponent, written_count)
                            };

                            let key = (x as u128).to_be_bytes();
                            let prev = db.get(&key).unwrap();
                            let prev = prev.into_iter().map(|x| !x).collect::<Vec<_>>();
                            db.insert(&key, &prev, fsync, false);
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
                        db.insert(&key, &key, fsync, true);
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
                        db.insert(&key, &key, fsync, true);
                        written_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });

            std::thread::spawn({
                log::debug!("Starting reader");
                let db = db.clone();

                move || loop {
                    let written_count = written_count.load(Ordering::Relaxed);
                    let last_key_bytes = (written_count as u128).to_be_bytes();
                    let end_exclusive = std::ops::Bound::Excluded(&last_key_bytes[..]);
                    let len = db.range_len((std::ops::Bound::Unbounded, end_exclusive), true, 1000);
                    assert_eq!(len, written_count.min(1000) as usize);
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
                        db.insert(&key, &key, fsync, true);
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

                    for x in 0u64.. {
                        let key = (hash_key(x) as u128).to_be_bytes();
                        rng.fill_bytes(&mut buf);
                        db.insert(&key, &buf, fsync, true);
                    }
                }
            });

            start_killer(args.seconds, finish_signal);
        }
        Workload::ReadWriteIndependent => {
            read_write::run_independent(args, &db, finish_signal);
        }
        Workload::ReadWrite => {
            read_write::run(args, &db, finish_signal);
        }
    }; */
}

/// Hash a key using the default hasher.
/// This is used to make incremental keys look random.
pub fn hash_key(key: impl std::hash::Hash) -> u64 {
    let mut hasher = std::hash::DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

/// Choose a key using a zipfian distribution biased towards the
/// end of the range.
/// The key is chosen from the range [0, written_count), except for
/// the case when written_count is 0, in which case 0 is returned.
pub fn choose_zipf(rng: &mut impl Rng, exponent: f64, written_count: u64) -> u64 {
    if written_count == 0 {
        return 0;
    }
    written_count
        - ZipfDistribution::new(written_count as usize, exponent)
            .unwrap()
            .sample(rng) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::thread_rng;

    #[test]
    fn test_zipf_1_based() {
        let mut rng = thread_rng();
        let exponent = 1.0;
        let zipf = ZipfDistribution::new(1, exponent).unwrap();
        for _ in 0..10000 {
            let x = zipf.sample(&mut rng);
            assert_eq!(x, 1);
        }
    }

    #[test]
    fn test_choose_zipf() {
        let mut rng = thread_rng();
        for exponent in [0.001, 0.1, 1.0, 2.0, 3.0] {
            for written_count in [0, 1, 1000] {
                for _ in 0..10000 {
                    let x = choose_zipf(&mut rng, exponent, written_count);
                    if written_count == 0 {
                        assert_eq!(x, 0);
                    } else {
                        assert!(x < written_count);
                    }
                }
            }
        }
    }
}
