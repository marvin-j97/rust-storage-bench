pub(crate) mod column_store;
pub(crate) mod event_log;
pub(crate) mod feed;
pub(crate) mod queue;
pub(crate) mod read_write;
pub(crate) mod timeseries;
pub(crate) mod webtable;
pub(crate) mod ycsb;

use crate::{
    args::{RunArgs, Workload},
    db::DatabaseWrapper,
};
use rand::{prelude::Distribution, Rng};
use std::{
    hash::Hasher,
    sync::{
        atomic::{AtomicIsize, Ordering},
        Arc,
    },
    time::Duration,
};
use zipf::ZipfDistribution;

fn start_killer(sec: u32, signal: Arc<AtomicIsize>) {
    log::debug!("Started killer ({sec}s)");
    std::thread::sleep(Duration::from_secs(sec as u64));
    signal.store(0, Ordering::Relaxed);
}

pub struct PanicGuard(Arc<AtomicIsize>);

impl Drop for PanicGuard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            log::error!("Thread panicked, aborting benchmark");
            self.0.store(1, Ordering::Relaxed);
        }
    }
}

pub fn run_workload(db: DatabaseWrapper, cmd: &RunArgs, finish_signal: Arc<AtomicIsize>) {
    let args = &cmd.args;

    log::info!("Starting workload {:#?}", cmd.workload);

    match &cmd.workload {
        Workload::ColumnStore(opts) => {
            use crate::workload::column_store::run;

            run(args, opts, &db, finish_signal);
        }

        &Workload::Idle => {
            start_killer(args.seconds, finish_signal);
            std::thread::sleep(std::time::Duration::from_hours(24));
        }

        Workload::Feed(opts) => {
            feed::run(args, opts, &db, finish_signal);
        }

        Workload::EventLog(opts) => {
            use crate::workload::event_log::run;

            run(args, opts, &db, finish_signal);
        }

        Workload::Webtable(opts) => {
            use crate::workload::webtable::run;

            run(args, opts, &db, finish_signal);
        }

        Workload::TimeSeries(opts) => {
            use crate::workload::timeseries::run;

            run(args, opts, &db, finish_signal);
        }

        Workload::Ycsb(ycsb_opts) => {
            use crate::workload::ycsb::YcsbType::{A, B, C};

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

        Workload::ReadWrite(opts) => {
            read_write::run(args, opts, &db, finish_signal);
        }
    }
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
    use crate::random::thread_rng;

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
