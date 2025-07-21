use super::start_killer;
use crate::corpus::Corpus;
use crate::workload::PanicGuard;
use crate::{args::CommonRunOptions, db::DatabaseWrapper};
use clap::Parser;
use serde::Serialize;
use std::sync::{atomic::AtomicBool, Arc};

#[derive(Parser, Clone, Debug, Serialize)]
pub struct Options {
    /// Enables queue backpressure
    #[arg(long, default_value_t = false)]
    pub backpressure: bool,

    /// Backpressure max queue size
    #[arg(long, default_value_t = 1_000)]
    pub max_pending: u64,

    /// Corpus type
    #[arg(long, value_enum, default_value_t = Corpus::Random)]
    pub corpus: Corpus,

    /// Value size in bytes
    #[arg(long, default_value_t = 128)]
    pub value_size: u32,
}

pub fn run(
    common_args: &CommonRunOptions,
    queue_opts: &Options,
    db: &DatabaseWrapper,
    finish_signal: Arc<AtomicBool>,
) {
    let fsync = common_args.fsync;

    let with_backpressure = queue_opts.backpressure;
    let max_pending = queue_opts.max_pending;

    let pending = Arc::new(std::sync::Mutex::new(0u64));
    let condvar = Arc::new(std::sync::Condvar::new());

    std::thread::Builder::new()
        .name(String::from("writer"))
        .spawn({
            log::debug!("Starting writer");

            let stop_signal = finish_signal.clone();
            let db = db.clone();
            let condvar = condvar.clone();
            let pending = pending.clone();

            let corpus = queue_opts.corpus;
            let mut buf = vec![0; queue_opts.value_size as usize];

            move || {
                let _guard = PanicGuard(stop_signal);

                let mut rng = rand::thread_rng();

                // NOTE: Note how we're starting at 1 instead of 0
                for seqno in 1u128.. {
                    db.insert(
                        &seqno.to_be_bytes(),
                        {
                            corpus.fetch(&mut rng, &mut buf);
                            &buf
                        },
                        fsync,
                        true,
                    );

                    let mut guard = pending.lock().unwrap();
                    if *guard == 0 {
                        // Notify the reader that we wrote one
                        condvar.notify_one();
                    }

                    *guard += 1;

                    if with_backpressure {
                        // Wait for pending to drop < max_pending
                        guard = condvar.wait_while(guard, |g| *g >= max_pending).unwrap();
                    }
                }
            }
        })
        .unwrap();

    let value_size = queue_opts.value_size;
    let decrement_workload_size = Some((std::mem::size_of::<u128>() + value_size as usize) as u64);

    std::thread::Builder::new()
        .name(String::from("reader"))
        .spawn({
            log::debug!("Starting reader");

            let stop_signal = finish_signal.clone();
            let db = db.clone();

            move || {
                let _guard = PanicGuard(stop_signal);

                let mut last_key = 0u128;

                loop {
                    let last_key_bytes = last_key.to_be_bytes();
                    let start_exclusive = std::ops::Bound::Excluded(&last_key_bytes[..]);

                    let mut consumed = false;
                    if let Some((key, _)) =
                        db.range_first((start_exclusive, std::ops::Bound::Unbounded))
                    {
                        last_key = u128::from_be_bytes(key[..].try_into().unwrap());

                        db.remove_unique(&key, fsync, decrement_workload_size);
                        consumed = true;
                    }

                    let mut guard = pending.lock().unwrap();

                    if consumed {
                        if with_backpressure && *guard >= max_pending {
                            // Notify the waiting producer
                            condvar.notify_one();
                        }
                        *guard -= 1;
                    }

                    // Wait for the one message to be pending
                    guard = condvar.wait_while(guard, |g| *g == 0).unwrap();
                }
            }
        })
        .unwrap();

    start_killer(common_args.seconds, finish_signal);
}
