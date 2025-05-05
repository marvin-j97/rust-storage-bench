use super::start_killer;
use crate::{
    args::{CommonRunOptions, QueueOptions},
    db::DatabaseWrapper,
};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

pub fn run(
    common_args: &CommonRunOptions,
    queue_opts: &QueueOptions,
    db: &DatabaseWrapper,
    finish_signal: Arc<AtomicBool>,
) {
    let fsync = common_args.fsync;

    let with_backpressure = queue_opts.backpressure;
    let max_pending = queue_opts.max_pending;

    let mutex = Arc::new(std::sync::Mutex::new(()));
    let condvar = Arc::new(std::sync::Condvar::new());
    let pending_writes = Arc::new(AtomicU64::new(0));

    std::thread::spawn({
        log::debug!("Starting writer");

        let corpus = queue_opts.corpus;
        let mut buf = vec![0; queue_opts.value_size as usize];

        let db = db.clone();
        let condvar = condvar.clone();
        let pending_writes = pending_writes.clone();
        let mutex = mutex.clone();

        move || {
            let mut rng = rand::thread_rng();

            // Note how we're starting at 1 instead of 0
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
                if pending_writes.fetch_add(1, Ordering::Relaxed) >= max_pending
                    && with_backpressure
                {
                    // Wait for the consumer to consume one
                    let _guard = condvar.wait(mutex.lock().unwrap()).unwrap();
                } else {
                    // Notify the consumer that we wrote one
                    condvar.notify_one();
                }
            }
        }
    });

    let value_size = queue_opts.value_size;
    let decrement_workload_size = Some((16 + value_size) as u64);

    std::thread::spawn({
        log::debug!("Starting reader");
        let db = db.clone();

        move || {
            let mut last_key = 0u128;

            loop {
                let last_key_bytes = last_key.to_be_bytes();
                let start_exclusive = std::ops::Bound::Excluded(&last_key_bytes[..]);

                if let Some((key, _)) =
                    db.range_first((start_exclusive, std::ops::Bound::Unbounded))
                {
                    last_key = u128::from_be_bytes(key[..].try_into().unwrap());
                    db.remove_unique(&key, fsync, decrement_workload_size);

                    if pending_writes.fetch_sub(1, Ordering::Relaxed) >= max_pending
                        && with_backpressure
                    {
                        // Notify the writer that we consumed one
                        condvar.notify_one();
                    }
                } else {
                    // Wait for the writer to write one
                    let _guard = condvar.wait(mutex.lock().unwrap()).unwrap();
                }
            }
        }
    });

    start_killer(common_args.seconds, finish_signal);
}
