use super::{hash_key, start_killer};
use crate::args::CommonRunOptions;
use crate::workload::{choose_zipf, PanicGuard};
use crate::{corpus::Corpus, db::DatabaseWrapper};
use clap::Parser;
use rand::Rng;
use serde::Serialize;
use std::sync::atomic::AtomicIsize;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

// TODO: restore ReadWrite dependent (read-write mix per thread instead of independent workers)

#[derive(Parser, Clone, Debug, Serialize)]
pub struct Options {
    #[arg(long, default_value_t = 1_000_000)]
    pub item_count: usize,

    #[arg(long, default_value_t = false)]
    pub write_only: bool,

    #[arg(long, default_value_t = true)]
    #[arg(long = "no-write-random", action = clap::ArgAction::SetFalse)]
    pub write_random: bool,

    #[arg(long, default_value_t = false)]
    pub read_random: bool,

    #[arg(long, value_enum, default_value_t = Corpus::Random)]
    pub corpus: Corpus,

    #[arg(long, default_value_t = 200)]
    pub value_size: u32,

    #[arg(long, default_value_t = 1.0)]
    pub zipf_exponent: f64,

    #[arg(long, default_value_t = 1)]
    pub threads: usize,
}

pub(crate) fn run(
    args: &CommonRunOptions,
    opts: &Options,
    db: &DatabaseWrapper,
    finish_signal: Arc<AtomicIsize>,
) {
    let random_key_distribution = opts.write_random;

    let key_mapper = move |k: u64| -> u64 {
        if random_key_distribution {
            hash_key(k)
        } else {
            k
        }
    };

    let item_count = opts.item_count as u64;

    if !opts.write_only {
        assert!(item_count > 0);
    }

    let value_size = opts.value_size as usize;

    {
        log::debug!("Writing initial data ({item_count} items)");

        let mut rng = crate::random::thread_rng();
        let mut buf = vec![0; value_size];

        db.ingest((0..item_count).map(|x| {
            let k = x.to_be_bytes();
            opts.corpus.fetch(&mut rng, &mut buf);
            (k.to_vec(), buf.to_vec())
        }));
    }

    let written_count = Arc::new(AtomicU64::new(item_count));

    let writer = std::thread::Builder::new()
        .name("writer".into())
        .spawn({
            log::debug!("Starting writer");

            let stop_signal = finish_signal.clone();
            let db = db.clone();
            let written_count = written_count.clone();
            let fsync = args.fsync;
            let corpus = opts.corpus;

            move || {
                let _guard = PanicGuard(stop_signal);

                let mut rng = crate::random::thread_rng();
                let mut buf = vec![0; value_size];

                for x in item_count.. {
                    let key = &key_mapper(x).to_be_bytes();
                    corpus.fetch(&mut rng, &mut buf);

                    db.insert(key, &buf, fsync, true);
                    written_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        })
        .unwrap();

    if !opts.write_only {
        std::thread::Builder::new()
            .name("reader".into())
            .spawn({
                log::debug!("Starting reader");

                let stop_signal = finish_signal.clone();
                let db = db.clone();
                let read_random = opts.read_random;
                let exponent = opts.zipf_exponent;

                move || {
                    let _guard = PanicGuard(stop_signal);

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
            })
            .unwrap();
    }

    start_killer(args.seconds, finish_signal);

    writer.join().unwrap();
}
