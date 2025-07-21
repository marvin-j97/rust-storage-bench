use super::super::start_killer;
use crate::args::CommonRunOptions;
use crate::db::DatabaseWrapper;
use crate::workload::ycsb::Options;
use crate::workload::{choose_zipf, PanicGuard};
use rand::{Rng, RngCore};
use std::sync::atomic::AtomicIsize;
use std::sync::Arc;

pub fn run(
    common_args: &CommonRunOptions,
    ycsb_opts: &Options,
    db: &DatabaseWrapper,
    finish_signal: Arc<AtomicIsize>,
) {
    let item_count = ycsb_opts.item_count as u64;
    assert!(item_count > 0);

    {
        log::debug!("Pre-writing {item_count} items");
        let mut rng = rand::thread_rng();
        let mut buf = vec![0; ycsb_opts.value_size as usize];

        let iter = (0..(item_count as u128)).map(|x| {
            rng.fill_bytes(&mut buf);
            (x.to_be_bytes().to_vec(), {
                ycsb_opts.corpus.fetch(&mut rng, &mut buf);
                buf.to_vec()
            })
        });

        db.ingest(iter);
    }

    let worker = std::thread::Builder::new()
        .name("reader".to_owned())
        .spawn({
            log::debug!("Starting reader");

            let stop_signal = finish_signal.clone();
            let db = db.clone();

            let random = ycsb_opts.read_random;
            let exponent = ycsb_opts.zipf_exponent;

            move || {
                let _guard = PanicGuard(stop_signal);

                let mut rng = rand::thread_rng();

                loop {
                    let x: u128 = if random {
                        rng.gen_range(0..item_count as u128)
                    } else {
                        choose_zipf(&mut rng, exponent, item_count) as u128
                    };

                    db.get(&x.to_be_bytes()).unwrap();
                }
            }
        })
        .unwrap();

    start_killer(common_args.seconds, finish_signal);

    worker.join().unwrap();
}
