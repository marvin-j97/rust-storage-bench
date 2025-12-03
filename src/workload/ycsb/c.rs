use super::super::start_killer;
use crate::args::CommonRunOptions;
use crate::db::DatabaseWrapper;
use crate::workload::ycsb::Options;
use crate::workload::{choose_zipf, hash_key, PanicGuard};
use base64::Engine;
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
        let mut rng = crate::random::thread_rng();
        let mut buf = vec![0; ycsb_opts.value_size as usize];

        let iter = (0..(item_count as u128)).map(|x| {
            let k = {
                let key = x.to_be_bytes();
                base64::engine::general_purpose::URL_SAFE
                    .encode(key)
                    .into_bytes()
            };
            let v = {
                rng.fill_bytes(&mut buf);
                ycsb_opts.corpus.fetch(&mut rng, &mut buf);
                buf.to_vec()
            };
            (k, v)
        });

        db.ingest_unordered(iter);
    }

    for thread_no in 0..ycsb_opts.threads {
        std::thread::Builder::new()
            .name(format!("reader {thread_no}"))
            .spawn({
                log::debug!("Starting reader {thread_no}");

                let stop_signal = finish_signal.clone();
                let db = db.clone();

                let read_random = ycsb_opts.read_random;
                let exponent = ycsb_opts.zipf_exponent;

                move || {
                    let _guard = PanicGuard(stop_signal);

                    let mut rng = crate::random::thread_rng();

                    loop {
                        let x: u128 = if read_random {
                            rng.gen_range(0..item_count as u128)
                        } else {
                            choose_zipf(&mut rng, exponent, item_count) as u128
                        };

                        let key = x.to_be_bytes();
                        let encoded_string = base64::engine::general_purpose::URL_SAFE.encode(key);

                        db.get(encoded_string.as_bytes()).unwrap();
                    }
                }
            })
            .unwrap();
    }

    start_killer(common_args.seconds, finish_signal);
}
