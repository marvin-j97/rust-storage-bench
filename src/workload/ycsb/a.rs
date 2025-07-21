use super::super::start_killer;
use crate::args::CommonRunOptions;
use crate::db::DatabaseWrapper;
use crate::workload::ycsb::Options;
use crate::workload::{choose_zipf, PanicGuard};
use rand::Rng;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

const POINT_READ_CHANCE: f32 = 0.5;

pub fn run(
    common_args: &CommonRunOptions,
    ycsb_opts: &Options,
    db: &DatabaseWrapper,
    finish_signal: Arc<AtomicBool>,
) {
    let item_count = ycsb_opts.item_count as u64;
    assert!(item_count > 0);

    let mut buf = vec![0; ycsb_opts.value_size as usize];

    {
        log::debug!("Pre-writing {item_count} items");
        let mut rng = rand::thread_rng();

        let iter = (0..(item_count as u128)).map(|x| {
            (x.to_be_bytes().to_vec(), {
                ycsb_opts.corpus.fetch(&mut rng, &mut buf);
                buf.to_vec()
            })
        });

        db.ingest(iter);
    }

    let worker = std::thread::Builder::new()
        .name("worker".into())
        .spawn({
            log::debug!("Starting worker");

            let stop_signal = finish_signal.clone();
            let db = db.clone();

            let random = ycsb_opts.read_random;
            let exponent = ycsb_opts.zipf_exponent;
            let corpus = ycsb_opts.corpus;
            let fsync = common_args.fsync;

            move || {
                let _guard = PanicGuard(stop_signal);

                let mut rng = rand::thread_rng();

                loop {
                    match rng.gen_range(0.0..1.0) {
                        x if x <= POINT_READ_CHANCE => {
                            let x: u128 = if random {
                                rng.gen_range(0..item_count as u128)
                            } else {
                                choose_zipf(&mut rng, exponent, item_count) as u128
                            };

                            db.get(&x.to_be_bytes()).unwrap();
                        }
                        _ => {
                            let x: u128 = if random {
                                rng.gen_range(0..item_count as u128)
                            } else {
                                choose_zipf(&mut rng, exponent, item_count) as u128
                            };

                            corpus.fetch(&mut rng, &mut buf);
                            db.insert(&x.to_be_bytes(), &buf, fsync, false);
                        }
                    }
                }
            }
        })
        .unwrap();

    start_killer(common_args.seconds, finish_signal);

    worker.join().unwrap();
}
