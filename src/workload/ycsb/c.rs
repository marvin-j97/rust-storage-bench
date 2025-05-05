use super::super::start_killer;
use crate::args::RunOptions;
use crate::db::DatabaseWrapper;
use rand::{Rng, RngCore};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use zipf::ZipfDistribution;

pub fn run(args: &RunOptions, db: &DatabaseWrapper, finish_signal: Arc<AtomicBool>) {
    let item_count = args.item_count as u64;

    assert!(item_count > 0);

    {
        log::debug!("Pre-writing {item_count} items");
        let mut rng = rand::thread_rng();
        let mut buf = vec![0; args.value_size as usize];

        let iter = (0..(item_count as u128)).map(|x| {
            rng.fill_bytes(&mut buf);
            (x.to_be_bytes().to_vec(), buf.to_vec())
        });

        db.ingest(iter);
    }

    let worker = std::thread::Builder::new()
        .name("workload".to_owned())
        .spawn({
            log::debug!("Starting reader");
            let db = db.clone();
            let random = args.read_random;
            let exponent = args.zipf_exponent;

            move || {
                use rand::prelude::Distribution;

                let mut rng = rand::thread_rng();
                let zipf = ZipfDistribution::new(item_count as usize, exponent).unwrap();

                loop {
                    let x: u128 = if random {
                        rng.gen_range(0..item_count as u128)
                    } else {
                        (zipf.sample(&mut rng) - 1) as u128
                    };

                    db.get(&x.to_be_bytes()).unwrap();
                }
            }
        })
        .unwrap();

    start_killer(args.seconds, finish_signal);

    worker.join().unwrap();
}
