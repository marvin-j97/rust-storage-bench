use super::super::start_killer;
use crate::args::RunOptions;
use crate::db::DatabaseWrapper;
use rand::{Rng, RngCore};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use zipf::ZipfDistribution;

pub fn run(args: &RunOptions, db: &DatabaseWrapper, finish_signal: Arc<AtomicBool>) {
    let fsync = args.fsync;
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

    let worker = std::thread::spawn({
        log::debug!("Starting reader");
        let db = db.clone();
        let random = args.random;
        let mut buf = vec![0; args.value_size as usize];

        move || {
            use rand::prelude::Distribution;

            let mut rng = rand::thread_rng();

            loop {
                match rng.gen_range(0.0..1.0) {
                    x if x >= 0.5 => {
                        let x: u128 = if random {
                            rng.gen_range(0..item_count as u128)
                        } else {
                            let zipf =
                                ZipfDistribution::new((item_count as usize) - 1, 1.0).unwrap();

                            zipf.sample(&mut rng) as u128
                        };

                        db.get(&x.to_be_bytes()).unwrap();
                    }
                    _ => {
                        let x: u128 = if random {
                            rng.gen_range(0..item_count as u128)
                        } else {
                            let zipf =
                                ZipfDistribution::new((item_count as usize) - 1, 1.0).unwrap();

                            zipf.sample(&mut rng) as u128
                        };

                        rng.fill_bytes(&mut buf);
                        db.insert(&x.to_be_bytes(), &buf, fsync);
                    }
                }
            }
        }
    });

    start_killer(args.seconds, finish_signal);

    worker.join().unwrap();
}
