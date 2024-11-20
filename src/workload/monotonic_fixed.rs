use super::start_killer;
use crate::args::RunOptions;
use crate::db::DatabaseWrapper;
use rand::prelude::Distribution;
use rand::{Rng, RngCore};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use zipf::ZipfDistribution;

pub fn run(args: &RunOptions, db: &DatabaseWrapper, finish_signal: Arc<AtomicBool>) {
    println!("Ingesting data");
    let item_count = args.item_count as u128;

    let mut buf = vec![0; args.value_size as usize];
    let mut rng = rand::thread_rng();

    let iter = (0..item_count).map(|x| {
        rng.fill_bytes(&mut buf);
        (x.to_be_bytes().to_vec(), buf.to_vec())
    });

    db.ingest(iter);

    std::thread::spawn({
        println!("Starting reader");
        let db = db.clone();
        let random = args.random;

        move || {
            let mut rng = rand::thread_rng();
            let dist = ZipfDistribution::new((item_count as usize) - 1, 1.0).unwrap();

            loop {
                let x = if random {
                    rng.gen_range(0..item_count)
                } else {
                    dist.sample(&mut rng) as u128
                };
                db.get(&x.to_be_bytes()).unwrap();
            }
        }
    });

    start_killer(args.seconds, finish_signal);
}
