use super::start_killer;
use crate::args::RunOptions;
use crate::db::DatabaseWrapper;
use rand::{Rng, RngCore};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use zipf::ZipfDistribution;

pub fn run(args: &RunOptions, db: &DatabaseWrapper, finish_signal: Arc<AtomicBool>) {
    let fsync = args.fsync;
    let item_count = args.item_count as u64;

    let written_count = Arc::new(AtomicU64::new(item_count));
    let mut buf = vec![0; args.value_size as usize];

    if item_count > 0 {
        log::debug!("Pre-writing {item_count} items");
        let mut rng = rand::thread_rng();

        let iter = (0..(item_count as u128)).map(|x| {
            rng.fill_bytes(&mut buf);
            (x.to_be_bytes().to_vec(), buf.to_vec())
        });

        db.ingest(iter);
    }

    std::thread::spawn({
        log::debug!("Starting writer");
        let db = db.clone();
        let written_count = written_count.clone();

        move || {
            let mut rng = rand::thread_rng();

            for x in (item_count as u128).. {
                let key = x.to_be_bytes();
                rng.fill_bytes(&mut buf);
                db.insert(&key, &buf, fsync);

                written_count.fetch_add(1, Ordering::Relaxed);
            }
        }
    });

    std::thread::spawn({
        log::debug!("Starting reader");
        let db = db.clone();
        let written_count = written_count.clone();
        let random = args.random;

        move || {
            let mut rng = rand::thread_rng();

            loop {
                let item_count = written_count.load(Ordering::Relaxed) as u128;

                if item_count > 1 {
                    use rand::prelude::Distribution;

                    let x: u128 = if random {
                        rng.gen_range(0..item_count)
                    } else {
                        let zipf = ZipfDistribution::new((item_count as usize) - 1, 1.0).unwrap();

                        zipf.sample(&mut rng) as u128
                    };

                    db.get(&x.to_be_bytes()).unwrap();
                }
            }
        }
    });

    start_killer(args.seconds, finish_signal);
}
