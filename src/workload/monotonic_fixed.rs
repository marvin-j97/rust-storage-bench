use super::start_killer;
use crate::args::RunOptions;
use crate::db::DatabaseWrapper;
use crate::workload::choose_zipf;
use rand::{Rng, RngCore};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

pub fn run(args: &RunOptions, db: &DatabaseWrapper, finish_signal: Arc<AtomicIsize>) {
    log::debug!("Ingesting data");
    let item_count = args.item_count as u64;

    let mut buf = vec![0; args.value_size as usize];
    let mut rng = rand::thread_rng();

    let iter = (0..item_count).map(|x| {
        rng.fill_bytes(&mut buf);
        ((x as u128).to_be_bytes().to_vec(), buf.to_vec())
    });

    db.ingest(iter);

    if args.warmup_cache {
        // TODO: this should probably be a separate function
        // as length might perform no warn-up at all
        log::debug!("Warming up cache");
        assert_eq!(db.len(), args.item_count);
    }

    std::thread::spawn({
        log::debug!("Starting reader");
        let db = db.clone();
        let random = args.read_random;
        let exponent = args.zipf_exponent;

        move || {
            let mut rng = rand::thread_rng();

            loop {
                let x = if random {
                    rng.gen_range(0..item_count)
                } else {
                    choose_zipf(&mut rng, exponent, item_count)
                };
                db.get(&(x as u128).to_be_bytes()).unwrap();
            }
        }
    });

    start_killer(args.seconds, finish_signal);
}
