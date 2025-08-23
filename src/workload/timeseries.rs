use super::start_killer;
use crate::corpus::Corpus;
use crate::workload::PanicGuard;
use crate::{args::CommonRunOptions, db::DatabaseWrapper};
use clap::Parser;
use rand::seq::SliceRandom;
use rand::Rng;
use serde::Serialize;
use std::sync::atomic::AtomicIsize;
use std::sync::Arc;

#[derive(Parser, Clone, Debug, Serialize)]
pub struct Options {
    /// Corpus type
    #[arg(long, value_enum, default_value_t = Corpus::Random)]
    pub corpus: Corpus,

    /// Value size in bytes
    #[arg(long, default_value_t = 8)]
    pub value_size: u32,
}

pub fn run(
    common_args: &CommonRunOptions,
    opts: &Options,
    db: &DatabaseWrapper,
    finish_signal: Arc<AtomicIsize>,
) {
    let series_ids = Arc::new(
        (0..100_000)
            .map(|_| uuid::Uuid::new_v4())
            .collect::<Vec<_>>(),
    );

    std::thread::Builder::new()
        .name(String::from("writer"))
        .spawn({
            log::debug!("Starting writer");

            let stop_signal = finish_signal.clone();
            let db = db.clone();

            let corpus = opts.corpus;
            let mut buf = vec![0; opts.value_size as usize];

            move || {
                let _guard = PanicGuard(stop_signal);

                let mut rng = rand::thread_rng();

                loop {
                    let datapoint_id = scru128::new().to_u128();

                    let series_to_index = rng.gen_range(2..8);

                    for _ in 0..series_to_index {
                        let series_id = series_ids.choose(&mut rng).unwrap();

                        // NOTE: Add to topic index
                        let mut index_item_key = [0; 32];
                        index_item_key[0..16].copy_from_slice(series_id.as_bytes());
                        index_item_key[16..].copy_from_slice(&datapoint_id.to_be_bytes());

                        corpus.fetch(&mut rng, &mut buf);
                        db.insert(&index_item_key, &buf, false, true);
                    }

                    // // NOTE: Limit to 100k inserts per second
                    // std::thread::sleep(Duration::from_micros(10));
                }
            }
        })
        .unwrap();

    // TODO: read most recent 1000 items from random series

    start_killer(common_args.seconds, finish_signal);
}
