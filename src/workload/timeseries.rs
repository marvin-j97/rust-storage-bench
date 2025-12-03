use super::start_killer;
use crate::corpus::Corpus;
use crate::workload::{choose_zipf, PanicGuard};
use crate::{args::CommonRunOptions, db::DatabaseWrapper};
use clap::Parser;
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

    #[arg(long, default_value_t = 8)]
    pub readers: usize,

    #[arg(long, default_value_t = 100_000)]
    pub series_count: usize,

    #[arg(long, default_value_t = 500_000_000)]
    pub item_count: usize,
}

pub fn run(
    common_args: &CommonRunOptions,
    opts: &Options,
    db: &DatabaseWrapper,
    finish_signal: Arc<AtomicIsize>,
) {
    let series_ids = Arc::new({
        let mut v = (0..opts.series_count)
            .map(|_| uuid::Uuid::new_v4())
            .collect::<Vec<_>>();
        v.sort();
        v
    });

    {
        let iter = series_ids.iter().flat_map({
            move |id| {
                let corpus = opts.corpus;
                let mut rng = rand::thread_rng();
                let mut buf = vec![0; opts.value_size as usize];

                (0..(opts.item_count / opts.series_count)).map({
                    move |_| {
                        let datapoint_id = scru128::new().to_u128();

                        // NOTE: Add to topic index
                        let mut index_item_key = [0; 32];
                        index_item_key[0..16].copy_from_slice(id.as_bytes());

                        // We need to write in ascending order
                        index_item_key[16..].copy_from_slice(&datapoint_id.to_be_bytes());

                        corpus.fetch(&mut rng, &mut buf);
                        (index_item_key.to_vec(), buf.clone())
                    }
                })
            }
        });

        db.ingest(iter);
    }

    std::thread::Builder::new()
        .name(String::from("writer"))
        .spawn({
            log::debug!("Starting writer");

            let stop_signal = finish_signal.clone();
            let db = db.clone();
            let series_ids = series_ids.clone();

            let corpus = opts.corpus;
            let mut buf = vec![0; opts.value_size as usize];

            move || {
                let _guard = PanicGuard(stop_signal);

                let mut rng = rand::thread_rng();

                loop {
                    let datapoint_id = scru128::new().to_u128();

                    let series_to_index = rng.gen_range(2..8);

                    for _ in 0..series_to_index {
                        let series_idx = choose_zipf(&mut rng, 1.0, series_ids.len() as u64);
                        let series_id = series_ids[series_idx as usize];

                        // NOTE: Add to topic index
                        let mut index_item_key = [0; 32];
                        index_item_key[0..16].copy_from_slice(series_id.as_bytes());
                        index_item_key[16..].copy_from_slice(&(!datapoint_id).to_be_bytes());

                        corpus.fetch(&mut rng, &mut buf);
                        db.insert(&index_item_key, &buf, false, true);
                    }
                }
            }
        })
        .unwrap();

    (0..opts.readers).for_each(|_| {
        std::thread::Builder::new()
            .name(String::from("reader"))
            .spawn({
                log::debug!("Starting reader");

                let stop_signal = finish_signal.clone();
                let db = db.clone();
                let series_ids = series_ids.clone();

                move || {
                    let _guard = PanicGuard(stop_signal);

                    let mut rng = rand::thread_rng();

                    loop {
                        let series_idx = choose_zipf(&mut rng, 1.0, series_ids.len() as u64);
                        let series_id = series_ids[series_idx as usize];
                        assert!(db.prefix_len(&series_id.into_bytes(), false, 1_000) <= 1_000);
                    }
                }
            })
            .unwrap();
    });

    start_killer(common_args.seconds, finish_signal);
}
