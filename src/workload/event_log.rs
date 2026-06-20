use super::start_killer;
use crate::corpus::Corpus;
use crate::workload::PanicGuard;
use crate::{args::CommonRunOptions, db::DatabaseWrapper};
use clap::Parser;
use rand::Rng;
use serde::Serialize;
use std::sync::atomic::AtomicIsize;
use std::sync::Arc;

const SCAN_CHANCE: f32 = 0.01;

#[derive(Parser, Clone, Debug, Serialize)]
pub struct Options {
    /// Corpus type
    #[arg(long, value_enum, default_value_t = Corpus::Json)]
    pub corpus: Corpus,

    /// Value size in bytes
    #[arg(long, default_value_t = 100)]
    pub value_size: u32,
}

pub fn run(
    common_args: &CommonRunOptions,
    opts: &Options,
    db: &DatabaseWrapper,
    finish_signal: Arc<AtomicIsize>,
) {
    std::thread::Builder::new()
        .name(String::from("worker"))
        .spawn({
            log::debug!("Starting worker");

            let stop_signal = finish_signal.clone();
            let db = db.clone();

            let corpus = opts.corpus;
            let mut buf = vec![0; opts.value_size as usize];

            move || {
                let _guard = PanicGuard(stop_signal);

                let mut rng = rand::thread_rng();

                loop {
                    match rng.gen_range(0.0..1.0) {
                        x if x < SCAN_CHANCE => {
                            db.prefix_len(b"", false, 256);
                        }
                        _ => {
                            let datapoint_id = scru128::new().to_u128();
                            let datapoint_id = !datapoint_id; // Invert to store in reverse order
                            let datapoint_id = datapoint_id.to_be_bytes();

                            corpus.fetch(&mut rng, &mut buf);
                            db.insert(&datapoint_id, &buf, false, true);
                        }
                    }
                }
            }
        })
        .unwrap();

    start_killer(common_args.seconds, finish_signal);
}
