use crate::{args::RunOptions, db::DatabaseWrapper};
use clap::ValueEnum;
use serde::Serialize;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

fn start_killer(min: u16, signal: Arc<AtomicBool>) {
    println!("Started killer");
    std::thread::sleep(Duration::from_secs(min as u64 * 60));
    signal.store(true, Ordering::Relaxed);
}

// TODO: add more workloads
#[derive(Copy, Debug, Clone, ValueEnum, Serialize, PartialEq, Eq)]
#[clap(rename_all = "kebab_case")]
pub enum Workload {
    TimeseriesWrite,
}

pub fn run_workload(db: DatabaseWrapper, args: &RunOptions, finish_signal: Arc<AtomicBool>) {
    println!("Starting workload {:?}", args.workload);

    match args.workload {
        Workload::TimeseriesWrite => {
            std::thread::spawn({
                println!("Starting writer");
                let db = db.clone();

                move || {
                    for x in 0u128.. {
                        let key = x.to_be_bytes();
                        db.insert(&key, &key, false);
                    }
                }
            });

            start_killer(args.minutes, finish_signal);
        }
        _ => {
            unimplemented!()
        }
    };
}
