use crate::{args::Args, db::DatabaseWrapper, unix_timestamp};
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

// #[derive(Copy, Debug, Clone, ValueEnum, Serialize, PartialEq, Eq)]
// #[clap(rename_all = "kebab_case")]
// pub enum Workload {
//     GarageBlockRef,

//     /// Workload A: (The company formerly known as Twitter)-style feed
//     ///
//     /// 1000 virtual users that post data to their feed
//     /// - each user's profile is stored as userID#p
//     /// - each post's key is: userID#f#cuid
//     ///
//     /// 90% a random virtual user's feed is queried by the last 10 items
//     ///
//     /// 10% a random virtual user will create a new post
//     Feed,

//     FeedWriteOnly,

//     // TODO: remove?
//     /// Workload B: Ingest 1 billion items as fast as possible
//     ///
//     /// Monotonic keys, no reads, no sync
//     Billion,

//     HtmlDump,

//     /// Webtable-esque storing of HTML documents per domain and deleting old versions
//     Webtable,

//     /// Monotonic writes and Zipfian point reads
//     Monotonic,

//     /// Mononic writes, then Zipfian point reads
//     MonotonicFixed,

//     MonotonicWriteOnly,

//     /// Mononic writes, then Zipfian point reads
//     MonotonicFixedRandom,

//     /// Timeseries data
//     ///
//     /// Monotonic keys, write-heavy, no sync, scan most recent data
//     Timeseries,
//     /*  /// Workload A: Update heavy workload
//     ///
//     /// Application example: Session store recording recent actions
//     TaskA,

//     /// Workload B: Read mostly workload
//     ///
//     /// Application example: photo tagging; add a tag is an update, but most operations are to read tags
//     TaskB,

//     /// Workload C: Read only
//     ///
//     /// Application example: user profile cache, where profiles are constructed elsewhere (e.g., Hadoop)
//     TaskC,

//     /// Workload D: Read latest workload with light inserts
//     ///
//     /// Application example: user status updates; people want to read the latest
//     TaskD,

//     /// Workload E: Read latest workload with heavy inserts
//     ///
//     /// Application example: Event logging, getting the latest events
//     TaskE,

//     /// Workload F: Read zipfian workload with light inserts
//     TaskF,

//     /// Workload G: Read zipfian workload with heavy inserts
//     TaskG,

//     TaskI, */
// }

// #[derive(Clone, Debug, Eq, PartialEq, clap::ValueEnum)]
// pub enum LsmCompaction {
//     Leveled,
//     Tiered,
// }

// impl std::fmt::Display for LsmCompaction {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(
//             f,
//             "{}",
//             match self {
//                 Self::Leveled => "LCS",
//                 Self::Tiered => "STCS",
//             }
//         )
//     }
// }

// #[derive(Copy, Clone, ValueEnum, Debug, PartialEq, Eq)]
// pub enum Compression {
//     None,
//     Lz4,
//     Miniz,
// }

#[derive(Copy, Debug, Clone, ValueEnum, Serialize, PartialEq, Eq)]
#[clap(rename_all = "kebab_case")]
pub enum Workload {
    TimeseriesWrite,
}

pub fn run_workload(db: DatabaseWrapper, args: &Args, finish_signal: Arc<AtomicBool>) {
    println!("Starting workload {:?}", args.workload);

    match args.workload {
        Workload::TimeseriesWrite => {
            {
                let db = db.clone();

                println!("Starting writer");
                std::thread::spawn(move || loop {
                    db.insert(&unix_timestamp().as_nanos().to_be_bytes(), b"asdasd", false);
                });
            }

            println!("Starting reader");
            std::thread::spawn(move || loop {
                db.get(&0u64.to_be_bytes());
            });

            start_killer(args.minutes, finish_signal);
        }
        _ => {
            unimplemented!()
        }
    };
}
