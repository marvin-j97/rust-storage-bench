use crate::args::CommonRunOptions;
use crate::corpus::Corpus;
use crate::db::DatabaseWrapper;
use crate::workload::{choose_zipf, start_killer, PanicGuard};
use clap::Parser;
use rand::prelude::Distribution;
use serde::Serialize;
use std::sync::atomic::AtomicIsize;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

#[derive(Parser, Clone, Debug, Serialize)]
pub struct Options {
    #[arg(long, default_value_t = 100)]
    pub database_count: usize,

    #[arg(long, default_value_t = 5)]
    pub column_count: usize,

    #[arg(long, default_value_t = 1_000_000)]
    pub initial_rows: usize,

    /// Corpus type
    #[arg(long, value_enum, default_value_t = Corpus::ProtoBuf)]
    pub corpus: Corpus,

    /// Value size in bytes
    #[arg(long, default_value_t = 100)]
    pub value_size: u16,

    #[arg(long, default_value_t = 8)]
    pub readers: usize,
}

fn format_key(database_id: uuid::Uuid, col_id: uuid::Uuid, row_id: uuid::Uuid) -> fjall_3::UserKey {
    use std::io::Write;

    let mut builder = unsafe { fjall_3::UserKey::builder_unzeroed(48) };
    let mut writer = std::io::Cursor::new(&mut builder[..]);
    writer.write_all(&database_id.into_bytes()).unwrap();
    writer.write_all(&col_id.into_bytes()).unwrap();
    writer.write_all(&row_id.into_bytes()).unwrap();
    builder.freeze().into()
}

pub fn run(
    common_args: &CommonRunOptions,
    opts: &Options,
    db: &DatabaseWrapper,
    finish_signal: Arc<AtomicIsize>,
) {
    let mut rng = rand::thread_rng();
    let mut buf = vec![0; opts.value_size as usize];

    let db_ids = {
        let mut v = (0..opts.database_count)
            .map(|_| Uuid::new_v4())
            .collect::<Vec<_>>();

        v.sort();
        v
    };

    let col_ids = {
        let mut v = (0..opts.column_count)
            .map(|_| Uuid::new_v4())
            .collect::<Vec<_>>();

        v.sort();
        v
    };

    db.ingest(
        db_ids
            .iter()
            .flat_map(|db_id| col_ids.iter().map(move |cid| (db_id, cid)))
            .flat_map(|(db_id, col_id)| {
                (0..opts.initial_rows)
                    .map(move |row_id| (db_id, col_id, Uuid::from_u128(row_id as u128)))
            })
            .map(|(db_id, col_id, row_id)| {
                let key = format_key(*db_id, *col_id, row_id);
                opts.corpus.fetch(&mut rng, &mut buf);
                (key.to_vec(), buf.to_vec())
            }),
    );

    std::thread::Builder::new()
        .name(String::from("writer"))
        .spawn({
            log::debug!("Starting writer thread");

            let corpus = opts.corpus;
            let item_count = opts.initial_rows;
            let stop_signal = finish_signal.clone();
            let db = db.clone();
            let db_ids = db_ids.clone();
            let col_ids = col_ids.clone();

            move || {
                let _guard = PanicGuard(stop_signal);

                let mut rng = rand::thread_rng();

                loop {
                    for _ in 0..8_000 {
                        let db_idx = choose_zipf(&mut rng, 1.0, db_ids.len() as u64);
                        let db_id = db_ids[db_idx as usize];

                        let col_idx = choose_zipf(&mut rng, 1.0, col_ids.len() as u64);
                        let col_id = col_ids[col_idx as usize];

                        let row_id = zipf::ZipfDistribution::new(item_count, 1.0)
                            .unwrap()
                            .sample(&mut rng) as u128;

                        let key = format_key(db_id, col_id, Uuid::from_u128(row_id));

                        corpus.fetch(&mut rng, &mut buf);
                        db.insert(&key, &buf, false, false);
                    }

                    std::thread::sleep(Duration::from_secs(1));
                }
            }
        })
        .unwrap();

    (0..opts.readers).for_each(|_| {
        std::thread::Builder::new()
            .name(String::from("reader"))
            .spawn({
                log::debug!("Starting reader thread");

                let stop_signal = finish_signal.clone();
                let db = db.clone();
                let db_ids = db_ids.clone();

                move || {
                    let _guard = PanicGuard(stop_signal);

                    let mut rng = rand::thread_rng();

                    loop {
                        let db_idx = choose_zipf(&mut rng, 1.0, db_ids.len() as u64);
                        let db_id = db_ids[db_idx as usize];
                        assert!(db.prefix_len(&db_id.into_bytes(), false, 100) <= 100);
                    }
                }
            })
            .unwrap();
    });

    start_killer(common_args.seconds, finish_signal);
}
