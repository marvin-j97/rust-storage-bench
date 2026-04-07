mod args;
mod corpus;
mod db;
mod monitor;
mod random;
mod report;
mod workload;

use crate::report::generate_report;
use args::Args;
use clap::Parser;
use db::{Backend, DatabaseBuilder};
use monitor::start_monitor;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::sync::atomic::AtomicIsize;
use std::sync::Arc;
use workload::run_workload;

#[cfg(not(target_pointer_width = "64"))]
compile_error!("This crate can only be used on 64-bit systems.");

#[cfg(feature = "jemalloc")]
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(feature = "tcmalloc")]
#[global_allocator]
static GLOBAL: tcmalloc::TCMalloc = tcmalloc::TCMalloc;

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc::SnMalloc = snmalloc::SnMalloc;

/// Gets the unix timestamp as a duration
pub fn unix_timestamp() -> std::time::Duration {
    let now = std::time::SystemTime::now();

    // NOTE: Unwrap is trivial
    #[allow(clippy::unwrap_used)]
    now.duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
}

const COLUMN_HEADERS: &[&str] = &[
    "time_ms",
    "cpu",
    "mem_kib",
    "disk_space_kib",
    "disk_writes_kib",
    "disk_reads_kib",
    //
    "data_block_io",
    "index_block_io",
    "filter_block_io",
    //
    "disk_table_count", // TODO: replace with level_sizes: [L0, L1, L2, L3, L4, L5, L6]
    "blob_file_count",
    "journal_count",
    "journal_size",
    "filter_size",
    "pinned_filter_size",
    "pinned_block_index_size",
    "cache_size",
    "write_buffer_size",
    "tree_height",
    "fragmented_bytes",
    "stale_blob_bytes",
    "running_compactions",
    "time_compacting_us",
    "tombstone_count",
    "l0_runs",
    "l0_table_avg_lifetime_ms",
    //
    "filter_true_negative_ratio",
    "block_cache_hit_rate",
    "data_block_cache_hit_rate",
    "index_block_cache_hit_rate",
    "filter_block_cache_hit_rate",
    "table_file_cache_hit_rate",
    //
    "write_ops",
    "point_read_ops",
    "range_ops",
    "delete_ops",
    //
    "write_latency",
    "point_read_latency",
    "range_latency",
    "delete_latency",
    //
    "write_rate",
    "point_read_rate",
    "range_rate",
    "delete_rate",
    //
    "write_potential",
    "point_read_potential",
    "range_potential",
    "delete_potential",
    //
    "write_amp",
    "space_amp",
    "read_amp",
    //
];

pub fn main() -> std::io::Result<()> {
    env_logger::Builder::from_default_env()
        .filter_module("rust_storage_bench", log::LevelFilter::Debug)
        // .filter_module("lsm_tree", log::LevelFilter::Debug)
        .init();

    #[cfg(feature = "antithesis")]
    {
        use precept::dispatch::{antithesis::AntithesisDispatch, noop::NoopDispatch};
        let dispatcher =
            AntithesisDispatch::try_load_boxed().unwrap_or_else(|| NoopDispatch::new_boxed());
        precept::init_boxed(dispatcher).expect("failed to setup precept");
    }

    log::info!("rust-storage-bench {}", env!("CARGO_PKG_VERSION"));
    {
        use chrono::{DateTime, Utc};
        use std::time::SystemTime;

        let now = SystemTime::now();
        let now: DateTime<Utc> = now.into();
        log::debug!("Datetime: {now}");
    }

    match Args::parse().command {
        args::Commands::Aggregate(args) => {
            let out = std::fs::File::create(args.out)?;
            let mut out = BufWriter::new(out);

            let files = args.files;

            // Create an iterator over all input files
            let mut iters = files
                .into_iter()
                .map(|path| {
                    let file = std::fs::File::open(path)?;
                    let file = BufReader::new(file);
                    let mut line_reader = file.lines();
                    line_reader.next().unwrap()?; // Skip system info

                    // Get args
                    let args = line_reader.next().unwrap()?;
                    let args: serde_json::Value = serde_json::from_str(&args).unwrap();
                    let id = args["id"].as_str().unwrap().to_owned();

                    let headers = line_reader.next().unwrap()?;
                    let headers: Vec<String> = serde_json::from_str(&headers).unwrap();

                    let iter = line_reader
                        .map(|line| line.unwrap())
                        .take_while(|line| !line.contains(r#""fin""#))
                        .map(|line| {
                            let json: serde_json::Value = serde_json::from_str(&line).unwrap();
                            json
                        });

                    Ok::<_, std::io::Error>((id, headers, iter))
                })
                .collect::<std::io::Result<Vec<_>>>()?;

            // Merge into JSON object and emit into out
            'outer: loop {
                let mut obj = serde_json::Value::Object(Default::default());

                for (id, headers, row_iter) in &mut iters {
                    let Some(next_row) = row_iter.next() else {
                        break 'outer;
                    };

                    obj["time"] = serde_json::Value::Number(serde_json::Number::from(
                        next_row[0].as_u64().unwrap(),
                    ));

                    let mut projection = serde_json::Value::Object(Default::default());

                    for column in &args.project {
                        let idx = headers
                            .iter()
                            .enumerate()
                            .find(|(_, x)| *x == column)
                            .map(|(idx, _)| idx)
                            .unwrap_or_else(|| panic!("should have column {column:?}"));

                        projection[column.as_str()] = next_row[idx].clone();
                    }

                    obj[id.as_str()] = projection;
                }

                out.write_all(serde_json::to_string(&obj).unwrap().as_bytes())?;
                out.write_all(b"\n")?;
            }

            out.flush()?;
            out.get_mut().sync_all()?;
        }
        args::Commands::Report(args) => {
            generate_report(args)?;
        }
        args::Commands::Run(mut cmd) => {
            let args = &mut cmd.args;

            if args.fsync && args.backend == Backend::Sled {
                panic!("Sled does not support proper synchronous writes: https://github.com/spacejam/sled/issues/1351");
            }

            if args.display_name.is_none() {
                args.display_name = Some(args.backend.to_string());
            }

            let out_path = std::path::absolute(
                args.out
                    .as_ref()
                    .cloned()
                    .unwrap_or_else(|| format!("{}.jsonl", scru128::new_string()).into()),
            )
            .unwrap();

            log::info!("Outputting to {out_path:?}");

            if out_path.try_exists()? {
                log::warn!("{out_path:?} already exists");
                std::process::exit(0);
            }

            let data_dir = args.data_dir.clone();

            if data_dir.try_exists()? && args.clean_data_dir {
                log::info!("Cleaning data dir {data_dir:?}");
                std::fs::remove_dir_all(&data_dir).unwrap();
            }

            {
                let out_parent = out_path.parent().unwrap();
                std::fs::create_dir_all(out_parent)?;
            }

            // The disk format of a log file is like this:
            // { system info object }
            // { args object }
            // [table header 1, table header 2, table header 3]
            // [data point, data point, data point]
            // [data point, data point, data point]
            // { fin: true }
            let mut file_writer = std::fs::File::create(&out_path).unwrap();

            let mut sys = sysinfo::System::new_all();
            sys.refresh_all();

            let start_time = unix_timestamp();

            // Write the system info object
            {
                let datetime = {
                    use chrono::{DateTime, Utc};
                    use std::time::SystemTime;

                    let now = SystemTime::now();
                    let now: DateTime<Utc> = now.into();
                    now.to_string()
                };

                let allocator = match (cfg!(feature = "jemalloc"), cfg!(feature = "mimalloc")) {
                    (true, false) => "jemalloc",
                    (false, true) => "mimalloc",
                    (false, false) => "system",
                    _ => unreachable!(),
                };

                let json = serde_json::json!({
                    "version": env!("CARGO_PKG_VERSION"),
                    "allocator": allocator,
                    "time_ms": start_time.as_millis(),
                    "os": sysinfo::System::long_os_version(),
                    "kernel": sysinfo::System::kernel_version(),
                    "cpu": sys.global_cpu_info().brand(),
                    "mem": sys.total_memory(),
                    "datetime": datetime,
                });

                log::debug!("System: {}", serde_json::to_string_pretty(&json).unwrap());

                let json = serde_json::to_string(&json).unwrap();
                writeln!(&mut file_writer, "{json}").unwrap();
            }

            // Write the args
            {
                log::debug!("Args: {}", serde_json::to_string_pretty(&args).unwrap());

                let mut args = serde_json::to_value(&args).unwrap();
                args["cmd"] = std::env::args().collect::<Vec<String>>().into();
                let json = serde_json::to_string(&args).unwrap();
                writeln!(&mut file_writer, "{json}").unwrap();
            }

            // Write the table headers
            {
                let json = serde_json::json!(COLUMN_HEADERS);
                writeln!(&mut file_writer, "{json}").unwrap();
            }

            let db = DatabaseBuilder::build(&data_dir, args);

            let finished = Arc::new(AtomicIsize::new(-1));

            let monitor = start_monitor(
                file_writer,
                data_dir,
                sys,
                db.clone(),
                args.clone(),
                finished.clone(),
                out_path,
            );

            run_workload(db, &cmd, finished.clone());

            monitor.join().unwrap();
        }
    }

    Ok(())
}
