mod args;
mod corpus;
mod db;
mod monitor;
mod report;
mod workload;

use args::Args;
use clap::Parser;
use db::{Backend, DatabaseBuilder};
use monitor::start_monitor;
use std::io::Write;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use workload::run_workload;

use crate::report::generate_report;

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

/// Gets the unix timestamp as a duration
pub fn unix_timestamp() -> std::time::Duration {
    let now = std::time::SystemTime::now();

    // NOTE: Unwrap is trivial
    #[allow(clippy::unwrap_used)]
    now.duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
}

pub fn main() -> std::io::Result<()> {
    env_logger::Builder::from_default_env()
        .filter_module("rust_storage_bench", log::LevelFilter::Debug)
        .init();

    log::info!("rust-storage-bench {}", env!("CARGO_PKG_VERSION"));
    {
        use chrono::{DateTime, Utc};
        use std::time::SystemTime;

        let now = SystemTime::now();
        let now: DateTime<Utc> = now.into();
        log::debug!("Datetime: {now}");
    }

    match Args::parse().command {
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

            if data_dir.try_exists()? {
                std::fs::remove_dir_all(&data_dir).unwrap();
            }

            // The disk format of a log file is like this:
            // { system info object }
            // { args object }
            // [table header 1, table header 2, table header 3]
            // [data point, data point, data point]
            // [data point, data point, data point]
            // { fin: true }
            let mut file_writer = std::fs::File::create(out_path).unwrap();

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
                    "os": sysinfo::System::long_os_version(),
                    "kernel": sysinfo::System::kernel_version(),
                    "cpu": sys.global_cpu_info().brand(),
                    "mem": sys.total_memory(),
                    "datetime": datetime,
                    "ts": start_time.as_millis(),
                    "allocator": allocator,
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
                let json = serde_json::json!([
                    "time_ms",
                    "cpu",
                    "mem_kib",
                    "disk_space_kib",
                    "disk_writes_kib",
                    "disk_reads_kib",
                    //
                    "disk_segment_count", // TODO: replace with level_sizes: [L0, L1, L2, L3, L4, L5, L6]
                    "blob_file_count",
                    "journal_count",
                    "journal_size",
                    "bloom_filter_size",
                    "block_index_size",
                    "cache_size",
                    "write_buffer_size",
                    "tree_height",
                    "fragmented_bytes",
                    "running_compactions",
                    "time_compacting_us",
                    "l0_runs",
                    "l0_segment_avg_lifetime_ms",
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
                ]);
                writeln!(&mut file_writer, "{json}").unwrap();
            }

            let db = DatabaseBuilder::build(&data_dir, args);

            let finished = Arc::new(AtomicBool::default());

            let monitor = start_monitor(
                file_writer,
                data_dir,
                sys,
                db.clone(),
                args.clone(),
                finished.clone(),
            );

            run_workload(db, &cmd, finished.clone());

            monitor.join().unwrap();
        }
    }

    Ok(())
}
