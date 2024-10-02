mod args;
mod db;
mod monitor;
mod workload;

use args::Args;
use clap::Parser;
use db::DatabaseWrapper;
use monitor::start_monitor;
use std::io::Write;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use workload::run_workload;

#[cfg(feature = "jemalloc")]
#[cfg(not(target_env = "msvc"))]
use jemallocator::Jemalloc;

#[cfg(feature = "jemalloc")]
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

/// Gets the unix timestamp as a duration
pub fn unix_timestamp() -> std::time::Duration {
    let now = std::time::SystemTime::now();

    // NOTE: Unwrap is trivial
    #[allow(clippy::unwrap_used)]
    now.duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
}

pub fn main() {
    env_logger::Builder::from_default_env().init();

    println!("rust-storage-bench {}", env!("CARGO_PKG_VERSION"));
    {
        use chrono::{DateTime, Utc};
        use std::time::SystemTime;

        let now = SystemTime::now();
        let now: DateTime<Utc> = now.into();
        println!("Datetime: {now}");
    }

    let mut args = Args::parse();
    if args.display_name.is_none() {
        args.display_name = Some(args.backend.to_string());
    }

    let data_dir = args.data_dir.clone();

    if data_dir.exists() {
        std::fs::remove_dir_all(&data_dir).unwrap();
    }

    // The disk format of a log file is like this:
    // { system info object }
    // { args object }
    // [table header 1, table header 2, table header 3]
    // [data point, data point, data point]
    // [data point, data point, data point]
    // { fin: true }
    let mut file_writer = std::fs::File::create(&args.out).unwrap();

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

        let jmalloc = {
            #[cfg(feature = "jemallocator")]
            {
                true
            }

            #[cfg(not(feature = "jemallocator"))]
            {
                false
            }
        };

        let json = serde_json::json!({
            "os": sysinfo::System::long_os_version(),
            "kernel": sysinfo::System::kernel_version(),
            "cpu": sys.global_cpu_info().brand(),
            "mem": sys.total_memory(),
            "datetime": datetime,
            "ts": start_time.as_millis(),
            "jemalloc": jmalloc,
        });

        println!("System: {}", serde_json::to_string_pretty(&json).unwrap());

        let json = serde_json::to_string(&json).unwrap();
        writeln!(&mut file_writer, "{json}").unwrap();
    }

    // Write the args
    {
        println!("Args: {}", serde_json::to_string_pretty(&args).unwrap());

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
            "write_amp",
        ]);
        writeln!(&mut file_writer, "{json}").unwrap();
    }

    let db = DatabaseWrapper::load(&data_dir, &args);

    let finished = Arc::new(AtomicBool::default());

    let monitor = start_monitor(
        file_writer,
        data_dir,
        sys,
        db.clone(),
        args.clone(),
        finished.clone(),
    );

    run_workload(db, &args, finished.clone());

    monitor.join().unwrap();
}
