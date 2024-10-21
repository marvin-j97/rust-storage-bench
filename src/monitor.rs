use crate::{args::RunOptions, db::DatabaseWrapper};
use std::{
    fs::File,
    io::Write,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};
use sysinfo::{Pid, ProcessRefreshKind, System};

pub fn start_monitor(
    mut file_writer: File,
    data_dir: PathBuf,
    mut sys: System,
    db: DatabaseWrapper,
    args: RunOptions,
    finish_signal: Arc<AtomicBool>,
) -> JoinHandle<()> {
    let mut prev_write_ops = 0;
    let mut prev_point_read_ops = 0;

    println!("Starting monitor");

    let pid = std::process::id();
    let pid = Pid::from(pid as usize);

    let start_instant = Instant::now();

    std::thread::spawn(move || {
        // "How often does this run per second?"
        let frequency = (Duration::from_secs(1).as_millis() as f32) / (args.granularity_ms as f32);

        let mut potential_write_ops = 0;
        let mut potential_point_read_ops = 0;

        loop {
            let duration = Duration::from_millis(args.granularity_ms.into());
            std::thread::sleep(duration);

            sys.refresh_process_specifics(pid, ProcessRefreshKind::everything());

            let proc = sys.processes();
            let child = proc.get(&pid).unwrap();

            let time_ms = start_instant.elapsed().as_millis();
            let cpu = child.cpu_usage();
            let mem = (child.memory() as f32 / 1_024.0) as u64;

            if mem >= 16 * 1_024 * 1_024 {
                println!("OOM KILLER!! Exceeded 16GB of memory");
                std::process::exit(666);
            }

            let disk_space_kib = fs_extra::dir::get_size(&data_dir).unwrap_or_default() / 1_024;

            let disk = child.disk_usage();

            let written_user_bytes = db.written_bytes.load(Ordering::Relaxed);
            let write_amp = (disk.total_written_bytes as f64) / (written_user_bytes as f64);

            let disk_writes_kib = disk.total_written_bytes / 1_024;
            let disk_reads_kib = disk.total_read_bytes / 1_024;

            let write_ops = db.write_ops.load(Ordering::Relaxed);
            let point_read_ops = db.point_read_ops.load(Ordering::Relaxed);
            let range_ops = 0;
            let delete_ops = 0;

            let accumulated_write_latency = db
                .write_latency
                .fetch_min(0, std::sync::atomic::Ordering::Release);
            let write_ops_since = write_ops - prev_write_ops;
            let avg_write_latency = accumulated_write_latency / write_ops_since.max(1);
            let write_rate_per_second = if avg_write_latency > 0 {
                Duration::from_secs(1).as_nanos() / avg_write_latency as u128
            } else {
                0
            };
            potential_write_ops += (write_rate_per_second as f32 / frequency) as u64;

            let accumulated_point_read_latency = db
                .point_read_latency
                .fetch_min(0, std::sync::atomic::Ordering::Release);
            let point_read_ops_since = point_read_ops - prev_point_read_ops;
            let avg_point_read_latency =
                accumulated_point_read_latency / point_read_ops_since.max(1);
            let point_read_rate_per_second = if avg_point_read_latency > 0 {
                Duration::from_secs(1).as_nanos() / avg_point_read_latency as u128
            } else {
                0
            };
            potential_point_read_ops += (point_read_rate_per_second as f32 / frequency) as u64;

            let json = serde_json::json!([
                time_ms,
                format!("{:.2}", cpu).parse::<f64>().unwrap(),
                mem,
                //
                disk_space_kib,
                disk_writes_kib,
                disk_reads_kib,
                //
                write_ops,
                point_read_ops,
                range_ops,
                delete_ops,
                //
                avg_write_latency,
                avg_point_read_latency,
                0, // TODO:
                0, // TODO:
                //
                write_rate_per_second,
                point_read_rate_per_second,
                0, // TODO:
                0, // TODO:
                //
                potential_write_ops,
                potential_point_read_ops,
                0, // TODO:
                0, // TODO:
                //
                format!("{:.2}", write_amp).parse::<f64>().unwrap(),
                1.0, // TODO:
                1.0, // TODO:
            ]);
            writeln!(&mut file_writer, "{json}").unwrap();

            if finish_signal.load(Ordering::Relaxed) {
                println!("its joever");
                writeln!(&mut file_writer, "{{\"fin\":true}}").unwrap();
                file_writer.sync_all().unwrap();
                std::process::exit(0);
            }

            prev_write_ops = write_ops;
            prev_point_read_ops = point_read_ops;
        }
    })
}
