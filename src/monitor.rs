use crate::{args::RunOptions, db::DatabaseWrapper, unix_timestamp};
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
    let mut prev_range_ops = 0;

    log::debug!("Starting monitor");

    let pid = std::process::id();
    let pid = Pid::from(pid as usize);

    let start_instant = Instant::now();

    std::thread::Builder::new()
        .name("monitor".to_owned())
        .spawn(move || {
            // "How often does this run per second?"
            let frequency =
                (Duration::from_secs(1).as_millis() as f32) / (args.granularity_ms as f32);

            let mut potential_write_ops = 0;
            let mut potential_point_read_ops = 0;
            let mut potential_range_ops = 0;

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
                    log::error!("OOM KILLER!! Exceeded 16GB of memory");
                    std::process::exit(666);
                }

                let disk_space_kib = fs_extra::dir::get_size(&data_dir).unwrap_or_default() / 1_024;

                // 500 GiB limit
                if disk_space_kib >= 500 * 1_024 * 1_024 {
                    log::error!("DRIVE LIMITER!! Exceeded 500 GiB of data, good job");
                    std::process::exit(0);
                }

                let disk = child.disk_usage();

                let written_user_bytes = db.written_bytes.load(Ordering::Relaxed);
                let write_amp = (disk.total_written_bytes as f64) / (written_user_bytes as f64);

                let disk_writes_kib = disk.total_written_bytes / 1_024;
                let disk_reads_kib = disk.total_read_bytes / 1_024;

                let write_ops = db.write_ops.load(Ordering::Relaxed);
                let point_read_ops = db.point_read_ops.load(Ordering::Relaxed);
                let range_ops = db.range_ops.load(Ordering::Relaxed);
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
                let point_read_rate_per_second = Duration::from_secs(1)
                    .as_nanos()
                    .checked_div(avg_point_read_latency as u128)
                    .unwrap_or_default();
                potential_point_read_ops += (point_read_rate_per_second as f32 / frequency) as u64;

                let accumulated_range_latency = db
                    .range_latency
                    .fetch_min(0, std::sync::atomic::Ordering::Release);
                let range_ops_since = range_ops - prev_range_ops;
                let avg_range_latency = accumulated_range_latency / range_ops_since.max(1);
                let range_rate_per_second = if avg_range_latency > 0 {
                    Duration::from_secs(1).as_nanos() / avg_range_latency as u128
                } else {
                    0
                };
                potential_range_ops += (range_rate_per_second as f32 / frequency) as u64;

                // TODO: space amp is broken for update workloads because written_user_bytes increases
                // TODO: but an update doesn't actually add more data to the database (logically)
                let space_amp = if disk_space_kib == 0 {
                    0.0
                } else {
                    ((disk_space_kib * 1_024) as f64) / (written_user_bytes as f64)
                };

                let l0_avg_segment_lifetime_ms = {
                    let now = unix_timestamp().as_micros() as u64;
                    let l0_avg_creation_date = db.avg_l0_segment_creation_date_us();
                    if l0_avg_creation_date == 0 {
                        0
                    } else {
                        let diff_us = now - l0_avg_creation_date;
                        diff_us / 1_000
                    }
                };

                let json = serde_json::json!([
                    time_ms,
                    format!("{:.2}", cpu).parse::<f64>().unwrap(),
                    mem,
                    //
                    disk_space_kib,
                    disk_writes_kib,
                    disk_reads_kib,
                    //
                    db.disk_segment_count(),
                    db.blob_file_count(),
                    db.journal_count(),
                    db.bloom_filter_size(),
                    0, // TODO:
                    0, // TODO:
                    db.write_buffer_size(),
                    db.tree_height(),
                    db.fragmented_bytes(),
                    db.active_compactions(),
                    db.time_compacting(),
                    db.l0_runs(),
                    l0_avg_segment_lifetime_ms,
                    //
                    write_ops,
                    point_read_ops,
                    range_ops,
                    delete_ops,
                    //
                    avg_write_latency,
                    avg_point_read_latency,
                    avg_range_latency,
                    0, // TODO:
                    //
                    write_rate_per_second,
                    point_read_rate_per_second,
                    range_rate_per_second,
                    0, // TODO:
                    //
                    potential_write_ops,
                    potential_point_read_ops,
                    potential_range_ops,
                    0, // TODO:
                    //
                    format!("{:.2}", write_amp)
                        .parse::<f64>()
                        .unwrap_or_default(),
                    format!("{:.2}", space_amp)
                        .parse::<f64>()
                        .unwrap_or_default(),
                    1.0, // TODO: read amp
                ]);

                writeln!(&mut file_writer, "{json}").unwrap();

                if finish_signal.load(Ordering::Relaxed) {
                    log::debug!("its joever");

                    writeln!(&mut file_writer, "{}", serde_json::json!({ "fin": true })).unwrap();

                    {
                        // NOTE: We store values in deci-nano-seconds
                        let histogram = db.write_latency_histogram.lock().unwrap();
                        writeln!(
                            &mut file_writer,
                            "{}",
                            serde_json::json!({
                                "histogram": true,
                                "type": "write",
                                "unit": "ns",
                                "mean": (histogram.mean() * 10.0) as u64,
                                "p50": histogram.value_at_quantile(0.50) * 10,
                                "p90": histogram.value_at_quantile(0.90) * 10,
                                "p95": histogram.value_at_quantile(0.95) * 10,
                                "p99": histogram.value_at_quantile(0.99) * 10,
                            })
                        )
                        .unwrap();
                    }

                    {
                        // NOTE: We store values in deci-nano-seconds
                        let histogram = db.point_read_latency_histogram.lock().unwrap();
                        writeln!(
                            &mut file_writer,
                            "{}",
                            serde_json::json!({
                                "histogram": true,
                                "type": "point_read",
                                "unit": "ns",
                                "mean": (histogram.mean() * 10.0) as u64,
                                "p50": histogram.value_at_quantile(0.50) * 10,
                                "p90": histogram.value_at_quantile(0.90) * 10,
                                "p95": histogram.value_at_quantile(0.95) * 10,
                                "p99": histogram.value_at_quantile(0.99) * 10,
                            })
                        )
                        .unwrap();
                    }

                    file_writer.sync_all().unwrap();

                    std::process::exit(0);
                }

                prev_write_ops = write_ops;
                prev_point_read_ops = point_read_ops;
                prev_range_ops = range_ops;
            }
        })
        .unwrap()
}
