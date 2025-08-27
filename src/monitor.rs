use crate::{args::CommonRunOptions, db::DatabaseWrapper, unix_timestamp};
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    sync::{
        atomic::{AtomicIsize, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};
use sysinfo::{Pid, ProcessRefreshKind, System};

pub fn start_monitor(
    file_writer: File,
    data_dir: PathBuf,
    mut sys: System,
    db: DatabaseWrapper,
    args: CommonRunOptions,
    finish_signal: Arc<AtomicIsize>,
    out_path: PathBuf,
) -> JoinHandle<()> {
    let mut prev_write_ops = 0;
    let mut prev_point_read_ops = 0;
    let mut prev_range_ops = 0;
    let mut prev_delete_ops = 0;

    let mut file_writer = BufWriter::new(file_writer);

    log::debug!("Starting monitor");

    let pid = std::process::id();
    let pid = Pid::from(pid as usize);

    let start_instant = Instant::now();

    let granularity_ms = if let Some(desired_datapoint_count) = args.auto_granularity {
        log::trace!("Ignoring granularity setting, instead using auto granularity");
        (args.seconds / desired_datapoint_count) * 1_000
    } else {
        args.granularity_ms
    };
    log::debug!("Using granularity: {granularity_ms}ms");

    // "How often does this run per second?"
    let frequency = (Duration::from_secs(1).as_millis() as f64) / (granularity_ms as f64);

    std::thread::Builder::new()
        .name("monitor".to_owned())
        .spawn(move || {
            let mut potential_write_ops = 0;
            let mut potential_point_read_ops = 0;
            let mut potential_range_ops = 0;
            let mut potential_delete_ops = 0;

            loop {
                let duration = Duration::from_millis(granularity_ms.into());
                std::thread::sleep(duration);

                sys.refresh_process_specifics(pid, ProcessRefreshKind::everything());

                let proc = sys.processes();
                let child = proc.get(&pid).unwrap();

                let time_ms = start_instant.elapsed().as_millis();
                let cpu = child.cpu_usage();
                let mem_kib = (child.memory() as f64 / 1_024.0) as u64;

                // if mem_kib >= 16 * 1_024 * 1_024 {
                //     log::error!("OOM KILLER!! Exceeded 16GB of memory");
                //     std::process::exit(666);
                // }

                let disk_space_kib = fs_extra::dir::get_size(&data_dir).unwrap_or_default() / 1_024;

                // 500 GiB limit
                if disk_space_kib >= 500 * 1_024 * 1_024 {
                    log::error!("DRIVE LIMITER!! Exceeded 500 GiB of data, good job");
                    std::process::exit(0);
                }

                let workload_real_bytes = db.workload_real_bytes.load(Ordering::Relaxed);
                let disk = child.disk_usage();

                let read_user_bytes = db.range_read_bytes.load(Ordering::Relaxed)
                    + db.point_read_bytes.load(Ordering::Relaxed);

                let written_user_bytes = db.written_bytes.load(Ordering::Relaxed);
                let write_amp = if written_user_bytes == 0 {
                    0.0
                } else {
                    (disk.total_written_bytes as f64) / (written_user_bytes as f64)
                };

                let disk_writes_kib = disk.total_written_bytes / 1_024;
                let disk_reads_kib = disk.total_read_bytes / 1_024;

                let write_ops = db.write_ops.load(Ordering::Relaxed);
                let point_read_ops = db.point_read_ops.load(Ordering::Relaxed);
                let range_ops = db.range_ops.load(Ordering::Relaxed);
                let delete_ops = db.delete_ops.load(Ordering::Relaxed);

                let accumulated_write_latency = db
                    .write_latency
                    .fetch_min(0, std::sync::atomic::Ordering::Release);
                let write_ops_since = write_ops - prev_write_ops;
                let avg_write_latency = accumulated_write_latency / write_ops_since.max(1);
                let write_rate_per_second = if avg_write_latency > 0 {
                    Duration::from_secs(1).as_nanos() as u64 / avg_write_latency
                } else {
                    0
                };
                potential_write_ops += (write_rate_per_second as f64 / frequency) as u64;

                let accumulated_point_read_latency = db
                    .point_read_latency
                    .fetch_min(0, std::sync::atomic::Ordering::Release);
                let point_read_ops_since = point_read_ops - prev_point_read_ops;
                let avg_point_read_latency =
                    accumulated_point_read_latency / point_read_ops_since.max(1);
                let point_read_rate_per_second = (Duration::from_secs(1).as_nanos() as u64)
                    .checked_div(avg_point_read_latency)
                    .unwrap_or_default();
                potential_point_read_ops += (point_read_rate_per_second as f64 / frequency) as u64;

                let accumulated_range_latency = db
                    .range_latency
                    .fetch_min(0, std::sync::atomic::Ordering::Release);
                let range_ops_since = range_ops - prev_range_ops;
                let avg_range_latency = accumulated_range_latency / range_ops_since.max(1);
                let range_rate_per_second = if avg_range_latency > 0 {
                    Duration::from_secs(1).as_nanos() as u64 / avg_range_latency
                } else {
                    0
                };
                potential_range_ops += (range_rate_per_second as f64 / frequency) as u64;

                let accumulated_delete_latency = db
                    .delete_latency
                    .fetch_min(0, std::sync::atomic::Ordering::Release);
                let delete_ops_since = delete_ops - prev_delete_ops;
                let avg_delete_latency = accumulated_delete_latency / delete_ops_since.max(1);
                let delete_rate_per_second = if avg_delete_latency > 0 {
                    Duration::from_secs(1).as_nanos() as u64 / avg_delete_latency
                } else {
                    0
                };
                potential_delete_ops += (delete_rate_per_second as f64 / frequency) as u64;

                let space_amp = if workload_real_bytes == 0 {
                    0.0
                } else {
                    ((disk_space_kib * 1_024) as f64) / (workload_real_bytes as f64)
                };

                let read_amp = if read_user_bytes == 0 {
                    0.0
                } else {
                    (disk.total_read_bytes as f64) / (read_user_bytes as f64)
                };

                let l0_avg_segment_lifetime_ms = {
                    let now = unix_timestamp().as_micros();
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
                    mem_kib,
                    //
                    disk_space_kib,
                    disk_writes_kib,
                    disk_reads_kib,
                    //
                    db.disk_segment_count(),
                    db.blob_file_count(),
                    db.journal_count(),
                    db.journal_size(),
                    db.bloom_filter_size(),
                    db.block_index_size(),
                    0, // TODO:
                    db.write_buffer_size(),
                    db.tree_height(),
                    db.fragmented_bytes(),
                    db.active_compactions(),
                    db.time_compacting_us(),
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
                    avg_delete_latency,
                    //
                    write_rate_per_second,
                    point_read_rate_per_second,
                    range_rate_per_second,
                    delete_rate_per_second,
                    //
                    potential_write_ops,
                    potential_point_read_ops,
                    potential_range_ops,
                    potential_delete_ops,
                    //
                    format!("{:.2}", write_amp)
                        .parse::<f64>()
                        .unwrap_or_default(),
                    format!("{:.2}", space_amp)
                        .parse::<f64>()
                        .unwrap_or_default(),
                    format!("{:.2}", read_amp)
                        .parse::<f64>()
                        .unwrap_or_default(),
                ]);

                writeln!(&mut file_writer, "{json}").unwrap();

                if finish_signal.load(Ordering::Relaxed) >= 0 {
                    break;
                }

                if (disk_space_kib * 1_024) >= args.max_data_bytes {
                    let disk_space_bytes = disk_space_kib * 1_024;
                    let disk_space = pretty_bytes::converter::convert(disk_space_bytes as f64);
                    log::warn!("Stopping because database size reached {disk_space}");
                    break;
                }

                prev_write_ops = write_ops;
                prev_point_read_ops = point_read_ops;
                prev_range_ops = range_ops;
                prev_delete_ops = delete_ops;
            }

            log::info!("Benchmark finished");
            log::trace!("Writing remaining metrics");

            writeln!(&mut file_writer, "{}", serde_json::json!({ "fin": true })).unwrap();

            {
                let histogram = db.write_latency_histogram.lock().unwrap();
                writeln!(
                    &mut file_writer,
                    "{}",
                    serde_json::json!({
                        "histogram": true,
                        "type": "write",
                        "unit": "ns",
                        "mean": histogram.sum().unwrap_or(0.0) / histogram.count() as f64,
                        "p50": histogram.quantile(0.50).unwrap().unwrap_or_default(),
                        "p90": histogram.quantile(0.90).unwrap().unwrap_or_default(),
                        "p95": histogram.quantile(0.95).unwrap().unwrap_or_default(),
                        "p99": histogram.quantile(0.99).unwrap().unwrap_or_default(),
                    })
                )
                .unwrap();
            }

            {
                let histogram = db.point_read_latency_histogram.lock().unwrap();
                writeln!(
                    &mut file_writer,
                    "{}",
                    serde_json::json!({
                        "histogram": true,
                        "type": "point_read",
                        "unit": "ns",
                        "mean": histogram.sum().unwrap_or(0.0) / histogram.count() as f64,
                        "p50": histogram.quantile(0.50).unwrap().unwrap_or_default(),
                        "p90": histogram.quantile(0.90).unwrap().unwrap_or_default(),
                        "p95": histogram.quantile(0.95).unwrap().unwrap_or_default(),
                        "p99": histogram.quantile(0.99).unwrap().unwrap_or_default(),
                    })
                )
                .unwrap();
            }

            {
                let histogram = db.range_latency_histogram.lock().unwrap();
                writeln!(
                    &mut file_writer,
                    "{}",
                    serde_json::json!({
                        "histogram": true,
                        "type": "range_read",
                        "unit": "ns",
                        "mean": histogram.sum().unwrap_or(0.0) / histogram.count() as f64,
                        "p50": histogram.quantile(0.50).unwrap().unwrap_or_default(),
                        "p90": histogram.quantile(0.90).unwrap().unwrap_or_default(),
                        "p95": histogram.quantile(0.95).unwrap().unwrap_or_default(),
                        "p99": histogram.quantile(0.99).unwrap().unwrap_or_default(),
                    })
                )
                .unwrap();
            }

            log::trace!("Flushing metrics file");

            file_writer.flush().unwrap();
            file_writer.into_inner().unwrap().sync_all().unwrap();

            if out_path.extension().and_then(|s| s.to_str()) == Some("gzip") {
                use base64::Engine;
                use flate2::write::GzEncoder;
                use flate2::Compression;

                log::trace!("Compressing output file using gzip");

                let contents = std::fs::read_to_string(&out_path).unwrap();

                let mut encoder = GzEncoder::new(Vec::new(), Compression::best());
                encoder.write_all(contents.as_bytes()).unwrap();
                let compressed_bytes = encoder.finish().unwrap();
                let encoded_string =
                    base64::engine::general_purpose::STANDARD.encode(&compressed_bytes);

                std::fs::write(&out_path, encoded_string).unwrap();
            }

            std::process::exit(finish_signal.load(Ordering::Relaxed).abs() as i32);
        })
        .unwrap()
}
