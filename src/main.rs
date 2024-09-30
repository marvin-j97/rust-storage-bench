mod db;
mod workload;

use clap::Parser;
use db::Backend;
use db::DatabaseWrapper;
use db::GenericDatabase;
use serde::Serialize;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use sysinfo::Pid;
use workload::Workload;

#[cfg(feature = "jemalloc")]
#[cfg(not(target_env = "msvc"))]
use jemallocator::Jemalloc;

#[cfg(feature = "jemalloc")]
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

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

/// CLI argument parse
#[derive(Clone, Parser, Debug, Serialize)]
#[command(author = "marvin-j97", version = env!("CARGO_PKG_VERSION"), about = "Rust KV-store profiler")]
#[command(propagate_version = true)]
pub struct Args {
    #[arg(long, value_enum)]
    pub backend: Backend,

    #[arg(long)]
    pub data_dir: PathBuf,

    #[arg(long, default_value = "log.jsonl")]
    pub out: PathBuf,

    #[arg(long)]
    pub display_name: Option<String>,

    #[arg(long, value_enum)]
    pub workload: Workload,

    #[arg(long, default_value_t = 1)]
    pub minutes: u16,

    #[arg(long, alias = "granularity", default_value_t = 500)]
    pub granularity_ms: u16,
    // #[arg(long, default_value_t = 1)]
    // pub threads: u8,

    // #[arg(long, default_value_t = 0)]
    // pub items: u32,

    // #[arg(long)]
    // pub key_size: u8,

    // #[arg(long)]
    // pub value_size: u32,

    // /// Use KV-separation
    // #[arg(long, alias = "lsm_kv_sep", default_value_t = false)]
    // pub lsm_kv_separation: bool,

    // /// Block size for LSM-trees
    // #[arg(long, default_value_t = 4_096)]
    // pub lsm_block_size: u16,

    // /// Compaction for LSM-trees
    // #[arg(long, value_enum, default_value_t = LsmCompaction::Leveled)]
    // pub lsm_compaction: LsmCompaction,

    // /// Compression for LSM-trees
    // #[arg(long, value_enum, default_value_t = Compression::Lz4)]
    // pub lsm_compression: Compression,

    // /// Intermittenly flush sled to keep memory usage sane
    // /// This is hopefully a temporary workaround
    // #[arg(long, default_value_t = false)]
    // pub sled_flush: bool,

    // #[arg(long, default_value_t = 16_000_000)]
    // pub cache_size: u64,

    // #[arg(long, default_value = "log.jsonl")]
    // pub out: String,

    // #[arg(long)]
    // pub data_dir: PathBuf,

    // #[arg(long, default_value_t = false)]
    // pub fsync: bool,

    // #[arg(long, default_value_t = 1)]
    // pub minutes: u16,
}

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

    let pid = std::process::id();
    let pid = Pid::from(pid as usize);

    let start_instant = Instant::now();
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

    let db = match args.backend {
        Backend::Bloodstone => GenericDatabase::Bloodstone(
            bloodstone::Config::new()
                // .cache_capacity_bytes(args.cache_size as usize)
                .path(&data_dir)
                .open()
                .unwrap(),
        ),
        Backend::Sled => GenericDatabase::Sled(
            sled::Config::new()
                .path(&data_dir)
                // .flush_every_ms(if args.fsync { None } else { Some(1_000) })
                // .cache_capacity(args.cache_size)
                .open()
                .unwrap(),
        ),
        Backend::Fjall => {
            use fjall::PartitionCreateOptions;

            let config = fjall::Config::new(&data_dir);
            let keyspace = config.open().unwrap();

            let create_opts = PartitionCreateOptions::default();
            let db = keyspace.open_partition("data", create_opts).unwrap();

            /* let compaction_strategy = match args.lsm_compaction {
                rust_storage_bench::LsmCompaction::Leveled => Strategy::Leveled(Leveled {
                    level_ratio: 8,
                    ..Default::default()
                }),
                rust_storage_bench::LsmCompaction::Tiered => {
                    Strategy::SizeTiered(SizeTiered::default())
                }
            };

            let config = fjall::Config::new(&data_dir)
                .max_write_buffer_size(256_000_000)
                .fsync_ms(if args.fsync { None } else { Some(1_000) })
                .block_cache(BlockCache::with_capacity_bytes(args.cache_size).into())
                .blob_cache(fjall::BlobCache::with_capacity_bytes(args.cache_size).into());

            let create_opts = PartitionCreateOptions::default()
                .block_size(args.lsm_block_size.into())
                .compression(match args.lsm_compression {
                    rust_storage_bench::Compression::None => fjall::CompressionType::None,
                    rust_storage_bench::Compression::Lz4 => fjall::CompressionType::Lz4,
                    rust_storage_bench::Compression::Miniz => {
                        unimplemented!()
                        // fjall::CompressionType::Miniz(6)
                    }
                })
                // .max_memtable_size(8_000_000)
                .manual_journal_persist(true)
                .compaction_strategy(compaction_strategy); */

            /* let keyspace = config.open().unwrap();
            let db = if args.lsm_kv_separation {
                keyspace
                    .open_partition("data", create_opts.with_kv_separation(Default::default()))
                    .unwrap()
            } else {
                keyspace.open_partition("data", create_opts).unwrap()
            }; */

            GenericDatabase::Fjall { keyspace, db }
        }
    };

    let db = DatabaseWrapper {
        inner: db,

        write_ops: Default::default(),
        write_latency: Default::default(),
        written_bytes: Default::default(),

        point_read_ops: Default::default(),
        point_read_latency: Default::default(),
        /*  read_ops: Default::default(),
        delete_ops: Default::default(),
        scan_ops: Default::default(),
        read_latency: Default::default(),
        write_latency: Default::default(),
        scan_latency: Default::default(),
        written_bytes: Default::default(),
        deleted_bytes: Default::default(),
        delete_latency: Default::default(), */
    };

    let finished = Arc::new(AtomicBool::default());

    let monitor = {
        let finished = finished.clone();
        let db = db.clone();

        let mut prev_write_ops = 0;
        let mut prev_point_read_ops = 0;

        println!("Starting monitor");

        std::thread::spawn(move || {
            loop {
                let duration = Duration::from_millis(args.granularity_ms.into());
                std::thread::sleep(duration);

                sys.refresh_all();

                let proc = sys.processes();
                let child = proc.get(&pid).unwrap();

                let time_ms = start_instant.elapsed().as_millis();
                let cpu = sys.global_cpu_info().cpu_usage();
                let mem = (child.memory() as f32 / 1_024.0) as u64;

                if mem >= 16 * 1_024 * 1_024 {
                    println!("OOM KILLER!! Exceeded 16GB of memory");
                    std::process::exit(666);
                }

                let disk_space = fs_extra::dir::get_size(&data_dir).unwrap_or_default() / 1_024;

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

                let accumulated_point_read_latency = db
                    .point_read_latency
                    .fetch_min(0, std::sync::atomic::Ordering::Release);
                let point_read_ops_since = point_read_ops - prev_point_read_ops;
                let avg_point_read_latency =
                    accumulated_point_read_latency / point_read_ops_since.max(1);

                let json = serde_json::json!([
                    time_ms,
                    format!("{:.2}", cpu).parse::<f64>().unwrap(),
                    mem,
                    disk_space,
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
                    0,
                    0,
                    //
                    format!("{:.2}", write_amp).parse::<f64>().unwrap(),
                ]);
                writeln!(&mut file_writer, "{json}").unwrap();

                if finished.load(Ordering::Relaxed) {
                    println!("its joever");
                    writeln!(&mut file_writer, "{{\"fin\":true}}").unwrap();
                    file_writer.sync_all().unwrap();
                    std::process::exit(0);
                }

                prev_write_ops = write_ops;
                prev_point_read_ops = point_read_ops;
            }
        })
    };

    fn start_killer(min: u16, signal: Arc<AtomicBool>) {
        println!("Started killer");
        std::thread::sleep(Duration::from_secs(min as u64 * 60));
        signal.store(true, Ordering::Relaxed);
    }

    println!("Starting workload {:?}", args.workload);

    // TODO: match workload
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

            start_killer(args.minutes, finished);
        }
        _ => {
            unimplemented!()
        }
    };

    monitor.join().unwrap();
}
