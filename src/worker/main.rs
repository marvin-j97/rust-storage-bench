mod db;

use crate::db::DatabaseWrapper;
use clap::Parser;
use db::GenericDatabase;
use profiler::{Args, Backend, Workload};
use rand::distributions::Distribution;
use rand::Rng;
use std::fs::{create_dir_all, remove_dir_all};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use sysinfo::{CpuExt, Pid, ProcessExt, SystemExt};
use zipf::ZipfDistribution;

/// Gets the unix timestamp as a duration
pub fn unix_timestamp() -> std::time::Duration {
    let now = std::time::SystemTime::now();

    // NOTE: Unwrap is trivial
    #[allow(clippy::unwrap_used)]
    now.duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
}

fn start_killer(min: u64) {
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(min * 60));
        std::process::exit(0);
    });
}

/*
#[cfg(not(target_env = "msvc"))]
use jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;
*/

fn main() {
    env_logger::Builder::from_default_env().init();

    let args = Args::parse();

    eprintln!("Workload: {:?}", args.workload);
    eprintln!("Backend : {:?}", args.backend);
    eprintln!("Threads : {}", args.threads);
    eprintln!("# items : {}", args.items);

    if args.fsync && (args.backend == Backend::Sled || args.backend == Backend::Bloodstone) {
        panic!("Sled doesn't fsync...");
    }

    let data_dir = Path::new(".data").join(args.backend.to_string());

    if data_dir.exists() {
        remove_dir_all(&data_dir).unwrap();
    }

    let db = match args.backend {
        Backend::LsmTree => {
            let compaction_strategy: Arc<
                dyn lsm_tree::compaction::CompactionStrategy + Send + Sync,
            > = match args.lsm_compaction {
                profiler::LsmCompaction::Leveled => {
                    Arc::new(lsm_tree::compaction::Levelled::default())
                }
                profiler::LsmCompaction::Tiered => {
                    Arc::new(lsm_tree::compaction::SizeTiered::default())
                }
            };

            GenericDatabase::MyLsmTree(
                lsm_tree::Config::new(&data_dir)
                    .compaction_strategy(compaction_strategy)
                    .block_cache(
                        lsm_tree::BlockCache::with_capacity_blocks(
                            (args.cache_size / u32::from(args.lsm_block_size)) as usize,
                        )
                        .into(),
                    )
                    .fsync_ms(if args.fsync { None } else { Some(1_000) })
                    .open()
                    .unwrap(),
            )
        }
        Backend::Sled => GenericDatabase::Sled(
            sled::Config::new()
                .path(&data_dir)
                .flush_every_ms(if args.fsync { None } else { Some(1_000) })
                .cache_capacity(args.cache_size as u64)
                .open()
                .unwrap(),
        ),
        Backend::Bloodstone => GenericDatabase::Bloodstone(
            bloodstone::Config::new()
                .cache_capacity_bytes(args.cache_size as usize)
                .path(&data_dir)
                .open()
                .unwrap(),
        ),
        Backend::JammDb => {
            create_dir_all(&data_dir).unwrap();

            let db = jammdb::DB::open(data_dir.join("data.db")).unwrap();
            let tx = db.tx(true).unwrap();
            let _ = tx.create_bucket("data").unwrap();
            tx.commit().unwrap();

            GenericDatabase::Jamm(db)
        }

        Backend::Persy => {
            use persy::{Config, Persy, PersyId, ValueMode};

            create_dir_all(&data_dir).unwrap();

            Persy::create(data_dir.join("data.persy")).unwrap();

            let mut cfg = Config::default();
            cfg.change_cache_size(args.cache_size.into());
            let db = Persy::open(data_dir.join("data.persy"), cfg).unwrap();

            let mut tx = db.begin().unwrap();
            tx.create_segment("data").unwrap();
            tx.create_index::<u64, PersyId>("primary", ValueMode::Replace)
                .unwrap();
            let prepared = tx.prepare().unwrap();
            prepared.commit().unwrap();

            GenericDatabase::Persy(db)
        }
        Backend::Redb => {
            create_dir_all(&data_dir).unwrap();

            GenericDatabase::Redb(Arc::new(
                redb::Builder::new()
                    .set_cache_size(args.cache_size as usize)
                    .create(data_dir.join("my_db.redb"))
                    .unwrap(),
            ))
        }
    };

    let db = DatabaseWrapper {
        inner: db,
        write_ops: Default::default(),
        read_ops: Default::default(),
        delete_ops: Default::default(),
        scan_ops: Default::default(),
    };

    {
        let db = db.clone();

        std::thread::spawn(move || {
            use std::sync::atomic::Ordering::Relaxed;

            let backend = match args.backend {
                Backend::LsmTree => format!("{} {}", args.backend, args.lsm_compaction),
                _ => args.backend.to_string(),
            };

            let mut sys = sysinfo::System::new_all();
            sys.refresh_all();

            let pid = std::process::id();
            let pid = Pid::from(pid as usize);

            let mut file_writer = std::fs::File::create(&args.out).unwrap();

            {
                let json = serde_json::json!({
                    "time_micro": unix_timestamp().as_micros(),
                    "type": "system",
                    "os": sys.long_os_version(),
                    "kernel": sys.kernel_version(),
                    "cpu": sys.global_cpu_info().brand(),
                    "mem": sys.total_memory(),
                });

                writeln!(
                    &mut file_writer,
                    "{}",
                    serde_json::to_string(&json).unwrap()
                )
                .unwrap();
            }

            {
                let json = serde_json::json!({
                    "time_micro": unix_timestamp().as_micros(),
                    "type": "setup",
                    "backend": args.backend.to_string(),
                    "workload": args.workload,
                    "threads": args.threads,
                    "items": args.items,
                    "value_size": args.value_size
                });

                writeln!(
                    &mut file_writer,
                    "{}",
                    serde_json::to_string(&json).unwrap()
                )
                .unwrap();
            }

            loop {
                if let Ok(du) = fs_extra::dir::get_size(&data_dir) {
                    sys.refresh_all();

                    let cpu = sys.global_cpu_info().cpu_usage();

                    let proc = sys.processes();
                    let child = proc.get(&pid).unwrap();

                    let mem = child.memory() as f32;
                    let disk = child.disk_usage();

                    let json = serde_json::json!({
                        "backend": backend,
                        "type": "metrics",
                        "time_micro": unix_timestamp().as_micros(),
                        "write_ops": db.write_ops.load(Relaxed),
                        "read_ops": db.read_ops.load(Relaxed),
                        "delete_ops": db.delete_ops,
                        "scan_ops": db.scan_ops,
                        "cpu": cpu,
                        "mem_bytes": mem,
                        "mem_mib": mem / 1024.0 / 1024.0,
                        "disk_bytes_w": disk.total_written_bytes,
                        "disk_bytes_r": disk.total_read_bytes,
                        "disk_mib_w": (disk.total_written_bytes as f32) / 1024.0 / 1024.0,
                        "disk_mib_r": (disk.total_read_bytes as f32) / 1024.0 / 1024.0,
                        "du_bytes": du,
                        "du_mib": (du as f32) / 1024.0 / 1024.0
                    });

                    writeln!(
                        &mut file_writer,
                        "{}",
                        serde_json::to_string(&json).unwrap()
                    )
                    .unwrap();
                }

                std::thread::sleep(Duration::from_secs(1));
            }
        });
    }

    match args.workload {
        Workload::TaskA => {
            let mut rng = rand::thread_rng();

            for x in 0..args.items {
                let key = (x as u64).to_be_bytes();

                let mut val: Vec<u8> = Vec::with_capacity(args.value_size.into());
                for _ in 0..args.value_size {
                    val.push(rng.gen::<u8>());
                }

                db.insert(&key, &val, args.fsync);
            }

            start_killer(args.minutes.into());

            let zipf = ZipfDistribution::new((args.items - 1) as usize, 0.99).unwrap();

            loop {
                let x = zipf.sample(&mut rng);
                let key = (x as u64).to_be_bytes();

                let choice: f32 = rng.gen_range(0.0..1.0);

                if choice > 0.5 {
                    let mut val: Vec<u8> = Vec::with_capacity(args.value_size.into());
                    for _ in 0..args.value_size {
                        val.push(rng.gen::<u8>());
                    }

                    db.insert(&key, &val, args.fsync);
                } else {
                    db.get(&key).unwrap();
                }
            }
        }

        Workload::TaskB => {
            let mut rng = rand::thread_rng();

            for x in 0..args.items {
                let key = (x as u64).to_be_bytes();

                let mut val: Vec<u8> = Vec::with_capacity(args.value_size.into());
                for _ in 0..args.value_size {
                    val.push(rng.gen::<u8>());
                }

                db.insert(&key, &val, args.fsync);
            }

            start_killer(args.minutes.into());

            let zipf = ZipfDistribution::new((args.items - 1) as usize, 0.99).unwrap();

            loop {
                let x = zipf.sample(&mut rng);
                let key = (x as u64).to_be_bytes();

                let choice: f32 = rng.gen_range(0.0..1.0);

                if choice > 0.95 {
                    let mut val: Vec<u8> = Vec::with_capacity(args.value_size.into());
                    for _ in 0..args.value_size {
                        val.push(rng.gen::<u8>());
                    }

                    db.insert(&key, &val, args.fsync);
                } else {
                    db.get(&key).unwrap();
                }
            }
        }

        Workload::TaskC => {
            let mut rng = rand::thread_rng();

            for x in 0..args.items {
                let key = (x as u64).to_be_bytes();

                let mut val: Vec<u8> = Vec::with_capacity(args.value_size.into());
                for _ in 0..args.value_size {
                    val.push(rng.gen::<u8>());
                }

                db.insert(&key, &val, args.fsync);
            }

            start_killer(args.minutes.into());

            let zipf = ZipfDistribution::new((args.items - 1) as usize, 0.99).unwrap();

            loop {
                let x = zipf.sample(&mut rng);
                let key = (x as u64).to_be_bytes();

                db.get(&key).unwrap();
            }
        }

        Workload::TaskD => {
            let mut rng = rand::thread_rng();

            for x in 0_u64..args.items.into() {
                let key = x.to_be_bytes();

                let mut val: Vec<u8> = Vec::with_capacity(args.value_size.into());
                for _ in 0..args.value_size {
                    val.push(rng.gen::<u8>());
                }

                db.insert(&key, &val, args.fsync);
            }

            start_killer(args.minutes.into());

            let mut records = u64::from(args.items);

            loop {
                let choice: f32 = rng.gen_range(0.0..1.0);

                if choice > 0.95 {
                    let mut val: Vec<u8> = Vec::with_capacity(args.value_size.into());
                    for _ in 0..args.value_size {
                        val.push(rng.gen::<u8>());
                    }

                    let key = records.to_be_bytes();
                    db.insert(&key, &val, args.fsync);
                    records += 1;
                } else {
                    let key = (records - 1).to_be_bytes();
                    db.get(&key).unwrap();
                }
            }
        }

        Workload::TaskE => {
            let mut rng = rand::thread_rng();

            for x in 0_u64..args.items.into() {
                let key = x.to_be_bytes();

                let mut val: Vec<u8> = Vec::with_capacity(args.value_size.into());
                for _ in 0..args.value_size {
                    val.push(rng.gen::<u8>());
                }

                db.insert(&key, &val, args.fsync);
            }

            start_killer(args.minutes.into());

            let mut records = u64::from(args.items);

            loop {
                let choice: f32 = rng.gen_range(0.0..1.0);

                if choice < 0.95 {
                    let mut val: Vec<u8> = Vec::with_capacity(args.value_size.into());
                    for _ in 0..args.value_size {
                        val.push(rng.gen::<u8>());
                    }

                    let key = records.to_be_bytes();
                    db.insert(&key, &val, args.fsync);
                    records += 1;
                } else {
                    let key = (records - 1).to_be_bytes();
                    db.get(&key).unwrap();
                }
            }
        }
    }
}
