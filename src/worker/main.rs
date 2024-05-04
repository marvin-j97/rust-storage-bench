mod db;

use crate::db::DatabaseWrapper;
use clap::Parser;
use db::GenericDatabase;
use rand::distributions::Distribution;
use rand::Rng;
use rust_storage_bench::{Args, Backend, Workload};
use std::fs::{create_dir_all, remove_dir_all};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use sysinfo::Pid;
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

    let args = Arc::new(Args::parse());

    eprintln!("Workload: {:?}", args.workload);
    eprintln!("Backend : {:?}", args.backend);
    eprintln!("Threads : {}", args.threads);
    eprintln!("# items : {}", args.items);

    if args.workload != Workload::TaskC {
        if args.fsync && (args.backend == Backend::Sled/*|| args.backend == Backend::Bloodstone*/) {
            panic!("Sled doesn't fsync...");
        }
    }

    let data_dir = Path::new(".data").join(match args.backend {
        Backend::Fjall => match args.lsm_compaction {
            rust_storage_bench::LsmCompaction::Leveled => "fjall_lcs".to_owned(),
            rust_storage_bench::LsmCompaction::Tiered => "fjall_stcs".to_owned(),
        },
        be => be.to_string(),
    });

    if data_dir.exists() {
        remove_dir_all(&data_dir).unwrap();
    }

    let db = match args.backend {
        Backend::Fjall => {
            use fjall::{
                compaction::{Levelled, SizeTiered, Strategy},
                BlockCache, PartitionCreateOptions,
            };

            let compaction_strategy: Arc<dyn Strategy + Send + Sync> = match args.lsm_compaction {
                rust_storage_bench::LsmCompaction::Leveled => Arc::new(Levelled::default()),
                rust_storage_bench::LsmCompaction::Tiered => Arc::new(SizeTiered::default()),
            };

            let config = fjall::Config::new(&data_dir)
                .fsync_ms(if args.fsync { None } else { Some(1_000) })
                .block_cache(BlockCache::with_capacity_bytes(args.cache_size.into()).into());

            let create_opts = PartitionCreateOptions::default();

            let keyspace = config.open().unwrap();
            let db = keyspace.open_partition("data", create_opts).unwrap();
            db.set_compaction_strategy(compaction_strategy);

            GenericDatabase::Fjall { keyspace, db }
        }
        Backend::Sled => GenericDatabase::Sled(
            sled::Config::new()
                .path(&data_dir)
                .flush_every_ms(if args.fsync { None } else { Some(1_000) })
                .cache_capacity(args.cache_size as u64)
                .open()
                .unwrap(),
        ),
        // Backend::Bloodstone => GenericDatabase::Bloodstone(
        //     bloodstone::Config::new()
        //         .cache_capacity_bytes(args.cache_size as usize)
        //         .path(&data_dir)
        //         .open()
        //         .unwrap(),
        // ),
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
            tx.create_index::<String, PersyId>("primary", ValueMode::Replace)
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
        Backend::Nebari => {
            use nebari::{
                tree::{Root, Unversioned},
                Config,
            };

            create_dir_all(&data_dir).unwrap();

            let roots = Config::default_for(data_dir.join("db.nebari"))
                .open()
                .unwrap();
            let tree = roots.tree(Unversioned::tree("data")).unwrap();

            GenericDatabase::Nebari { roots, tree }
        }
    };

    let db = DatabaseWrapper {
        inner: db,
        write_ops: Default::default(),
        read_ops: Default::default(),
        delete_ops: Default::default(),
        scan_ops: Default::default(),
        read_latency: Default::default(),
        write_latency: Default::default(),
    };

    {
        let db = db.clone();
        let args = args.clone();

        std::thread::spawn(move || {
            use std::sync::atomic::Ordering::Relaxed;

            let backend = match args.backend {
                Backend::Fjall => format!("{} {}", args.backend, args.lsm_compaction),
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
                    "os": sysinfo::System::long_os_version(),
                    "kernel": sysinfo::System::kernel_version(),
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
                    "backend": backend.to_string(),
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

            let mut prev_write_ops = 0;
            let mut prev_read_ops = 0;

            loop {
                if let Ok(du_bytes) = fs_extra::dir::get_size(&data_dir) {
                    sys.refresh_all();

                    let cpu = sys.global_cpu_info().cpu_usage();

                    let proc = sys.processes();
                    let child = proc.get(&pid).unwrap();

                    let mem = child.memory() as f32;
                    let disk = child.disk_usage();

                    let write_ops = db.write_ops.load(Relaxed);
                    let read_ops = db.read_ops.load(Relaxed);

                    let dataset_size_bytes =
                        write_ops as f64 * (args.key_size as f64 + args.value_size as f64);

                    let space_amp = du_bytes as f64 / dataset_size_bytes;

                    let write_amp = disk.total_written_bytes as f64 / dataset_size_bytes;

                    let accumulated_write_latency = db
                        .write_latency
                        .fetch_min(0, std::sync::atomic::Ordering::Release);

                    let accumulated_read_latency = db
                        .read_latency
                        .fetch_min(0, std::sync::atomic::Ordering::Release);

                    let write_ops_since = write_ops - prev_write_ops;
                    let read_ops_since = read_ops - prev_read_ops;

                    let avg_write_latency = accumulated_write_latency / write_ops_since.max(1);
                    let avg_read_latency = accumulated_read_latency / read_ops_since.max(1);

                    let json = serde_json::json!({
                        "backend": backend,
                        "type": "metrics",
                        "time_micro": unix_timestamp().as_micros(),
                        "write_ops": write_ops,
                        "read_ops": read_ops,
                        "delete_ops": db.delete_ops,
                        "scan_ops": db.scan_ops,
                        "cpu": cpu,
                        "mem_bytes": mem,
                        "mem_mib": mem / 1024.0 / 1024.0,
                        "disk_bytes_w": disk.total_written_bytes,
                        "disk_bytes_r": disk.total_read_bytes,
                        "disk_mib_w": (disk.total_written_bytes as f32) / 1024.0 / 1024.0,
                        "disk_mib_r": (disk.total_read_bytes as f32) / 1024.0 / 1024.0,
                        "du_bytes": du_bytes,
                        "du_mib": (du_bytes as f32) / 1024.0 / 1024.0,
                        "space_amp": space_amp,
                        "write_amp": write_amp,
                        "dataset_size": dataset_size_bytes,
                        "avg_write_latency": avg_write_latency,
                        "avg_read_latency": avg_read_latency,
                    });

                    prev_write_ops = write_ops;
                    prev_read_ops = read_ops;

                    writeln!(
                        &mut file_writer,
                        "{}",
                        serde_json::to_string(&json).unwrap()
                    )
                    .unwrap();
                }

                // As minutes increase, decrease granularity
                // to keep log files low(ish)
                let sec = args.minutes as f32 / 2.0;
                let duration = Duration::from_secs_f32(sec);
                std::thread::sleep(duration);
            }
        });
    }

    match args.workload {
        Workload::TaskA => {
            let users = args.threads;

            {
                let mut rng = rand::thread_rng();

                for idx in 0..users {
                    let user_id = format!("user{idx}");

                    for x in 0..args.items {
                        let mut val: Vec<u8> = Vec::with_capacity(args.value_size.into());
                        for _ in 0..args.value_size {
                            val.push(rng.gen::<u8>());
                        }

                        let key = format!("{user_id}:{x}");
                        let key = key.as_bytes();

                        db.insert(key, &val, false, args.clone());
                    }
                }
            }

            let threads = (0..users)
                .map(|idx| {
                    let args = args.clone();
                    let db = db.clone();
                    let user_id = format!("user{idx}");

                    std::thread::spawn(move || {
                        let mut rng = rand::thread_rng();

                        let zipf = ZipfDistribution::new((args.items - 1) as usize, 0.99).unwrap();

                        loop {
                            let x = zipf.sample(&mut rng);
                            let key = format!("{user_id}:{x}");
                            let key = key.as_bytes();

                            let choice: f32 = rng.gen_range(0.0..1.0);

                            if choice > 0.5 {
                                let mut val: Vec<u8> = Vec::with_capacity(args.value_size.into());
                                for _ in 0..args.value_size {
                                    val.push(rng.gen::<u8>());
                                }

                                db.insert(key, &val, args.fsync, args.clone());
                            } else {
                                db.get(key).unwrap();
                            }
                        }
                    })
                })
                .collect::<Vec<_>>();

            start_killer(args.minutes.into());

            for t in threads {
                t.join().unwrap();
            }
        }

        Workload::TaskB => {
            let users = args.threads;

            {
                let mut rng = rand::thread_rng();

                for idx in 0..users {
                    let user_id = format!("user{idx}");

                    for x in 0..args.items {
                        let mut val: Vec<u8> = Vec::with_capacity(args.value_size.into());
                        for _ in 0..args.value_size {
                            val.push(rng.gen::<u8>());
                        }

                        let key = format!("{user_id}:{x}");
                        let key = key.as_bytes();

                        db.insert(key, &val, false, args.clone());
                    }
                }
            }

            let threads = (0..users)
                .map(|idx| {
                    let args = args.clone();
                    let db = db.clone();
                    let user_id = format!("user{idx}");

                    std::thread::spawn(move || {
                        let mut rng = rand::thread_rng();

                        let zipf = ZipfDistribution::new((args.items - 1) as usize, 0.99).unwrap();

                        loop {
                            let x = zipf.sample(&mut rng);
                            let key = format!("{user_id}:{x}");
                            let key = key.as_bytes();

                            let choice: f32 = rng.gen_range(0.0..1.0);

                            if choice > 0.95 {
                                let mut val: Vec<u8> = Vec::with_capacity(args.value_size.into());
                                for _ in 0..args.value_size {
                                    val.push(rng.gen::<u8>());
                                }

                                db.insert(key, &val, args.fsync, args.clone());
                            } else {
                                db.get(key).unwrap();
                            }
                        }
                    })
                })
                .collect::<Vec<_>>();

            start_killer(args.minutes.into());

            for t in threads {
                t.join().unwrap();
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

                db.insert(&key, &val, false, args.clone());
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
            let users = args.threads;

            {
                let mut rng = rand::thread_rng();

                for idx in 0..users {
                    let user_id = format!("user{idx}");

                    for x in 0..args.items {
                        let mut val: Vec<u8> = Vec::with_capacity(args.value_size.into());
                        for _ in 0..args.value_size {
                            val.push(rng.gen::<u8>());
                        }

                        let key = format!("{user_id}:{x}");
                        let key = key.as_bytes();

                        db.insert(key, &val, false, args.clone());
                    }
                }
            }

            let threads = (0..users)
                .map(|idx| {
                    let args = args.clone();
                    let db = db.clone();
                    let user_id = format!("user{idx}");

                    std::thread::spawn(move || {
                        let mut rng = rand::thread_rng();
                        let mut records = args.items;

                        loop {
                            let choice: f32 = rng.gen_range(0.0..1.0);

                            if choice > 0.95 {
                                let mut val: Vec<u8> = Vec::with_capacity(args.value_size.into());
                                for _ in 0..args.value_size {
                                    val.push(rng.gen::<u8>());
                                }

                                let key = format!("{user_id}:{records}");
                                let key = key.as_bytes();

                                db.insert(key, &val, args.fsync, args.clone());
                                records += 1;
                            } else {
                                let key = format!("{user_id}:{}", records - 1);
                                let key = key.as_bytes();

                                db.get(key).unwrap();
                            }
                        }
                    })
                })
                .collect::<Vec<_>>();

            start_killer(args.minutes.into());

            for t in threads {
                t.join().unwrap();
            }
        }

        Workload::TaskE => {
            let users = args.threads;

            {
                let mut rng = rand::thread_rng();

                for idx in 0..users {
                    let user_id = format!("user{idx}");

                    for x in 0..args.items {
                        let mut val: Vec<u8> = Vec::with_capacity(args.value_size.into());
                        for _ in 0..args.value_size {
                            val.push(rng.gen::<u8>());
                        }

                        let key = format!("{user_id}:{x}");
                        let key = key.as_bytes();

                        db.insert(key, &val, false, args.clone());
                    }
                }
            }

            let threads = (0..users)
                .map(|idx| {
                    let args = args.clone();
                    let db = db.clone();
                    let user_id = format!("user{idx}");

                    std::thread::spawn(move || {
                        let mut rng = rand::thread_rng();
                        let mut records = args.items;

                        loop {
                            let choice: f32 = rng.gen_range(0.0..1.0);

                            if choice < 0.95 {
                                let mut val: Vec<u8> = Vec::with_capacity(args.value_size.into());
                                for _ in 0..args.value_size {
                                    val.push(rng.gen::<u8>());
                                }

                                let key = format!("{user_id}:{records}");
                                let key = key.as_bytes();

                                db.insert(key, &val, args.fsync, args.clone());
                                records += 1;
                            } else {
                                let key = format!("{user_id}:{}", records - 1);
                                let key = key.as_bytes();

                                db.get(key).unwrap();
                            }
                        }
                    })
                })
                .collect::<Vec<_>>();

            start_killer(args.minutes.into());

            for t in threads {
                t.join().unwrap();
            }
        }

        Workload::TaskF => {
            let users = args.threads;

            {
                let mut rng = rand::thread_rng();

                for idx in 0..users {
                    let user_id = format!("user{idx}");

                    for x in 0..args.items {
                        let mut val: Vec<u8> = Vec::with_capacity(args.value_size.into());
                        for _ in 0..args.value_size {
                            val.push(rng.gen::<u8>());
                        }

                        let key = format!("{user_id}:{x}");
                        let key = key.as_bytes();

                        db.insert(key, &val, false, args.clone());
                    }
                }
            }

            let threads = (0..users)
                .map(|idx| {
                    let args = args.clone();
                    let db = db.clone();
                    let user_id = format!("user{idx}");

                    std::thread::spawn(move || {
                        let mut rng = rand::thread_rng();
                        let mut records = args.items;

                        loop {
                            let choice: f32 = rng.gen_range(0.0..1.0);

                            if choice > 0.95 {
                                let mut val: Vec<u8> = Vec::with_capacity(args.value_size.into());
                                for _ in 0..args.value_size {
                                    val.push(rng.gen::<u8>());
                                }

                                let key = format!("{user_id}:{records}");
                                let key = key.as_bytes();

                                db.insert(key, &val, args.fsync, args.clone());
                                records += 1;
                            } else {
                                let zipf =
                                    ZipfDistribution::new((records - 1) as usize, 0.99).unwrap();
                                let x = zipf.sample(&mut rng);

                                let key = format!("{user_id}:{x}");
                                let key = key.as_bytes();

                                db.get(key).unwrap();
                            }
                        }
                    })
                })
                .collect::<Vec<_>>();

            start_killer(args.minutes.into());

            for t in threads {
                t.join().unwrap();
            }
        }

        Workload::TaskG => {
            let users = args.threads;

            {
                let mut rng = rand::thread_rng();

                for idx in 0..users {
                    let user_id = format!("user{idx}");

                    for x in 0..args.items {
                        let mut val: Vec<u8> = Vec::with_capacity(args.value_size.into());
                        for _ in 0..args.value_size {
                            val.push(rng.gen::<u8>());
                        }

                        let key = format!("{user_id}:{x}");
                        let key = key.as_bytes();

                        db.insert(key, &val, false, args.clone());
                    }
                }
            }

            let threads = (0..users)
                .map(|idx| {
                    let args = args.clone();
                    let db = db.clone();
                    let user_id = format!("user{idx}");

                    std::thread::spawn(move || {
                        let mut rng = rand::thread_rng();
                        let mut records = args.items;

                        loop {
                            let choice: f32 = rng.gen_range(0.0..1.0);

                            if choice < 0.95 {
                                let mut val: Vec<u8> = Vec::with_capacity(args.value_size.into());
                                for _ in 0..args.value_size {
                                    val.push(rng.gen::<u8>());
                                }

                                let key = format!("{user_id}:{records}");
                                let key = key.as_bytes();

                                db.insert(key, &val, args.fsync, args.clone());
                                records += 1;
                            } else {
                                let zipf =
                                    ZipfDistribution::new((records - 1) as usize, 0.99).unwrap();
                                let x = zipf.sample(&mut rng);

                                let key = format!("{user_id}:{x}");
                                let key = key.as_bytes();

                                db.get(key).unwrap();
                            }
                        }
                    })
                })
                .collect::<Vec<_>>();

            start_killer(args.minutes.into());

            for t in threads {
                t.join().unwrap();
            }
        }
    }
}
