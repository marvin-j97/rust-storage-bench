mod db;

use crate::db::DatabaseWrapper;
use clap::Parser;
use db::{GenericDatabase, TABLE};
use heed::EnvFlags;
use rand::distributions::Distribution;
use rand::Rng;
use rust_storage_bench::{Args, Backend, Workload};
use serde::{Deserialize, Serialize};
use std::fs::{create_dir_all, remove_dir_all};
use std::io::Write;
use std::sync::atomic::AtomicUsize;
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

#[cfg(not(target_env = "msvc"))]
use jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

// use mimalloc::MiMalloc;

// #[global_allocator]
// static GLOBAL: MiMalloc = MiMalloc;

fn main() {
    env_logger::Builder::from_default_env().init();

    let args = Arc::new(Args::parse());

    println!("Workload: {:?}", args.workload);
    println!("Backend : {:?}", args.backend);
    println!("Threads : {}", args.threads);

    /* if args.workload != Workload::TaskC {
        if args.fsync && (args.backend == Backend::Sled || args.backend == Backend::Bloodstone) {
            panic!("Sled doesn't fsync...");
        }
    } */

    let data_dir = args.data_dir.clone();

    if data_dir.exists() {
        remove_dir_all(&data_dir).unwrap();
    }

    let db = match args.backend {
        #[cfg(feature = "rocksdb")]
        Backend::RocksDb => {
            use rocksdb::BlockBasedOptions;

            create_dir_all(&data_dir).unwrap();

            let mut opts = rocksdb::Options::default();
            opts.create_if_missing(true);
            opts.set_enable_blob_files(args.lsm_kv_separation);
            opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
            opts.set_manual_wal_flush(true);

            let mut bopts = BlockBasedOptions::default();
            bopts.set_block_cache(&rocksdb::Cache::new_lru_cache(args.cache_size as usize));

            opts.set_block_based_table_factory(&bopts);
            opts.set_blob_compression_type(rocksdb::DBCompressionType::Lz4);

            // TODO: how to set blob cache???

            let db = rocksdb::DB::open(&opts, &data_dir).unwrap();
            GenericDatabase::RocksDb(Arc::new(db))
        }

        #[cfg(feature = "heed")]
        Backend::Heed => {
            create_dir_all(&data_dir).unwrap();

            let env = unsafe {
                heed::EnvOpenOptions::new()
                    .map_size(64_000_000_000)
                    .flags(if args.fsync {
                        EnvFlags::NO_READ_AHEAD
                    } else {
                        EnvFlags::NO_SYNC | EnvFlags::NO_READ_AHEAD
                    })
                    .open(&data_dir)
                    .unwrap()
            };

            let mut wtxn = env.write_txn().unwrap();
            let db = env.create_database(&mut wtxn, None).unwrap();
            wtxn.commit().unwrap();

            GenericDatabase::Heed { db, env }
        }
        Backend::Fjall => {
            use fjall::{
                compaction::{Leveled, SizeTiered, Strategy},
                BlockCache, PartitionCreateOptions,
            };

            let compaction_strategy = match args.lsm_compaction {
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
                .compaction_strategy(compaction_strategy);

            let keyspace = config.open().unwrap();
            let db = if args.lsm_kv_separation {
                keyspace
                    .open_partition("data", create_opts.with_kv_separation(Default::default()))
                    .unwrap()
            } else {
                keyspace.open_partition("data", create_opts).unwrap()
            };

            GenericDatabase::Fjall { keyspace, db }
        }
        Backend::Sled => GenericDatabase::Sled(
            sled::Config::new()
                .path(&data_dir)
                .flush_every_ms(if args.fsync { None } else { Some(1_000) })
                .cache_capacity(args.cache_size)
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
            cfg.change_cache_size(args.cache_size);
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

            let db = redb::Builder::new()
                .set_cache_size(args.cache_size as usize)
                .create(data_dir.join("my_db.redb"))
                .unwrap();

            {
                let tx = db.begin_write().unwrap();
                tx.open_table(TABLE).unwrap();
                tx.commit().unwrap();
            }

            GenericDatabase::Redb(Arc::new(db))
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
        scan_latency: Default::default(),
        written_bytes: Default::default(),
        deleted_bytes: Default::default(),
        delete_latency: Default::default(),
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
                    "display_name": args.display_name.as_ref().unwrap_or(&args.backend.to_string()),
                    "backend": backend.to_string(),
                    "workload": args.workload,
                    "threads": args.threads,
                    "items": args.items,
                    "value_size": args.value_size,
                    "cache_size_in_bytes": args.cache_size
                });

                writeln!(
                    &mut file_writer,
                    "{}",
                    serde_json::to_string(&json).unwrap()
                )
                .unwrap();
            }

            let mut prev_write_ops = 0;
            let mut prev_delete_ops = 0;
            let mut prev_read_ops = 0;
            let mut prev_scan_ops = 0;

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
                    let scan_ops = db.scan_ops.load(Relaxed);
                    let delete_ops = db.delete_ops.load(Relaxed);

                    let written_bytes = db.written_bytes.load(Relaxed) as f64;
                    let deleted_bytes = db.deleted_bytes.load(Relaxed) as f64;
                    let dataset_size_bytes = written_bytes - deleted_bytes;

                    let space_amp = du_bytes as f64 / dataset_size_bytes;

                    // TODO: memory amp

                    let write_amp = disk.total_written_bytes as f64 / written_bytes;

                    let accumulated_write_latency = db
                        .write_latency
                        .fetch_min(0, std::sync::atomic::Ordering::Release);

                    let accumulated_delete_latency = db
                        .delete_latency
                        .fetch_min(0, std::sync::atomic::Ordering::Release);

                    let accumulated_read_latency = db
                        .read_latency
                        .fetch_min(0, std::sync::atomic::Ordering::Release);

                    let accumulated_scan_latency = db
                        .scan_latency
                        .fetch_min(0, std::sync::atomic::Ordering::Release);

                    let write_ops_since = write_ops - prev_write_ops;
                    let read_ops_since = read_ops - prev_read_ops;
                    let scan_ops_since = scan_ops - prev_scan_ops;
                    let delete_ops_since = delete_ops - prev_delete_ops;

                    let avg_write_latency = accumulated_write_latency / write_ops_since.max(1);
                    let avg_read_latency = accumulated_read_latency / read_ops_since.max(1);
                    let avg_scan_latency = accumulated_scan_latency / scan_ops_since.max(1);
                    let avg_delete_latency = accumulated_delete_latency / delete_ops_since.max(1);

                    if mem >= 12_000_000_000.0 {
                        println!("OOM KILLER!! Exceeded 12GB of memory");
                        std::process::exit(777);
                    }

                    let json = serde_json::json!({
                        "display_name": args.display_name.as_ref().unwrap_or(&args.backend.to_string()),
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
                        "avg_scan_latency": avg_scan_latency,
                        "avg_delete_latency": avg_delete_latency,
                    });

                    prev_write_ops = write_ops;
                    prev_read_ops = read_ops;
                    prev_scan_ops = scan_ops;
                    prev_delete_ops = delete_ops;

                    writeln!(
                        &mut file_writer,
                        "{}",
                        serde_json::to_string(&json).unwrap()
                    )
                    .unwrap();
                }

                // As minutes increase, decrease granularity
                // to keep log files low(ish)
                let sec = args.minutes as f32 / 4.0;
                let duration = Duration::from_secs_f32(sec);
                std::thread::sleep(duration);
            }
        });
    }

    match args.workload {
        Workload::GarageBlockRef => {
            use std::sync::atomic::Ordering::Relaxed;
            use uuid::Uuid;

            /// Compute the blake2 of a slice
            pub fn blake2sum(data: &[u8]) -> [u8; 32] {
                use blake2::{Blake2b512, Digest};

                let mut hasher = Blake2b512::new();
                hasher.update(data);
                let mut hash = [0u8; 32];
                hash.copy_from_slice(&hasher.finalize()[..32]);

                hash
            }

            let item_count: Arc<AtomicUsize> = Arc::default();

            {
                let args = args.clone();
                let db = db.clone();
                let item_count = item_count.clone();

                std::thread::spawn(move || {
                    let mut key: Vec<u8> = Vec::new();
                    let mut val: Vec<u8> = Vec::with_capacity(args.value_size as usize);

                    loop {
                        key.extend(blake2sum(&Uuid::new_v4().as_u128().to_be_bytes()).to_vec());
                        key.extend(Uuid::new_v4().as_u128().to_be_bytes());

                        val.extend(&key);
                        val.push(0u8);

                        db.insert(&key, &val, args.fsync, args.clone());

                        key.clear();
                        val.clear();

                        item_count.fetch_add(1, Relaxed);
                    }
                })
            };

            loop {
                if item_count.load(Relaxed) >= 100_000_000 {
                    std::process::exit(0);
                }
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        }
        Workload::HtmlDump => {
            let mut ids = vec![];

            match &db.inner {
                GenericDatabase::Redb(redb) => {
                    let mut written_bytes = 0;

                    let start = std::time::Instant::now();

                    let write_txn = redb.begin_write().unwrap();

                    for (idx, dirent) in
                        std::fs::read_dir("/devssd/code/node/smoltable-webcrawler/docs")
                            .unwrap()
                            .enumerate()
                    {
                        let dirent = dirent.unwrap();

                        let key = dirent.file_name().to_string_lossy().to_string();
                        let val = std::fs::read(dirent.path()).unwrap();

                        written_bytes += (key.len() + val.len()) as u64;

                        let mut table = write_txn.open_table(TABLE).unwrap();
                        table.insert(key.as_bytes(), &*val).unwrap();

                        ids.push(key);

                        if idx % 250 == 0 {
                            eprintln!("ingested {idx}");
                        }
                    }

                    write_txn.commit().unwrap();

                    db.written_bytes
                        .fetch_add(written_bytes, std::sync::atomic::Ordering::Relaxed);

                    db.write_latency.fetch_add(
                        start.elapsed().as_nanos() as u64,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    db.write_ops
                        .fetch_add(ids.len() as u64, std::sync::atomic::Ordering::Relaxed);
                }
                _ => {
                    for (idx, dirent) in
                        std::fs::read_dir("/devssd/code/node/smoltable-webcrawler/docs")
                            .unwrap()
                            .enumerate()
                    {
                        let dirent = dirent.unwrap();

                        let key = dirent.file_name().to_string_lossy().to_string();

                        db.insert(
                            key.as_bytes(),
                            &std::fs::read(dirent.path()).unwrap(),
                            args.fsync,
                            args.clone(),
                        );

                        ids.push(key);

                        if idx % 250 == 0 {
                            eprintln!("ingested {idx}");
                        }
                    }
                }
            }

            start_killer(args.minutes.into());

            let mut rng = rand::thread_rng();

            /*   let mut ops = 0;
            let mut start = std::time::Instant::now(); */

            loop {
                let item_count = ids.len();

                let zipf = ZipfDistribution::new(item_count - 1, 1.0).unwrap();
                let idx = zipf.sample(&mut rng);
                let key = &ids[idx];

                assert!(db.len_of_value(key.as_bytes()).unwrap() > 0);

                /* ops += 1;

                if ops % 1_000_000 == 0 {
                    /* db.read_ops
                        .fetch_add(ops, std::sync::atomic::Ordering::Relaxed);

                    db.read_latency.fetch_add(
                        start.elapsed().as_nanos().try_into().unwrap(),
                        std::sync::atomic::Ordering::Relaxed,
                    ); */

                    log::warn!("batch: {ops} in {:?}", start.elapsed());

                    /*  ops = 0;
                    start = std::time::Instant::now(); */
                } */
            }

            //  log::warn!("batch: {ops} in {:?}", start.elapsed());
        }
        Workload::MonotonicFixedRandom => {
            let mut rng = rand::thread_rng();

            let mut val: Vec<u8> = Vec::with_capacity(args.value_size as usize);

            match &db.inner {
                GenericDatabase::Redb(redb) => {
                    let mut written_bytes = 0;

                    let start = std::time::Instant::now();

                    let write_txn = redb.begin_write().unwrap();

                    for x in 0..args.items {
                        for _ in 0..args.value_size {
                            val.push(rng.gen::<u8>());
                        }

                        let key = u128::from(x).to_be_bytes().to_vec();

                        let mut table = write_txn.open_table(TABLE).unwrap();
                        table.insert(&*key, &*val).unwrap();

                        written_bytes += (key.len() + val.len()) as u64;
                        val.clear();

                        if x % 1_000_000 == 0 {
                            println!("Written {x}/{}", args.items);
                        }
                    }

                    write_txn.commit().unwrap();

                    db.written_bytes
                        .fetch_add(written_bytes, std::sync::atomic::Ordering::Relaxed);
                    db.write_latency.fetch_add(
                        start.elapsed().as_nanos() as u64,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    db.write_ops
                        .fetch_add(args.items.into(), std::sync::atomic::Ordering::Relaxed);
                }
                #[cfg(feature = "heed")]
                GenericDatabase::Heed { env, db: heed } => {
                    let mut written_bytes = 0;

                    let start = std::time::Instant::now();

                    let mut write_txn = env.write_txn().unwrap();

                    for x in 0..args.items {
                        for _ in 0..args.value_size {
                            val.push(rng.gen::<u8>());
                        }

                        let key = u128::from(x).to_be_bytes().to_vec();

                        heed.put(&mut write_txn, &key, &val).unwrap();

                        written_bytes += (key.len() + val.len()) as u64;
                        val.clear();

                        if x % 1_000_000 == 0 {
                            println!("Written {x}/{}", args.items);
                        }
                    }

                    write_txn.commit().unwrap();

                    db.written_bytes
                        .fetch_add(written_bytes, std::sync::atomic::Ordering::Relaxed);
                    db.write_latency.fetch_add(
                        start.elapsed().as_nanos() as u64,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    db.write_ops
                        .fetch_add(args.items.into(), std::sync::atomic::Ordering::Relaxed);
                }
                _ => {
                    for x in 0..args.items {
                        for _ in 0..args.value_size {
                            val.push(rng.gen::<u8>());
                        }

                        db.insert(
                            &u128::from(x).to_be_bytes(),
                            &val,
                            // NOTE: Avoid too much disk space building up...
                            args.backend == Backend::Redb && x % 100_000 == 0,
                            args.clone(),
                        );
                        val.clear();

                        /*  if let GenericDatabase::Bloodstone(db) = &db.inner {
                            if x % 10_000 == 0 {
                                db.flush().unwrap();
                            }
                        } */

                        if x % 1_000_000 == 0 {
                            println!("Written {x}/{}", args.items);
                        }
                    }
                }
            }

            if let GenericDatabase::Bloodstone(db) = &db.inner {
                db.flush().unwrap();
            }

            println!("Wrote test data");

            start_killer(args.minutes.into());

            loop {
                let item_count = args.items as usize;

                //let zipf = ZipfDistribution::new(item_count - 1, 1.0).unwrap();
                //let idx = zipf.sample(&mut rng);
                let idx = rng.gen_range(0..item_count);
                let key = (idx as u128).to_be_bytes();

                let got = db.len_of_value(&key).unwrap();
                assert_eq!(args.value_size as usize, got);
            }
        }
        Workload::MonotonicFixed => {
            let mut rng = rand::thread_rng();

            let mut val: Vec<u8> = Vec::with_capacity(args.value_size as usize);

            match &db.inner {
                GenericDatabase::Redb(redb) => {
                    let mut written_bytes = 0;

                    let start = std::time::Instant::now();

                    let write_txn = redb.begin_write().unwrap();

                    for x in 0..args.items {
                        for _ in 0..args.value_size {
                            val.push(rng.gen::<u8>());
                        }

                        let key = u128::from(x).to_be_bytes().to_vec();

                        let mut table = write_txn.open_table(TABLE).unwrap();
                        table.insert(&*key, &*val).unwrap();

                        written_bytes += (key.len() + val.len()) as u64;
                        val.clear();

                        if x % 1_000_000 == 0 {
                            println!("Written {x}/{}", args.items);
                        }
                    }

                    write_txn.commit().unwrap();

                    db.written_bytes
                        .fetch_add(written_bytes, std::sync::atomic::Ordering::Relaxed);
                    db.write_latency.fetch_add(
                        start.elapsed().as_nanos() as u64,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    db.write_ops
                        .fetch_add(args.items.into(), std::sync::atomic::Ordering::Relaxed);
                }
                #[cfg(feature = "heed")]
                GenericDatabase::Heed { env, db: heed } => {
                    let mut written_bytes = 0;

                    let start = std::time::Instant::now();

                    let mut write_txn = env.write_txn().unwrap();

                    for x in 0..args.items {
                        for _ in 0..args.value_size {
                            val.push(rng.gen::<u8>());
                        }

                        let key = u128::from(x).to_be_bytes().to_vec();

                        heed.put(&mut write_txn, &key, &val).unwrap();

                        written_bytes += (key.len() + val.len()) as u64;
                        val.clear();

                        if x % 1_000_000 == 0 {
                            println!("Written {x}/{}", args.items);
                        }
                    }

                    write_txn.commit().unwrap();

                    db.written_bytes
                        .fetch_add(written_bytes, std::sync::atomic::Ordering::Relaxed);
                    db.write_latency.fetch_add(
                        start.elapsed().as_nanos() as u64,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    db.write_ops
                        .fetch_add(args.items.into(), std::sync::atomic::Ordering::Relaxed);
                }
                _ => {
                    for x in 0..args.items {
                        for _ in 0..args.value_size {
                            val.push(rng.gen::<u8>());
                        }

                        db.insert(
                            &u128::from(x).to_be_bytes(),
                            &val,
                            // NOTE: Avoid too much disk space building up...
                            args.backend == Backend::Redb && x % 100_000 == 0,
                            args.clone(),
                        );
                        val.clear();

                        /*  if let GenericDatabase::Bloodstone(db) = &db.inner {
                            if x % 10_000 == 0 {
                                db.flush().unwrap();
                            }
                        } */

                        if x % 1_000_000 == 0 {
                            println!("Written {x}/{}", args.items);
                        }
                    }
                }
            }

            println!("Wrote test data");

            start_killer(args.minutes.into());

            loop {
                let item_count = args.items as usize;

                let zipf = ZipfDistribution::new(item_count - 1, 1.0).unwrap();
                let idx = zipf.sample(&mut rng);
                let key = (idx as u128).to_be_bytes();

                let _got = db.get(&key).unwrap();
            }
        }
        Workload::Monotonic => {
            use std::sync::atomic::Ordering::Relaxed;

            let item_count: Arc<AtomicUsize> = Arc::default();

            let reader = {
                let db = db.clone();
                let item_count = item_count.clone();

                std::thread::spawn(move || {
                    let mut rng = rand::thread_rng();

                    loop {
                        let item_count = item_count.load(Relaxed);

                        if item_count >= 100 {
                            {
                                let zipf = ZipfDistribution::new(item_count - 1, 1.0).unwrap();
                                let idx = zipf.sample(&mut rng);
                                let key = (idx as u128).to_be_bytes();
                                let _got = db.get(&key).unwrap();
                            }

                            /*  {
                                let lower: &[u8] = &(item_count as u128 - 1 - 50).to_be_bytes();
                                let upper: &[u8] = &(item_count as u128 - 1).to_be_bytes();
                                let range = db.range(lower..upper, false);
                                assert_eq!(range.len(), 50);
                            } */
                        }
                    }
                })
            };

            let writer = {
                let args = args.clone();
                let db = db.clone();
                let item_count = item_count.clone();

                std::thread::spawn(move || {
                    let mut rng = rand::thread_rng();
                    let mut val: Vec<u8> = Vec::with_capacity(args.value_size as usize);

                    for x in 0u128.. {
                        for _ in 0..args.value_size {
                            val.push(rng.gen::<u8>());
                        }

                        /* let dur = if args.backend == Backend::Redb {
                            x % 100_000 == 0
                        } else {
                            args.fsync
                        }; */

                        db.insert(&x.to_be_bytes(), &val, args.fsync, args.clone());
                        val.clear();

                        item_count.fetch_add(1, Relaxed);
                    }
                })
            };

            start_killer(args.minutes.into());

            writer.join().unwrap();
            reader.join().unwrap();
        }
        Workload::MonotonicWriteOnly => {
            use std::sync::atomic::Ordering::Relaxed;

            let item_count: Arc<AtomicUsize> = Arc::default();

            let threads = (0..args.threads)
                .map(|_thread_no| {
                    let args = args.clone();
                    let db = db.clone();
                    let item_count = item_count.clone();

                    std::thread::spawn(move || {
                        let mut rng = rand::thread_rng();
                        let mut val: Vec<u8> = Vec::with_capacity(args.value_size as usize);

                        for x in 0u128.. {
                            for _ in 0..args.value_size {
                                val.push(rng.gen::<u8>());
                            }

                            db.insert(&x.to_be_bytes(), &val, args.fsync, args.clone());
                            val.clear();

                            item_count.fetch_add(1, Relaxed);
                        }
                    })
                })
                .collect::<Vec<_>>();

            start_killer(args.minutes.into());

            for t in threads {
                t.join().unwrap();
            }
        }
        Workload::Webtable => {
            use fake::faker::lorem::en::*;
            use fake::Fake;
            use rand::seq::SliceRandom;

            fn generate_domain() -> String {
                let tli = [
                    "com", "de", "en", "org", "uk", "eu", "gov", "es", "it", "fr", "cz", "ru",
                    "cn", "jp", "tw", "ninja", "pizza",
                ];
                let tli = tli.choose(&mut rand::thread_rng()).unwrap();

                let domain = random_string::generate(8, random_string::charsets::ALPHA_LOWER);
                let sub = random_string::generate(5, random_string::charsets::ALPHA_LOWER);

                format!("{tli}.{domain}.{sub}")
            }

            let domain_count = 10_000;

            let domains = (0..domain_count)
                .map(|_| generate_domain())
                .collect::<Vec<_>>();

            let t = {
                let args = args.clone();

                std::thread::spawn(move || {
                    for batch_no in 1.. {
                        for _ in 0..domain_count {
                            let domain = domains.choose(&mut rand::thread_rng()).unwrap();
                            let item_key = format!("{domain}#{}", scru128::new_string());

                            let html: String = Paragraph(10..1_600).fake();

                            db.insert(item_key.as_bytes(), html.as_bytes(), false, args.clone());
                        }

                        {
                            for domain in &domains {
                                // TODO: optimize with prefix_keys
                                let items =
                                    db.prefix(format!("{domain}#").as_bytes(), false, 1_000);

                                let overshoot = items.len().saturating_sub(5);

                                if overshoot > 0 {
                                    for (k, v) in items.into_iter().take(overshoot) {
                                        // TODO: SingleDelete
                                        db.remove(&k, v.len() as u64, false);
                                    }
                                }
                            }
                        }

                        if args.lsm_kv_separation && batch_no % 10 == 0 && batch_no > 0 {
                            if let GenericDatabase::Fjall { db, .. } = &db.inner {
                                use fjall::GarbageCollection;

                                let report = db.gc_scan().unwrap();
                                if report.space_amp() > 2.0 {
                                    db.gc_with_space_amp_target(2.0).unwrap();
                                    println!("GC done");
                                }
                            }
                        }

                        // NOTE: To not bloat disk space
                        if let GenericDatabase::Redb(db) = &db.inner {
                            let wtx = db.begin_write().unwrap();
                            wtx.commit().unwrap();
                        }
                    }
                })
            };

            start_killer(args.minutes.into());

            t.join().unwrap();
        }
        Workload::Timeseries => {
            let mut val: Vec<u8> = Vec::with_capacity(args.value_size as usize);

            let item_count: Arc<AtomicUsize> = Arc::default();

            let reader = {
                let db = db.clone();
                let item_count = item_count.clone();

                std::thread::spawn(move || loop {
                    /*   if item_count.load(std::sync::atomic::Ordering::Relaxed) > 1_000 {
                        let item_count =
                            item_count.load(std::sync::atomic::Ordering::Relaxed) as u128;
                        let lower = item_count - 100;
                        let lower: &[u8] = &lower.to_be_bytes();
                        let range = lower..;
                        let _scanned = db.range_len(range, true);
                    } */

                    if item_count.load(std::sync::atomic::Ordering::Relaxed) > 0 {
                        let _ = db.last().unwrap();
                    }
                })
            };

            let writer = {
                let args = args.clone();
                let db = db.clone();

                std::thread::spawn(move || {
                    let mut rng = rand::thread_rng();

                    for x in 0u128.. {
                        for _ in 0..args.value_size {
                            val.push(rng.gen::<u8>());
                        }

                        db.insert(&x.to_be_bytes(), &val, args.fsync, args.clone());
                        val.clear();

                        item_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                })
            };

            start_killer(args.minutes.into());

            writer.join().unwrap();
            reader.join().unwrap();
        }
        Workload::Billion => {
            let mut rng = rand::thread_rng();
            let mut val: Vec<u8> = Vec::with_capacity(args.value_size as usize);

            for x in 0u64..1_000_000_000 {
                for _ in 0..args.value_size {
                    val.push(rng.gen::<u8>());
                }

                db.insert(&x.to_be_bytes(), &val, false, args.clone());
                val.clear();
            }
        }
        Workload::Feed => {
            use fake::faker::boolean::en::*;
            use fake::faker::lorem::en::*;
            use fake::faker::name::en::*;
            use fake::uuid::UUIDv4;
            use fake::{Dummy, Fake, Faker};

            #[derive(Debug, Dummy, Deserialize, Serialize)]
            pub struct UserProfile {
                #[dummy(faker = "Name()")]
                display_name: String,

                #[dummy(faker = "Name()")]
                handle: String,

                #[dummy(faker = "Boolean(50)")]
                is_premium: bool,
            }

            #[derive(Debug, Dummy, Deserialize, Serialize)]
            pub struct FeedPost {
                #[dummy(faker = "Paragraph(1..10)")]
                content: String,

                #[dummy(faker = "UUIDv4")]
                user_id: uuid::Uuid,

                #[dummy(faker = "Boolean(50)")]
                is_pinned: bool,

                #[dummy(faker = "0..1_000_000")]
                likes: usize,

                #[dummy(faker = "0..1_000_000")]
                shares: usize,
            }

            let virtual_users = 1_000;

            for idx in 0..virtual_users {
                let user_id = format!("u{idx:0>7}");
                let user_profile_key = format!("{user_id}#p");

                let profile: UserProfile = Faker.fake();
                let profile = rmp_serde::to_vec(&profile).unwrap();

                db.insert(user_profile_key.as_bytes(), &profile, false, args.clone());

                for _ in 0..100 {
                    // Insert post
                    let post_id = scru128::new_string();
                    let post_key = format!("{user_id}#f#{post_id}");

                    let post: FeedPost = Faker.fake();
                    let post = rmp_serde::to_vec(&post).unwrap();

                    db.insert(post_key.as_bytes(), &post, false, args.clone());
                }
            }

            let threads = (0..args.threads)
                .map(|_thread_no| {
                    let args = args.clone();
                    let db = db.clone();

                    std::thread::spawn(move || {
                        let mut rng = rand::thread_rng();

                        for loop_idx in 0.. {
                            let choice: f32 = rng.gen_range(0.0..1.0);

                            if choice > 0.9 {
                                // Which user?
                                let zipf = ZipfDistribution::new(virtual_users - 1, 1.0).unwrap();
                                let idx = zipf.sample(&mut rng);
                                let user_id = format!("u{idx:0>7}");

                                // Insert post
                                let post_id = scru128::new_string();
                                let post_key = format!("{user_id}#f#{post_id}");

                                let post: FeedPost = Faker.fake();
                                let post = rmp_serde::to_vec(&post).unwrap();

                                db.insert(post_key.as_bytes(), &post, args.fsync, args.clone());

                                if args.lsm_kv_separation && loop_idx > 0 && loop_idx % 100_000 == 0
                                {
                                    if let GenericDatabase::Fjall { db, .. } = &db.inner {
                                        use fjall::GarbageCollection;

                                        let report = db.gc_scan().unwrap();
                                        if report.space_amp() > 2.0 {
                                            db.gc_with_space_amp_target(2.0).unwrap();
                                            println!("GC done");
                                        }
                                    }
                                }
                            } else {
                                // Which user?
                                let zipf = ZipfDistribution::new(virtual_users - 1, 1.0).unwrap();
                                let idx = zipf.sample(&mut rng);
                                let user_id = format!("u{idx:0>7}");

                                // Get profile
                                let user_profile_key = format!("{user_id}#p");
                                db.get(user_profile_key.as_bytes()).unwrap();

                                // + latest 10 posts
                                let feed_prefix = format!("{user_id}#f#");
                                let limit = 10;

                                assert_eq!(
                                    limit,
                                    db.prefix(feed_prefix.as_bytes(), true, limit).len(),
                                    "{feed_prefix} failed"
                                );
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
        Workload::FeedWriteOnly => {
            use fake::faker::boolean::en::*;
            use fake::faker::lorem::en::*;
            use fake::faker::name::en::*;
            use fake::uuid::UUIDv4;
            use fake::{Dummy, Fake, Faker};

            #[derive(Debug, Dummy, Deserialize, Serialize)]
            pub struct UserProfile {
                #[dummy(faker = "Name()")]
                display_name: String,

                #[dummy(faker = "Name()")]
                handle: String,

                #[dummy(faker = "Boolean(50)")]
                is_premium: bool,
            }

            #[derive(Debug, Dummy, Deserialize, Serialize)]
            pub struct FeedPost {
                #[dummy(faker = "Paragraph(1..10)")]
                content: String,

                #[dummy(faker = "UUIDv4")]
                user_id: uuid::Uuid,

                #[dummy(faker = "Boolean(50)")]
                is_pinned: bool,

                #[dummy(faker = "0..1_000_000")]
                likes: usize,

                #[dummy(faker = "0..1_000_000")]
                shares: usize,
            }

            let virtual_users = 10_000;

            for idx in 0..virtual_users {
                let user_id = format!("u{idx:0>7}");
                let user_profile_key = format!("{user_id}#p");

                let profile: UserProfile = Faker.fake();
                let profile = rmp_serde::to_vec(&profile).unwrap();

                db.insert(user_profile_key.as_bytes(), &profile, true, args.clone());

                for _ in 0..10 {
                    // Insert post
                    let post_id = scru128::new_string();
                    let post_key = format!("{user_id}#f#{post_id}");

                    let post: FeedPost = Faker.fake();
                    let post = rmp_serde::to_vec(&post).unwrap();

                    db.insert(post_key.as_bytes(), &post, args.fsync, args.clone());
                }
            }

            let threads = (0..args.threads)
                .map(|_thread_no| {
                    let args = args.clone();
                    let db = db.clone();
                    let users = 0..virtual_users;

                    std::thread::spawn(move || {
                        let mut rng = rand::thread_rng();

                        loop {
                            let idx = rng.gen_range(users.clone());
                            let user_id = format!("u{idx:0>7}");

                            // Insert post
                            let post_id = scru128::new_string();
                            let post_key = format!("{user_id}#f#{post_id}");

                            let post: FeedPost = Faker.fake();
                            let post = rmp_serde::to_vec(&post).unwrap();

                            db.insert(post_key.as_bytes(), &post, args.fsync, args.clone());
                        }
                    })
                })
                .collect::<Vec<_>>();

            start_killer(args.minutes.into());

            for t in threads {
                t.join().unwrap();
            }
        } /* Workload::TaskF => {
              let users = args.threads;

              {
                  let mut rng = rand::thread_rng();

                  for idx in 0..users {
                      let user_id = format!("user{idx:0>2}");

                      for x in 0..args.items {
                          let mut val: Vec<u8> = Vec::with_capacity(args.value_size as usize);
                          for _ in 0..args.value_size {
                              val.push(rng.gen::<u8>());
                          }

                          let key = format!("{user_id:0>2}:{x:0>10}");
                          let key = key.as_bytes();

                          db.insert(key, &val, false, args.clone());
                      }
                  }
              }

              let threads = (0..users)
                  .map(|idx| {
                      let args = args.clone();
                      let db = db.clone();
                      let user_id = format!("user{idx:0>2}");

                      std::thread::spawn(move || {
                          let mut rng = rand::thread_rng();
                          let mut records = args.items;

                          loop {
                              let choice: f32 = rng.gen_range(0.0..1.0);

                              if choice > 0.95 {
                                  let mut val: Vec<u8> = Vec::with_capacity(args.value_size as usize);
                                  for _ in 0..args.value_size {
                                      val.push(rng.gen::<u8>());
                                  }

                                  let key = format!("{user_id}:{records:0>10}");
                                  let key = key.as_bytes();

                                  db.insert(key, &val, args.fsync, args.clone());
                                  records += 1;
                              } else {
                                  let zipf =
                                      ZipfDistribution::new((records - 1) as usize, 0.99).unwrap();
                                  let x = zipf.sample(&mut rng);

                                  let key = format!("{user_id}:{x:0>10}");
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
          } */ /* Workload::TaskA => {
                let users = args.threads;

                {
                    let mut rng = rand::thread_rng();

                    for idx in 0..users {
                        let user_id = format!("user{idx:0>2}");

                        for x in 0..args.items {
                            let mut val: Vec<u8> = Vec::with_capacity(args.value_size as usize);
                            for _ in 0..args.value_size {
                                val.push(rng.gen::<u8>());
                            }

                            let key = format!("{user_id}:{x:0>10}");
                            let key = key.as_bytes();

                            db.insert(key, &val, false, args.clone());
                        }
                    }
                }

                let threads = (0..users)
                    .map(|idx| {
                        let args = args.clone();
                        let db = db.clone();
                        let user_id = format!("user{idx:0>2}");

                        std::thread::spawn(move || {
                            let mut rng = rand::thread_rng();

                            let zipf = ZipfDistribution::new((args.items - 1) as usize, 0.99).unwrap();

                            loop {
                                let x = zipf.sample(&mut rng);
                                let key = format!("{user_id}:{x:0>10}");
                                let key = key.as_bytes();

                                let choice: f32 = rng.gen_range(0.0..1.0);

                                if choice > 0.5 {
                                    let mut val: Vec<u8> = Vec::with_capacity(args.value_size as usize);
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
                        let user_id = format!("user{idx:0>2}");

                        for x in 0..args.items {
                            let mut val: Vec<u8> = Vec::with_capacity(args.value_size as usize);
                            for _ in 0..args.value_size {
                                val.push(rng.gen::<u8>());
                            }

                            let key = format!("{user_id}:{x:0>10}");
                            let key = key.as_bytes();

                            db.insert(key, &val, false, args.clone());
                        }
                    }
                }

                let threads = (0..users)
                    .map(|idx| {
                        let args = args.clone();
                        let db = db.clone();
                        let user_id = format!("user{idx:0>2}");

                        std::thread::spawn(move || {
                            let mut rng = rand::thread_rng();

                            let zipf = ZipfDistribution::new((args.items - 1) as usize, 0.99).unwrap();

                            loop {
                                let x = zipf.sample(&mut rng);
                                let key = format!("{user_id}:{x:0>10}");
                                let key = key.as_bytes();

                                let choice: f32 = rng.gen_range(0.0..1.0);

                                if choice > 0.95 {
                                    let mut val: Vec<u8> = Vec::with_capacity(args.value_size as usize);
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

                    let mut val: Vec<u8> = Vec::with_capacity(args.value_size as usize);
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
                        let user_id = format!("user{idx:0>2}");

                        for x in 0..args.items {
                            let mut val: Vec<u8> = Vec::with_capacity(args.value_size as usize);
                            for _ in 0..args.value_size {
                                val.push(rng.gen::<u8>());
                            }

                            let key = format!("{user_id}:{x:0>10}");
                            let key = key.as_bytes();

                            db.insert(key, &val, false, args.clone());
                        }
                    }
                }

                let threads = (0..users)
                    .map(|idx| {
                        let args = args.clone();
                        let db = db.clone();
                        let user_id = format!("user{idx:0>2}");

                        std::thread::spawn(move || {
                            let mut rng = rand::thread_rng();
                            let mut records = args.items;

                            loop {
                                let choice: f32 = rng.gen_range(0.0..1.0);

                                if choice > 0.95 {
                                    let mut val: Vec<u8> = Vec::with_capacity(args.value_size as usize);
                                    for _ in 0..args.value_size {
                                        val.push(rng.gen::<u8>());
                                    }

                                    let key = format!("{user_id}:{records:0>10}");
                                    let key = key.as_bytes();

                                    db.insert(key, &val, args.fsync, args.clone());
                                    records += 1;
                                } else {
                                    let key = format!("{user_id}:{:0>10}", records - 1);
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
                        let user_id = format!("user{idx:0>2}");

                        for x in 0..args.items {
                            let mut val: Vec<u8> = Vec::with_capacity(args.value_size as usize);
                            for _ in 0..args.value_size {
                                val.push(rng.gen::<u8>());
                            }

                            let key = format!("{user_id}:{x:0>10}");
                            let key = key.as_bytes();

                            db.insert(key, &val, false, args.clone());
                        }
                    }
                }

                let threads = (0..users)
                    .map(|idx| {
                        let args = args.clone();
                        let db = db.clone();
                        let user_id = format!("user{idx:0>2}");

                        std::thread::spawn(move || {
                            let mut rng = rand::thread_rng();
                            let mut records = args.items;

                            loop {
                                let choice: f32 = rng.gen_range(0.0..1.0);

                                if choice < 0.95 {
                                    let mut val: Vec<u8> = Vec::with_capacity(args.value_size as usize);
                                    for _ in 0..args.value_size {
                                        val.push(rng.gen::<u8>());
                                    }

                                    let key = format!("{user_id}:{records:0>10}");
                                    let key = key.as_bytes();

                                    db.insert(key, &val, args.fsync, args.clone());
                                    records += 1;
                                } else {
                                    let key = format!("{user_id}:{:0>10}", records - 1);
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
                        let user_id = format!("user{idx:0>2}");

                        for x in 0..args.items {
                            let mut val: Vec<u8> = Vec::with_capacity(args.value_size as usize);
                            for _ in 0..args.value_size {
                                val.push(rng.gen::<u8>());
                            }

                            let key = format!("{user_id:0>2}:{x:0>10}");
                            let key = key.as_bytes();

                            db.insert(key, &val, false, args.clone());
                        }
                    }
                }

                let threads = (0..users)
                    .map(|idx| {
                        let args = args.clone();
                        let db = db.clone();
                        let user_id = format!("user{idx:0>2}");

                        std::thread::spawn(move || {
                            let mut rng = rand::thread_rng();
                            let mut records = args.items;

                            loop {
                                let choice: f32 = rng.gen_range(0.0..1.0);

                                if choice > 0.95 {
                                    let mut val: Vec<u8> = Vec::with_capacity(args.value_size as usize);
                                    for _ in 0..args.value_size {
                                        val.push(rng.gen::<u8>());
                                    }

                                    let key = format!("{user_id}:{records:0>10}");
                                    let key = key.as_bytes();

                                    db.insert(key, &val, args.fsync, args.clone());
                                    records += 1;
                                } else {
                                    let zipf =
                                        ZipfDistribution::new((records - 1) as usize, 0.99).unwrap();
                                    let x = zipf.sample(&mut rng);

                                    let key = format!("{user_id}:{x:0>10}");
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
                        let user_id = format!("user{idx:0>2}");

                        for x in 0..args.items {
                            let mut val: Vec<u8> = Vec::with_capacity(args.value_size as usize);
                            for _ in 0..args.value_size {
                                val.push(rng.gen::<u8>());
                            }

                            let key = format!("{user_id}:{x:0>10}");
                            let key = key.as_bytes();

                            db.insert(key, &val, false, args.clone());
                        }
                    }
                }

                let threads = (0..users)
                    .map(|idx| {
                        let args = args.clone();
                        let db = db.clone();
                        let user_id = format!("user{idx:0>2}");

                        std::thread::spawn(move || {
                            let mut rng = rand::thread_rng();
                            let mut records = args.items;

                            loop {
                                let choice: f32 = rng.gen_range(0.0..1.0);

                                if choice < 0.95 {
                                    let mut val: Vec<u8> = Vec::with_capacity(args.value_size as usize);
                                    for _ in 0..args.value_size {
                                        val.push(rng.gen::<u8>());
                                    }

                                    let key = format!("{user_id}:{records:0>10}");
                                    let key = key.as_bytes();

                                    db.insert(key, &val, args.fsync, args.clone());
                                    records += 1;
                                } else {
                                    let zipf =
                                        ZipfDistribution::new((records - 1) as usize, 0.99).unwrap();
                                    let x = zipf.sample(&mut rng);

                                    let key = format!("{user_id}:{x:0>10}");
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

            Workload::TaskI => {
                let mut base = 0u64;
                let mut rng = rand::thread_rng();

                loop {
                    for x in base..(base + 100_000) {
                        let key = x.to_be_bytes();

                        let mut val: Vec<u8> = Vec::with_capacity(args.value_size as usize);
                        for _ in 0..args.value_size {
                            val.push(rng.gen::<u8>());
                        }

                        db.insert(&key, &val, args.fsync, args.clone());
                    }

                    for x in base..(base + 50_000) {
                        let key = x.to_be_bytes();
                        db.remove(&key, args.fsync);
                    }

                    base += 100_000;
                }
            } */
    }
}
