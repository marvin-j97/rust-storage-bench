use super::start_killer;
// use crate::db::GenericDatabase;
use crate::{args::CommonRunOptions, db::DatabaseWrapper};
use clap::Parser;
use rand::seq::SliceRandom;
use rand::Rng;
use rand::RngCore;
use serde::Serialize;
use std::hash::Hasher;
use std::sync::atomic::{AtomicIsize, Ordering};
use std::sync::Arc;

// #[rustfmt::skip]
// const TOP_LEVEL_DOMAINS: &[&str] =& [
//     "en",
//     "es",
//     "cn",
//     "ru",
//     "de",
//     "is",
//     "it",
//     "no",
//     "se",
//     "dk",
//     "pt",
// ];

#[derive(Parser, Clone, Debug, Serialize)]
pub struct Options {
    #[arg(long, default_value_t = 0)]
    data_max_size_bytes: u64,
}

pub fn run(
    args: &CommonRunOptions,
    opts: &Options,
    db: &DatabaseWrapper,
    finish_signal: Arc<AtomicIsize>,
) {
    // let fsync = args.fsync;
    // let item_count = args.item_count as u64;

    // let written_count = Arc::new(AtomicU64::new(item_count));
    // let mut buf = vec![0; args.value_size as usize];

    // if item_count > 0 {
    //     log::debug!("Pre-writing {item_count} items");
    //     let mut rng = rand::thread_rng();

    //     let iter = (0..(item_count as u128)).map(|x| {
    //         rng.fill_bytes(&mut buf);
    //         (x.to_be_bytes().to_vec(), buf.to_vec())
    //     });

    //     db.ingest(iter);
    // }

    std::thread::spawn({
        log::debug!("Starting writer");
        let db = db.clone();
        // let written_count = written_count.clone();

        move || {
            let mut rng = rand::thread_rng();

            let mut tld_buf = [0u8; 3];
            let mut subdomain_buf = [0u8; 5];
            let mut domain_buf = [0u8; 10];
            let mut pathname_buf = [0u8; 64];
            let mut anchor_buf = [0u8; 32];

            for x in 0.. {
                rng.fill_bytes(&mut tld_buf);
                rng.fill_bytes(&mut subdomain_buf);
                rng.fill_bytes(&mut domain_buf);
                rng.fill_bytes(&mut pathname_buf);

                let tld = &tld_buf;

                db.insert(
                    &format_bytes::format_bytes!(
                        b"{}.{}.{}/{}\0language\0\0\0",
                        tld,
                        subdomain_buf,
                        domain_buf,
                        pathname_buf,
                    ),
                    tld,
                    false,
                    true,
                );

                let mut hasher = std::hash::DefaultHasher::default();
                hasher.write_u64(x);
                let checksum = hasher.finish();

                db.insert(
                    &format_bytes::format_bytes!(
                        b"{}.{}.{}/{}\0checksum\0\0{}",
                        tld,
                        subdomain_buf,
                        domain_buf,
                        pathname_buf,
                        x.to_be_bytes(),
                    ),
                    &checksum.to_be_bytes(),
                    false,
                    true,
                );

                let anchor_count = rng.gen_range(10..50);

                for _ in 0..anchor_count {
                    rng.fill_bytes(&mut anchor_buf);

                    // TODO: abstract wide column key building into function
                    let anchor_key = format_bytes::format_bytes!(
                        b"{}.{}.{}/{}\0anchor\0{}\0\0",
                        tld,
                        domain_buf,
                        subdomain_buf,
                        pathname_buf,
                        anchor_buf,
                    );

                    rng.fill_bytes(&mut anchor_buf);
                    let anchor_text = String::from_utf8_lossy(&anchor_buf);

                    db.insert(&anchor_key, anchor_text.as_bytes(), false, true);
                }
            }
        }
    });

    if opts.data_max_size_bytes > 0 {
        std::thread::spawn({
            let db = db.clone();
            let finish_signal = finish_signal.clone();
            let data_max_size_bytes = opts.data_max_size_bytes;

            move || loop {
                std::thread::sleep(std::time::Duration::from_secs(10));

                let written_bytes = db.written_bytes.load(Ordering::Relaxed);

                if written_bytes >= data_max_size_bytes {
                    finish_signal.store(0, Ordering::Relaxed);
                }

                log::debug!("Written {written_bytes}/{data_max_size_bytes}B");
            }
        });
    } else {
        start_killer(args.seconds, finish_signal);
    }
}
