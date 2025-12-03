use super::start_killer;
use crate::corpus::Corpus;
use crate::{args::CommonRunOptions, db::DatabaseWrapper};
use clap::Parser;
use rand::seq::SliceRandom;
use rand::Rng;
use rand::RngCore;
use serde::Serialize;
use std::hash::Hasher;
use std::sync::atomic::AtomicIsize;
use std::sync::Arc;

#[rustfmt::skip]
const TOP_LEVEL_DOMAINS: &[&str] =& [
    "en",
    "es",
    "cn",
    "ru",
    "de",
    "is",
    "it",
    "no",
    "se",
    "dk",
    "pt",
    "pizza",
    "tech",
    "io",
    "xyz",
    "news",
    "eu",
    "gov",
    "uk",
    "org",
    "jp",
    "mil",
    "edu",
    "net",
];

#[derive(Parser, Clone, Debug, Serialize)]
pub struct Options {}

pub fn run(
    args: &CommonRunOptions,
    _opts: &Options,
    db: &DatabaseWrapper,
    finish_signal: Arc<AtomicIsize>,
) {
    let html_corpus = Corpus::Html;
    let mut html_buf = vec![0u8; 100_000];

    std::thread::spawn({
        log::debug!("Starting writer");
        let db = db.clone();

        move || {
            let mut rng = rand::thread_rng();

            let mut subdomain_buf = [0u8; 5];
            let mut domain_buf = [0u8; 10];
            let mut pathname_buf = [0u8; 64];
            let mut anchor_buf = [0u8; 32];

            for x in 0.. {
                let tld = TOP_LEVEL_DOMAINS.choose(&mut rng).unwrap().as_bytes();
                rng.fill_bytes(&mut subdomain_buf);
                rng.fill_bytes(&mut domain_buf);
                rng.fill_bytes(&mut pathname_buf);

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

                db.insert(
                    &format_bytes::format_bytes!(
                        b"{}.{}.{}/{}\0contents\0\0{}",
                        tld,
                        subdomain_buf,
                        domain_buf,
                        pathname_buf,
                        x.to_be_bytes(),
                    ),
                    {
                        html_corpus.fetch(&mut rng, &mut html_buf);
                        &html_buf
                    },
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

    start_killer(args.seconds, finish_signal);
}
