use super::start_killer;
use crate::args::CommonRunOptions;
use crate::db::DatabaseWrapper;
use clap::Parser;
use fake::faker::boolean::en::*;
use fake::faker::lorem::en::*;
use fake::faker::name::en::*;
use fake::uuid::UUIDv4;
use fake::{Dummy, Fake, Faker};
use rand::prelude::Distribution;
use rand::{Rng, RngCore};
use serde::{Deserialize, Serialize};
use std::sync::atomic::AtomicIsize;
use std::sync::Arc;
use zipf::ZipfDistribution;

#[derive(Debug, Dummy, Deserialize, Serialize)]
pub struct UserProfile {
    #[dummy(faker = "Name()")]
    display_name: String,

    #[dummy(faker = "Paragraph(1..2)")]
    description: String,

    #[dummy(faker = "Name()")]
    handle: String,

    #[dummy(faker = "Boolean(50)")]
    is_premium: bool,

    #[dummy(faker = "0..1_000_000")]
    follower_count: usize,
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

#[derive(Parser, Clone, Debug, Serialize)]
pub struct Options {
    /// Value size in bytes
    #[arg(long, default_value_t = 50)]
    pub tweet_size: u32,

    #[arg(long, default_value_t = 100)]
    pub item_count: usize,

    #[arg(long, default_value_t = 1)]
    pub threads: usize,

    #[arg(long, default_value_t = 1.0)]
    pub zipf_exponent: f64,

    #[arg(long, default_value_t = 1_000_000)]
    pub users: usize,
}

pub fn run(
    common_args: &CommonRunOptions,
    opts: &Options,
    db: &DatabaseWrapper,
    finish_signal: Arc<AtomicIsize>,
) {
    log::debug!("Pre-writing items");

    let mut rng = crate::random::thread_rng();
    let mut buf = vec![0; opts.tweet_size as usize];

    let feed_limit = 10;

    let iter = (0..opts.users)
        .flat_map(|x| (0..=feed_limit).clone().map(move |y| (x, y)))
        .map(|(user_idx, post_idx)| {
            let user_id = format!("u{user_idx:0>7}");

            // Insert profile last to keep insertion order consistent
            if post_idx == feed_limit {
                let user_profile_key: String = format!("{user_id}#p");

                let profile: UserProfile = Faker.fake();
                let profile = rmp_serde::to_vec(&profile).unwrap();
                return (user_profile_key.as_bytes().to_vec(), profile);
            }

            // Insert post
            let post_id = scru128::new_string();
            let post_key = format!("{user_id}#f#{post_id}");

            rng.fill_bytes(&mut buf);

            (post_key.as_bytes().to_vec(), buf.clone())
        });

    db.ingest(iter);

    // assert_eq!(
    //     db.len(),
    //     initial_posts_per_user * VIRTUAL_USERS + VIRTUAL_USERS,
    // );

    let threads = (0..opts.threads)
        .map(|_thread_no| {
            let common_args = common_args.clone();
            let opts = opts.clone();
            let db = db.clone();
            let tweet_size = opts.tweet_size;
            let zipf_exp = opts.zipf_exponent;
            let user_count = opts.users;

            std::thread::spawn(move || {
                let mut rng = crate::random::thread_rng();
                let mut buf = vec![0; tweet_size as usize];
                let zipf = ZipfDistribution::new(user_count, zipf_exp).unwrap();

                for _loop_idx in 0.. {
                    let choice: f32 = rng.gen_range(0.0..1.0);

                    // Which user?
                    let idx = zipf.sample(&mut rng) - 1;
                    let user_id = format!("u{idx:0>7}");

                    if choice > 0.5 {
                        // Insert post
                        let post_id = scru128::new_string();
                        let post_key = format!("{user_id}#f#{post_id}");

                        rng.fill_bytes(&mut buf);

                        db.insert(post_key.as_bytes(), &buf, common_args.fsync, true);
                    } else {
                        // Get profile
                        let user_profile_key = format!("{user_id}#p");
                        db.get(user_profile_key.as_bytes()).unwrap();

                        // + latest initial_posts_per_user posts
                        let feed_prefix = format!("{user_id}#f#");

                        assert_eq!(
                            feed_limit,
                            db.prefix_len(feed_prefix.as_bytes(), true, feed_limit),
                            "{feed_prefix} failed",
                        );
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    start_killer(common_args.seconds, finish_signal);

    for t in threads {
        t.join().unwrap();
    }
}
