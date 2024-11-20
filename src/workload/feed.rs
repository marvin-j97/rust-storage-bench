use super::start_killer;
use crate::args::RunOptions;
use crate::db::DatabaseWrapper;
use fake::faker::boolean::en::*;
use fake::faker::lorem::en::*;
use fake::faker::name::en::*;
use fake::uuid::UUIDv4;
use fake::{Dummy, Fake, Faker};
use rand::prelude::Distribution;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use zipf::ZipfDistribution;

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

const VIRTUAL_USERS: i32 = 1_000;

pub fn run(args: &RunOptions, db: &DatabaseWrapper, finish_signal: Arc<AtomicBool>) {
    for idx in 0..VIRTUAL_USERS {
        let user_id = format!("u{idx:0>7}");
        let user_profile_key = format!("{user_id}#p");

        let profile: UserProfile = Faker.fake();
        let profile = rmp_serde::to_vec(&profile).unwrap();

        db.insert(user_profile_key.as_bytes(), &profile, false);

        for _ in 0..1_000 {
            // Insert post
            let post_id = scru128::new_string();
            let post_key = format!("{user_id}#f#{post_id}");

            let post: FeedPost = Faker.fake();
            let post = rmp_serde::to_vec(&post).unwrap();

            db.insert(post_key.as_bytes(), &post, false);
        }
    }

    let threads = (0..1)
        .map(|_thread_no| {
            let args = args.clone();
            let db = db.clone();

            std::thread::spawn(move || {
                let mut rng = rand::thread_rng();

                for _loop_idx in 0.. {
                    let choice: f32 = rng.gen_range(0.0..1.0);

                    if choice > 0.9 {
                        // Which user?
                        let zipf =
                            ZipfDistribution::new((VIRTUAL_USERS - 1) as usize, 1.0).unwrap();
                        let idx = zipf.sample(&mut rng);
                        let user_id = format!("u{idx:0>7}");

                        // Insert post
                        let post_id = scru128::new_string();
                        let post_key = format!("{user_id}#f#{post_id}");

                        let post: FeedPost = Faker.fake();
                        let post = rmp_serde::to_vec(&post).unwrap();

                        db.insert(post_key.as_bytes(), &post, args.fsync);
                    } else {
                        // Which user?
                        let zipf =
                            ZipfDistribution::new((VIRTUAL_USERS - 1) as usize, 1.0).unwrap();
                        let idx = zipf.sample(&mut rng);
                        let user_id = format!("u{idx:0>7}");

                        // Get profile
                        let user_profile_key = format!("{user_id}#p");
                        db.get(user_profile_key.as_bytes()).unwrap();

                        // // + latest 10 posts
                        // let feed_prefix = format!("{user_id}#f#");
                        // let limit = 10;

                        // assert_eq!(
                        //     limit,
                        //     db.prefix_len(feed_prefix.as_bytes(), true, limit),
                        //     "{feed_prefix} failed"
                        // );
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    start_killer(args.seconds, finish_signal);

    for t in threads {
        t.join().unwrap();
    }
}
