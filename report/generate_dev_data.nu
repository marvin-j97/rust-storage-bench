#!/bin/nu

cd ..

alias bench = cargo run -r --

let cache_size = 16_000_000
let item_count = 2_000_000

RUST_BACKTRACE=full RUST_LOG=warn bench run --sync --backend fjall-nightly --data-dir .data --cache-size $cache_size --seconds 60 --out report/log.jsonl read-write --write-random --value-size 100 --item-count $item_count
RUST_BACKTRACE=full RUST_LOG=warn bench run --sync --backend rocksdb --data-dir .data --cache-size $cache_size --seconds 60 --out report/log2.jsonl.gzip read-write --write-random --value-size 100 --item-count $item_count
RUST_BACKTRACE=full RUST_LOG=warn bench run --sync --backend redb --data-dir .data --cache-size $cache_size --seconds 60 --out report/log3.jsonl.gzip read-write --write-random --value-size 100 --item-count $item_count
RUST_BACKTRACE=full RUST_LOG=warn bench run --sync --backend canopydb --data-dir .data --cache-size $cache_size --seconds 60 --out report/log4.jsonl.gzip read-write --write-random --value-size 100 --item-count $item_count
