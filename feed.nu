#!/bin/nu

let prefix = "feed";
let data_dir = ".data";
let seconds = 15 * 60;
let cache_mib = 4_000;
let tweet_size_bytes = 100;
let threads = 1;

###

alias bench = cargo run -r --features mimalloc --

let cache = $cache_mib * 1_024 * 1_024;

for db in ["fjall3"] {
    let out = $"($prefix)_($db).jsonl";
    RUST_LOG=info bench run --auto-granularity 10000 --backend $db --data-dir $data_dir --cache-size $cache --lsm-block-size 16000 --seconds $seconds --out $out feed --tweet-size $tweet_size_bytes --threads $threads
    sleep 100ms
}

# Generate report for the workload
let report_file = $"report_($prefix).html";
print $report_file;

bench report --out $report_file ($"($prefix)_*.jsonl" | into glob);
google-chrome $report_file
