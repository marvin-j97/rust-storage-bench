#!/bin/nu

let prefix = "feed";
let data_dir = ".data";
let seconds = 10 * 60;
let cache_mib = 64;
let tweet_size_bytes = 50;

###

alias bench = cargo run -r --

let cache = $cache_mib * 1_024 * 1_024;

for db in ["fjall", "sled", "rocksdb"] {
    let out = $"($prefix)_($db).jsonl";
    print $out;
    RUST_LOG=info bench run --seconds $seconds --out $out --workload feed --value-size $tweet_size_bytes --backend $db --data-dir $data_dir --cache-size $cache
    sleep 500ms
}

# Generate report for the workload
let report_file = $"report_($prefix).html";
print $report_file;

bench report --out $report_file ($"($prefix)_*.jsonl" | into glob);
google-chrome $report_file
