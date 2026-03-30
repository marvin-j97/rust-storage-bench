#!/bin/nu

let prefix = "colstore";
let data_dir = ".data";
let seconds = 60 * 60;
let cache_mib = 16_000;
let value_size = 100;
let threads = 8;
let db_count = 500;

###

alias bench = cargo run -r --features mimalloc --

let cache = $cache_mib * 1_024 * 1_024;

for db in ["fjall3"] {
    let out = $"($prefix)_($db).jsonl";
    RUST_LOG=info bench run --auto-granularity 100 --backend $db --data-dir $data_dir --cache-size $cache --lsm-block-size 16000 --seconds $seconds --out $out column-store --value-size $value_size --readers $threads --database-count $db_count
    sleep 100ms
}

# Generate report for the workload
let report_file = $"report_($prefix).html";
print $report_file;

bench report --out $report_file ($"($prefix)_*.jsonl" | into glob);
google-chrome-stable $report_file
