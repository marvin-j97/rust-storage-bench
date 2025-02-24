#!/bin/nu

let prefix = "randomwrite";
let data_dir = ".data";
let seconds = 10 * 60;
let cache_mib = 1_000 * 1_024 * 1_024;
let value_size = 96;

alias bench = cargo run -r --

for db in ["rocksdb", "heed", "fjall"] {
    let out = $"($prefix)_($db).jsonl";
    print $out;
    RUST_BACKTRACE=full RUST_LOG=warn bench run --seconds $seconds --out $out --workload random-write --value-size $value_size --backend $db --data-dir $data_dir --item-count 100 --cache-size $cache_mib
    sleep 500ms
}

# Generate report for the workload
let report_file = $"report_($prefix)($value_size).html";
print $report_file;

bench report --out $report_file ($"($prefix)_*.jsonl" | into glob);
google-chrome $report_file
