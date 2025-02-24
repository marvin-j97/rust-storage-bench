#!/bin/nu

let prefix = "monowrite";
let data_dir = ".data";
let seconds = 5 * 60;
let cache_mib = 100 * 1_024 * 1_024;
let value_size = 16;

alias bench = cargo run --

for db in ["fjall", "local-fjall", "rocksdb", "sled"] {
    let out = $"($prefix)_($db).jsonl";
    print $out;
    RUST_LOG=warn bench run --seconds $seconds --out $out --workload monotonic-write --value-size $value_size --backend $db --data-dir $data_dir --item-count 1 --cache-size $cache_mib
    sleep 500ms
}

# Generate report for the workload
let report_file = $"report_($prefix)($value_size).html";
print $report_file;

bench report --out $report_file ($"($prefix)_*.jsonl" | into glob);
google-chrome $report_file
