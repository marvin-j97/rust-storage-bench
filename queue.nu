#!/bin/nu

let prefix = "queue";
let data_dir = ".data";
let seconds = 3 * 60;
let cache_mib = 16 * 1_024 * 1_024;
let value_size = 128;

alias bench = cargo run -r --

for db in ["local-fjall", "fjall", "redb", "sled"] {
    let out = $"($prefix)_($db).jsonl";
    print $out;
    RUST_BACKTRACE=full RUST_LOG=warn bench run --seconds $seconds --out $out --workload queue --value-size $value_size --backend $db --data-dir $data_dir --item-count 100 --cache-size $cache_mib
    sleep 500ms
}

# Generate report for the workload
let report_file = $"report_($prefix)($value_size).html";
print $report_file;

bench report --out $report_file ($"($prefix)_*.jsonl" | into glob);
google-chrome $report_file
