#!/bin/nu

let prefix = "blob";
let data_dir = ".data";
let seconds = 10 * 60;
let cache_mib = 1000 * 1_024 * 1_024;

alias bench = cargo run -r --

for db in ["fjall", "redb", "sled"] {
    let out = $"($prefix)_($db).jsonl";
    print $out;
    bench run --fsync --seconds $seconds --out $out --workload random-write --value-size (32 * 1_024) --backend $db --data-dir $data_dir --item-count 10000 --cache-size $cache_mib
    sleep 1sec
}

# Generate report for the workload
let report_file = $"report_($prefix).html";
print $report_file;

bench report --out $report_file ($"($prefix)_*.jsonl" | into glob);
google-chrome $report_file
