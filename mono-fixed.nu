#!/bin/nu

let prefix = "mono-fixed";
let data_dir = ".data";
let seconds = 5 * 60;
let cache_mib = 4_000;
let value_size = 16;
let item_count = 125_000_000

let cache = $cache_mib * 1_024

alias bench = cargo run -r --

for db in ["local-fjall", "fjall"] {
    let out = $"($prefix)_($db).jsonl";
    print $out;
    RUST_BACKTRACE=full RUST_LOG=warn bench run --warmup-cache --random --seconds $seconds --out $out --workload monotonic-fixed --value-size $value_size --backend $db --data-dir $data_dir --item-count $item_count --cache-size $cache
    sleep 500ms
}

# Generate report for the workload
let report_file = $"report_($prefix)($value_size).html";
print $report_file;

bench report --out $report_file ($"($prefix)_*.jsonl" | into glob);
google-chrome $report_file
