#!/bin/nu

#
# CONFIG
#

let prefix = "ycsb"
let data_dir = ".data"
let seconds = 1 * 60
let cache_mib = 8
let value_size = 128
let db_size = 10_000_000

#
# BENCH
#

alias bench = cargo run -r --features snmalloc,metrics --

let cache = $cache_mib * 1_024 * 1_024

let ks = $db_size / 1000;

# ycsb task list
for task in [
    "a",
    # "b",
    # "c"
] {
    let prefix = [$prefix, $task, (($ks | into string) + "K")] | str join "_";

    for db in [
        "fjall3", "redb", "canopydb",
    ] {
        let out = $prefix + "_" + $db + ".jsonl";
        print $out;
        RUST_LOG=error bench run --auto-granularity 50 --lsm-use-partitioned-meta --backend $db --cache-size $cache --data-dir $data_dir --seconds $seconds --sync --out $out ycsb --type $task --value-size $value_size --item-count $db_size --corpus json
        sleep 100ms
    }

    # Generate report for the workload
    let report_file = ("report_" + $prefix + ".html")
    print $report_file;

    bench report --out $report_file (($prefix + "_*.jsonl") | into glob)
    google-chrome-stable $report_file
}
