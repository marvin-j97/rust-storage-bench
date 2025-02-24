#!/bin/nu

#
# CONFIG
#

let prefix = "temp"
let data_dir = ".data"
let seconds = 1 * 30
let cache_mib = 0.1
let value_size = 64

#
# BENCH
#

alias bench = cargo run -r --

let cache = $cache_mib * 1_024 * 1_024 | math floor

for db_size in [5_000_000] {
    let ks = $db_size / 1000;

    for task in ["ycsb-c"] {
        let prefix = [$prefix, $task, (($ks | into string) + "K")] | str join "_";

        for db in [
            "fjall", "local-fjall"
        ] {
            let out = $prefix + "_" + $db + ".jsonl";
            print $out;
            RUST_LOG=error bench run --random --seconds $seconds --out $out --workload $task --value-size $value_size --backend $db --data-dir $data_dir --item-count $db_size --cache-size $cache
            sleep 250ms
        }
    
        # Generate report for the workload
        let report_file = ("report_" + $prefix + ".html")
        print $report_file;

        bench report --out $report_file (($prefix + "_*.jsonl") | into glob)
        google-chrome $report_file
    }
}
