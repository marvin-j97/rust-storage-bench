#!/bin/nu

#
# CONFIG
#

let prefix = "ycsb"
let data_dir = ".data"
let seconds = 1 * 60
let cache_mib = 1000 * 1_024 * 1_024


#
# BENCH
#

alias bench = cargo run -r --

for db_size in [100000, 1000000, 10000000] {
    let ks = $db_size / 1000;

    for task in ["ycsb-a","ycsb-b","ycsb-c"] {
        let prefix = [$prefix, $task, (($ks | into string) + "K")] | str join "_";

        for db in [
            "local-fjall",
            "sled",  "redb",
            "rocksdb", "heed",
        ] {
            let out = $prefix + "_" + $db + ".jsonl";
            print $out;
            RUST_LOG=error bench run --fsync --seconds $seconds --out $out --workload $task --value-size 256 --backend $db --data-dir $data_dir --item-count $db_size --cache-size $cache_mib
            sleep 1sec
        }
    
        # Generate report for the workload
        let report_file = ("report_" + $prefix + ".html")
        print $report_file;

        bench report --out $report_file (($prefix + "_*.jsonl") | into glob)
        google-chrome $report_file
    }
}





# alias bench="cargo run -r --"

# prefix="dc"
# # data_dir="/home/marvin/Desktop/.data"
# data_dir=".data"
# secs=240

# for workload in feed monotonic-write; do
#     for values in 1000000; do
#         for db in redb sled; do
#             if [[ "$db" == "redb" ]] || [[ "$db" == "sled" ]]; then
#                 RUST_LOG=error bench run --display-name="" --seconds ${secs} --out ${prefix}_${workload}_${db}.jsonl --workload ${workload} --value-size 256 --backend ${db} --data-dir=${data_dir} --item-count ${values}
#             else
#                 RUST_LOG=error bench run --display-name="" --seconds ${secs} --out ${prefix}_${workload}_${db}_LCS.jsonl --workload ${workload} --value-size 256 --backend ${db} --data-dir=${data_dir} --item-count ${values}
#                 RUST_LOG=error bench run --display-name="" --seconds ${secs} --out ${prefix}_${workload}_${db}_STCS.jsonl --lsm-compaction tiered --workload ${workload} --value-size 256 --backend ${db} --data-dir=${data_dir} --item-count ${values}
#             fi
#             sleep 3
#         done
#     done

#     bench report --out report_${prefix}_${workload}.html ${prefix}_${workload}_*.jsonl
#     open ${prefix}_report_${values}.html
# done

# let prefix = "dc"
# let data_dir = ".data"
# let secs = 300

# for workload in ["monotonic-write"] {
#     for values in [1000000] {
#         for db in ["local-fjall", "redb" "sled"] {
#             if $db == "redb" or $db == "sled" {
#                 let out_file = ($prefix + "_" + $workload + "_" + $db + ".jsonl")
#                 bench run --seconds ($secs) --out ($out_file) --workload ($workload) --value-size 256 --backend ($db) --data-dir=($data_dir) --item-count ($values)
#             } else {
#                 let out_file_lcs = ($prefix + "_" + $workload + "_" + $db + "_LCS.jsonl")
#                 let out_file_stcs = ($prefix + "_" + $workload + "_" + $db + "_STCS.jsonl")
#                 #bench run --display-name ($db + " 2.4.0 LCS") --seconds ($secs) --out ($out_file_lcs) --workload ($workload) --value-size 256 --backend ($db) --data-dir=($data_dir) --item-count ($values)
#                 bench run --display-name ($db + " 2.4.0 STCS") --seconds ($secs) --out ($out_file_stcs) --lsm-compaction tiered --workload ($workload) --value-size 256 --backend ($db) --data-dir=($data_dir) --item-count ($values)
#             }

#             sleep 3sec
#         }
#     }

#     # Generate report for the workload
#     let report_file = ("report_" + $prefix + "_" + $workload + ".html")
#     bench report --out ($report_file) ($prefix + "_" + $workload + "_*.jsonl")
#     open $report_file
# }
