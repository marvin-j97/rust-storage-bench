cargo run -r -- run --backend "fjall" --data-dir ".data" --out "tmp_randomrw_fjall.jsonl" --display-name "fjall 2.6.3" --workload "random" --seconds 300 --granularity-ms 500 --cache-size 500000000 --item-count 10000000 --value-size 100
cargo run -r -- run --backend "heed" --data-dir ".data" --out "tmp_randomrw_heed.jsonl" --display-name "heed 0.20.5" --workload "random" --seconds 300 --granularity-ms 500 --cache-size 500000000 --item-count 10000000 --value-size 100
cargo run -r -- report "tmp_*.jsonl"
open out.html

