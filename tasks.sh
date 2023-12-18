cargo build -r
rm -rf .data
rm -rf *.jsonl

node tasks.mjs
