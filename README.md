# rust-storage-bench

Benchmarking Rust storage engines:

- lsm-tree (https://github.com/marvin-j97/lsm-tree)
- sled (https://sled.rs)
- persy (https://persy.rs)
- jammdb (https://github.com/pjtatlow/jammdb)
- redb (https://www.redb.org)

![Example result](/img.png)

## Example usage

```
cargo build -r
alias bencher='cargo run --bin daemon -r --'

bencher --out task_a_lsmt_lcs.jsonl --mi
rmSync(".data", {
  recursive: true,
-items 1000 --cache-size 1000000
```

## Run many benchmarks

```
node tasks.mjs <...filter> 
```
