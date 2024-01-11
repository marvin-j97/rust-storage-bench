# rust-storage-bench

Benchmarking Rust storage engines:

- fjall (https://github.com/marvin-j97/fjall)
- jammdb (https://github.com/pjtatlow/jammdb)
- nebari (https://github.com/khonsulabs/nebari)
- persy (https://persy.rs)
- redb (https://www.redb.org)
- sled (https://sled.rs)

## Example usage

```
cargo build -r
alias bencher='cargo run --bin daemon -r --'

bencher --out task_e_fjall_lcs.jsonl --workload task-e --backend fjall --minutes 5 --key-size 8 --value-size 256 --items 1000 --cache-size 1000000
```

## Run many benchmarks

```
node tasks.mjs <...filter> 
```
