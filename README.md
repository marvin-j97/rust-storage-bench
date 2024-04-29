# rust-storage-bench

Benchmarking Rust storage engines:

- fjall Δ (https://github.com/fjall-rs/fjall)
- jammdb Ω (https://github.com/pjtatlow/jammdb)
- nebari Ω (https://github.com/khonsulabs/nebari)
- persy Ω (https://persy.rs)
- redb Ω (https://www.redb.org)
- sled Ψ (https://sled.rs)

---

- Δ LSM based
- Ω B-tree based
- Ψ Hybrid (Bw-Tree, ...)

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
