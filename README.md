# rust-storage-bench

Benchmarking Rust storage engines:

- fjall Δ ★ (https://github.com/fjall-rs/fjall)
- persy Ω ★ (https://persy.rs)
- redb Ω ★ (https://www.redb.org)
- sled Ψ (https://sled.rs)

Non-Rust (bindings):

- rocksdb Δ (https://rocksdb.org/)
- heed Ω (https://github.com/meilisearch/heed)

---

- Δ LSM based
- Ω B-tree based
- Ψ Hybrid (Bw-Tree, ...)
- ★ has reached 1.0

## Example usage

```
cargo run -r -- --backend fjall --data-dir=.data --workload timeseries-write --out report/log.jsonl --minutes 3
```

TODO: create HTML report

## Run many benchmarks

```
node tasks.mjs
```
