# rust-storage-bench

Benchmarking Rust storage engines:

- fjall Δ ★ (https://github.com/fjall-rs/fjall)
- redb Ω ★ (https://www.redb.org)
- sled Ψ (https://sled.rs)

---

- Δ LSM based
- Ω B-tree based
- Ψ Hybrid (Bw-Tree, ...)
- ★ has reached 1.0

## Example usage

Build before:

```bash
sh build.sh
```

Then run benchmarks and create HTML report:

```bash
alias bench="cargo run -r --"
bench run --backend fjall --data-dir=.data --workload timeseries-write --out log.jsonl --minutes 1
bench run --backend sled --data-dir=.data --workload timeseries-write --out log2.jsonl --minutes 1
bench report --out report.html log.jsonl log2.jsonl
open report.html
```
