# rust-storage-bench

Benchmarking Rust storage engines:

- fjall Δ ★ (https://github.com/fjall-rs/fjall)
- redb Ω ★ (https://www.redb.org)
- sled Ψ (https://sled.rs)
- canopydb Ω (https://github.com/arthurprs/canopydb)

---

- Δ LSM based
- Ω B-tree based
- Ψ Hybrid (Bw-Tree, ...)
- ★ has reached 1.0

## Example usage

Build before:

```bash
nu build.nu
```

Then run benchmarks and create HTML report:

```bash
alias bench="cargo run -r --"
bench run --backend fjall --seconds 60 --value-size 100 --data-dir=.data --workload random --out stats.jsonl
bench run --backend redb --seconds 60 --value-size 100 --data-dir=.data --workload random --out stats2.jsonl
bench run --backend sled --seconds 60 --value-size 100 --data-dir=.data --workload random --out stats3.jsonl
bench run --backend canopydb --seconds 60 --value-size 100 --data-dir=.data --workload random --out stats4.jsonl
bench report --out report.html stats.jsonl stats2.jsonl stats3.jsonl stats4.jsonl
open report.html
```

Run YCSB-like benchmarks:

```bash
systemd-run --scope -p MemoryLimit=2G nu ycsb.nu
```

## Testing other storage engines

Other non-Rust storage engines can be compiled in using:

```bash
cargo build -r --features rocksdb,heed,sqlite
```

## Choosing memory allocator

By default `jemalloc` is used.
You can choose to compile another memory allocator:

```bash
cargo build -r --no-default-features --features mimalloc
cargo build -r --no-default-features --features tcmalloc
```

or use the system allocator instead:

```bash
cargo build -r --no-default-features
```
