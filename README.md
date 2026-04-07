# rust-storage-bench

Benchmarking Rust storage engines:

- canopydb Ω (https://github.com/arthurprs/canopydb)
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
nu scripts/build.nu
```

Then run benchmarks and create HTML report:

<!-- TODO: redo this -->
<!-- ```bash
alias bench="cargo run -r --"
bench run --backend fjall --seconds 60 --value-size 100 --data-dir=.data --workload random --out stats.jsonl
bench run --backend redb --seconds 60 --value-size 100 --data-dir=.data --workload random --out stats2.jsonl
bench run --backend sled --seconds 60 --value-size 100 --data-dir=.data --workload random --out stats3.jsonl
bench run --backend canopydb --seconds 60 --value-size 100 --data-dir=.data --workload random --out stats4.jsonl
bench report --out report.html stats.jsonl stats2.jsonl stats3.jsonl stats4.jsonl
open report.html
``` -->

Run YCSB-like benchmarks (look in the `scripts/ycsb.nu` file for some configuration):

```bash
systemd-run --scope -p MemoryMax=2G nu scripts/ycsb.nu
```

## Testing other storage engines

Other non-Rust storage engines can be compiled in using:

```bash
# Beware, RocksDB compile times!!!
cargo build -r --features rocksdb,heed,sqlite
```

## Choosing memory allocator

By default, the system allocator is used.
You can choose to compile another memory allocator using:

```bash
cargo build -r --features jemalloc
cargo build -r --features mimalloc
cargo build -r --features tcmalloc
cargo build -r --features snmalloc
```
