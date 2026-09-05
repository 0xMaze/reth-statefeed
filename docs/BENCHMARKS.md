# Performance baselines

These numbers are regression baselines, not production latency guarantees. End-to-end p99 must be
measured on the target Linux node, including Reth validation, publisher scheduling, UDS delivery,
and consumer recomputation.

## 2026-09-05: local quick run

- Host: Apple Mac Studio `Mac14,13`, Apple M2 Max, 12 logical CPUs
- OS: Darwin 24.5.0 arm64
- Build: Cargo `release` profile, Reth v2.5.2 fork commit `6d512bdb`, Criterion `--quick`
- CPU affinity: not set
- Command: `cargo bench --locked --bench hot_path -- --quick`

| Workload | watched keys | median-ish range |
| --- | ---: | ---: |
| untouched account | 1,000 | 5.14–5.19 ns |
| 4 changed slots, one address | 1,000 | 43.64–43.75 ns |
| 4 changed slots, four of 1,000 watched addresses | 1,000 | 83.50–84.26 ns |
| all changed | 1,000 | 15.52–15.89 µs |
| untouched account | 10,000 | 5.15–5.16 ns |
| 4 changed slots, one address | 10,000 | 43.52–43.73 ns |
| 4 changed slots, four of 10,000 watched addresses | 10,000 | 83.47–84.00 ns |
| all changed | 10,000 | 170.80–176.97 µs |
| extraction + bounded queue round trip, 4 changed slots | 1,000 | 152.66–153.36 ns |

The untouched and sparse paths are effectively independent of the total watch count. At both the
account and storage levels the extractor scans the smaller side and uses an immutable reverse index
for the other side. The distributed-address case verifies that the result is not an artifact of
placing every watched slot under one contract. The all-changed case remains linear and is the
relevant worst-case hot-path budget.

The suite was remeasured after the independent lifecycle and performance review. Despite adding the
enqueue reservation required for loss ordering, moving queue-depth sampling off the producer path
and improving sparse lookup reduced the same-thread extraction/queue/dequeue baseline from
168.91–169.27 ns to 152.66–153.36 ns (about 9%). This round trip is intentionally a microbenchmark:
it does not include cross-core ownership transfer, publisher park/wakeup, socket delivery, or a
production metrics recorder. Treat these local ranges as regression signals rather than production
latency guarantees.
