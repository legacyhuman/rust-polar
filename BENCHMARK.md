# Reproducible storage benchmark

`src/bin/benchmark.rs` generates a deterministic Rust-only workload and writes
one JSON report to stdout. The generator uses fixed seed `0x504f4c4152203236`,
keeps at most 256 measurements or 128 provider records in memory, and commits
each batch with the production redb durability setting. It refuses to reuse an
existing data directory unless `--reuse` is explicitly supplied.

## Dataset

The standard run uses 1,825 days (five 365-day years) for each synthetic user:

| Data | Points or records per user |
|---|---:|
| Glucose every 5 minutes | 525,600 |
| Heart rate every 5 minutes | 525,600 |
| Daily activity, sleep, and Nightly Recharge summaries | 5,475 |
| Sleep hypnogram at 30-second intervals over 8 hours per day | 1,752,000 |
| Exercise heart-rate samples (25 synthetic exercises × 360 samples) | 9,000 |
| **Total normalized measurements** | **2,817,675** |
| Parsed daily provider records | 5,475 |

Use `--users 10` for ten times the history and `--days 30` for a short smoke
run. Each user has independent stable IDs and deterministic values. The run
also performs a repeated stable-ID import and corrections that move existing
points to a new timestamp, creates 20 compressed archive responses, and
queries all glucose points in a 30-day window using 1,000-row keyset pages.

The archive workload is synthetic and exercises streaming compression,
content-addressed publication, receipt registration, and disk usage. Its
synthetic source response is not replayed as Polar data. Reimport throughput is
measured by sending the same normalized IDs through the production storage
writer again; it does not measure JSON decoding or HTTP transport.

## Commands

Run from the repository root. Keep each data directory on the target SSD and
start from a fresh path when comparing runs.

```sh
cargo build --release --offline --bin benchmark
target/release/benchmark --data-dir /mnt/ssd/rust-polar-bench-1x
target/release/benchmark --data-dir /mnt/ssd/rust-polar-bench-10x --users 10
target/release/benchmark --data-dir /tmp/rust-polar-bench-smoke --days 30
```

Capture process peak RSS outside the program so allocator and runtime memory
are included. On macOS:

```sh
/usr/bin/time -l target/release/benchmark --data-dir /Volumes/SSD/rust-polar-bench-1x
```

On Linux:

```sh
/usr/bin/time -v target/release/benchmark --data-dir /mnt/ssd/rust-polar-bench-1x
```

The JSON output reports OS, architecture, release/debug profile, seed,
configured history, row counts, durable insert throughput, record and archive
write times, repeated-import and correction times, p50/p95 latency for a full
30-day query paged at 1,000 rows, and final database/archive sizes. Peak RSS is
left `null` in the JSON and printed by the platform time utility; copy that
value next to the JSON output when recording a run. Save the complete output,
the command, storage device, filesystem, kernel/macOS version, and exact Rust
version together as the measurement record.

## Measurements and limits

Release measurements on 2026-10-21 used the five-year, one-user dataset above:
2,817,675 initial normalized measurements plus 1,825 late historical points.
The initial full run completed in 156.9 s, about 28,451 initial rows/s. Before
the context-index query optimization, full 30-day query p50/p95 was 1.30/7.41 s.
After that optimization, two repeated query-only runs on the existing dataset
reported p50 of 14.4–15.3 ms and p95 of 118.8–119.9 ms. Thus p50 is below the
SPEC's 100 ms p95 target, but p95 still exceeds it by about 19–20 ms. Run these
repeatable query measurements without rebuilding the dataset with:

```sh
/usr/bin/time -l target/release/benchmark --data-dir /path/to/existing-five-year-dataset --query-only --reuse
```

The query-only runs reported peak RSS of 17.9–19.1 MiB. Separate complete
30-day runs with 46,530 (1×) and 465,300 (10×) initial measurements reported
peak RSS of about 29.0 MiB and 24.2 MiB, respectively. These are individual
process measurements, not a monotonic scaling curve; peak RSS varies with
runtime and OS page-cache behavior, and the sample does not establish a memory
limit for Pi workloads. The five-year full run's exact OS build, CPU model,
Rust version, SSD/filesystem, and full JSON report were not retained with these
summary figures, so they should not be used as a hardware-to-hardware
comparison. Raspberry Pi measurements remain outstanding.

Historical smoke run on 2026-10-21, macOS arm64, debug profile, 30 days, one
synthetic user: 46,530 initial measurement writes in 8.04 s (5,786 rows/s), 30
late historical rows in 0.022 s, 30-day paginated query p50/p95 206/240 ms,
134,746,112 byte database file and 188,684 bytes of compressed archives. This
checks that the generator and report run; debug timings and the small dataset
are not performance claims. Peak RSS was not captured.

Repeat release run on 2026-10-21 used the same five-year one-user workload on
macOS 27.0.1 (Darwin 27.0.0, arm64), Rust 1.99.0, with redb Immediate
durability and the default 8 MiB cache. It completed in 160.7 s; initial
normalized throughput was 19,377 rows/s, query p50/p95 within the run was
18.8/138.9 ms, the final redb file was 8,589,938,688 bytes, and compressed
archives were 188,704 bytes. The 1000-row, 30-day query p95 remains above the
100 ms target. `/usr/bin/time -l` printed the benchmark JSON but exited after
`sysctl kern.clockrate` was denied by the sandbox, so peak RSS was not captured.
The generated 6.4 GiB benchmark directory was removed after the run to restore
local disk space. These figures replace neither the previous query-only
measurements nor Pi measurements; run-to-run variation is material.

The benchmark measures redb writes and local queries; it does not model Polar
HTTP latency, TLS buffers, full archive replay decoding, or concurrent
application clients.
