# Implementation status

Last updated: 2026-10-21

The repository has a working offline ingestion path and a scheduled, archive-first Polar sync path. Parsed provider records and normalized UTC measurements are persisted in redb. Temporary JSON files are not part of the storage contract. A compressed copy of each source response is retained for verification and crash recovery. No Polar credentials or live API requests have been used.

## Implemented

- One Rust package with library and CLI, Rust 2024, MSRV 1.90, and locked dependencies. The HTTP client selects RustCrypto for TLS and gzip uses the Rust backend.
- redb uses an explicit 8 MiB default cache (`RUST_POLAR_DB_CACHE_BYTES`, validated from 1–256 MiB), immediate durability, schema version 3, bounded batches, a v1-to-v2 archive-status migration, and v2-to-v3 parent-version tables.
- Provider records preserve decoded resource fields and unknown values. Normalized measurements use stable IDs and idempotent corrections. Only UTC-resolved measurements enter the time-series index; unresolved local-time data remains available in its parsed provider record.
- Corrected exercise, Activity, Sleep, Nightly Recharge, and SleepWise replay stage normalized child measurements in bounded batches and atomically activate complete parent generations. Queries continue to see the previous complete version until activation; obsolete versions are reclaimed in bounded chunks.
- Source response bodies stream to content-addressed gzip+BLAKE3 files after checking a configurable free-space reserve (512 MiB by default). Sidecars retain per-fetch provenance and allow startup recovery; archive replay verifies and imports them in bounded batches.
- Replay supports profile, physical info, exercise, activity, continuous HR, cardio load, sleep, Nightly Recharge, SleepWise alertness, and circadian bedtime records. Fixture tests verify parsed records survive CLI restarts and replay does not duplicate them.
- OAuth authorization-code flow validates loopback redirect and state, consumes a code once, registers an AccessLink user, and stores credentials with private Unix permissions. No live OAuth request has been made.
- The resident service owns one open database handle, provides a bounded read-only Unix-socket API, and runs an in-process scheduler every 30 minutes by default. Its interval is configurable and validated to 30 seconds–30 days. The sync planner covers documented backfill, recent polling windows, and explicit inclusive date ranges, archives pages before import, validates pagination origin, retries timeouts/429/5xx, and records durable receipt status/checkpoints. HTTP 401 and 403 produce distinct states. One-shot and scheduled sync share a lock.
- Consistent logical backup/restore uses a redb read snapshot, bounded table-copy transactions, archive verification, atomic directory publication, and a manifest. Credentials are excluded. Backup, restore, and replay are available in the CLI; query/export/replay/backup/sync use local IPC while the daemon owns redb.
- CLI commands: `init`, `status`, `auth polar`, `serve`, one-shot `sync polar [--from --to]`, bounded record `query`, bounded metric `export` (JSONL/CSV), `backup`, `restore`, `archive verify`, `archive diagnose`, `archive cleanup`, and `archive replay`.
- Exercise sync archives FIT and TCX for each exercise and GPX only when a route is reported. Binary receipts retain their exercise id and parsed index records expose the archive reference; 404 exports are recorded as no-data.
- The local API includes health/sources/sync status, metric catalog, bounded record and series queries, daily records, paginated JSONL/CSV measurement export, bounded-memory streaming of archived FIT/TCX/GPX by exercise id and format, and chart downsampling that returns actual first/min/max/last source points. Downsampling is limited to 250 buckets, one million scanned rows, and a 5 MiB response; original series values are unchanged.
- Private-socket admin routes support backup, archive replay/verify/diagnose/cleanup, and a sync trigger serialized against scheduled cycles. `status` and archive maintenance CLI commands use the daemon when it owns redb. API and CLI schemas/examples are in `API.md`.
- Local loopback HTTP and Unix-socket tests exercise the production response archiver with chunked bodies, response headers, interrupted bodies, and cancellation, plus archive-admin verification, diagnosis, cleanup, validation, and sync-lock conflict behavior. Archive fault injection covers ENOSPC during compressed writes, file fsync, object rename, and receipt rename. The production connector remains HTTPS-only with certificate validation.
- The resident scheduler performs a bounded stale archive cleanup after its first scheduled sync cycle (30 minutes by default) and then at most daily. It removes only orphan archive objects and `.part` files older than seven days; cleanup uses a preview fingerprint before deletion.
- Nightscout has no network connector yet, but synthetic integration tests verify glucose, treatments, device status, and profile documents, including source isolation, range queries, pagination, and JSON/CSV export through the shared data model.
- Context-free time-series range queries seek by time within known contexts and retain a fallback scan for unknown contexts. A cross-context pagination test protects ordering and cursor behavior.
- The reproducible benchmark generator is documented in `BENCHMARK.md` and reports bounded-batch writes, late points, reimports, corrections, queries, database/archive sizes, and synthetic provider-record counts.
- Raspberry Pi service and SSD restart notes are in `RASPBERRY_PI.md`.
- A hardened systemd unit and a root-run morning diagnostics bundle script are in `deploy/systemd/rust-polar.service` and `scripts/pi-diagnostics.sh`. They are not yet exercised on the target Pi; the first scheduled sync begins one full interval after service start, so use one manual sync as the pre-bed smoke test.
- A secret-free service environment template documents the current data-directory, socket, scheduler, redb-cache, and archive free-space settings.

## Remaining work

- Reconcile AccessLink endpoint availability, query limits, date-history bounds, and response variants against current official docs. Local notes disagree with published Swagger for several non-transactional endpoints. Browser use and live Polar/OAuth requests remain outside the current authorization, so real Polar Loop capability is unverified.
- Finish the config surface for shared HTTP/decoder/writer byte budgets, API concurrency, batch/page limits, and the 4 MiB maximum decoded object; those limits are still partly compile-time constants. The current scheduler deliberately fetches serially, and redb transactions are serialized, but there is no dedicated writer thread/queue matching the full SPEC contract.
- Complete endpoint-specific detail/fallback requests where the official contract supports them, and confirm exact exercise sample-code mappings. Provider-local timestamps without a reliable offset remain in parsed records and are intentionally excluded from UTC series.
- Add crash/replay fault tests at redb receipt registration, batch commit, checkpoint advancement, and version activation boundaries. Archive file-write/fsync/rename failures and exercise-export missing/corrupt/disconnect cases are now covered.
- Confirm shutdown/restart behavior on Linux. The daemon/CLI IPC test now proves status and archive maintenance commands use the live daemon while it owns redb.
- Continue optimizing and repeat the release benchmark. Across retained five-year runs, range-query p95 is 119–139 ms, above the 100 ms target; the latest full run's RSS measurement was blocked by sandbox `sysctl` access, and the exact hardware model was not captured.
- Run Linux ARM64 build and actual Raspberry Pi restart, memory, throughput, and SSD tests. These require the target host; they had not been exercised in this macOS environment as of this status note.

## Validation

Current validation on 2026-10-21: `cargo fmt --all -- --check`, `cargo clippy --offline --all-targets -- -D warnings`, and `cargo test --offline --all-targets` pass (119 library tests and 6 integration tests); the binary has 2 additional configuration-parser tests. The complete integration test run required permission for local Unix/loopback test sockets. Release benchmark data and limits are in `BENCHMARK.md`. No live Polar request, real Loop account check, Linux ARM64 build, or Raspberry Pi run has been performed.
