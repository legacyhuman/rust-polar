# rust-polar: implementation specification

Date: 2026-10-21. Status: implementation brief; source code has not been created yet.

## 1. Goal and scope

Build a local personal archive of health and fitness data in Rust. The first source is **Polar AccessLink API v3**, and the user's device is the new **screenless Polar Loop**. Collect the maximum data set that the API actually returns for the user. A field in Polar's general schema does not establish that the bracelet supports it.

One `rust-polar` binary provides the CLI, background synchronization, an embedded redb database, and a local API for other applications. Preserve all original API responses; expose typed data and range queries for analysis. Nightscout is the next source: prepare an extensible model now rather than building a second unfinished connector.

Mandatory constraints:

- The application code and database engine must be Rust. Do not use Python, Go, the JVM, Node, Open Health Sync, SQLite, DuckDB, RocksDB, or a separate database server.
- Do not use BLE, the mobile Polar SDK, raw sensor signals, private Polar Flow endpoints, or scraping.
- Original response archives mean cloud JSON/XML/files from AccessLink, not sensor signals.
- Do not build a GUI, medical recommendations, machine learning, cloud infrastructure, or a publicly accessible network server.
- macOS is the primary platform; the core must be portable to Linux. Windows is outside the first release.
- Do not clone or modify the remote repository, switch the shared `gh` account, push, or start another user-facing chat without a separate instruction from the user.

## 2. Fixed design decisions

1. `redb` is the only operational database; response archives are ordinary files stored alongside it.
2. One process owns the open database. Other applications access it through a Unix domain socket.
3. Use one dedicated database writer, bounded queues, and short read transactions.
4. Store the original response before normalization. Reprocessing must be idempotent.
5. Store measurement time, retrieval time, source, and measurement semantics separately.
6. Data completeness takes precedence over squeezing every metric into a small common schema. Preserve Polar-specific structures without changing their meaning.
7. Do not claim to have the fastest engine or promise a particular RSS without measurements. Optimize using reproducible results.

## 3. Dependency validation before main development

Start with a small technical milestone: compatible stable versions of redb, HTTP/TLS, gzip, serialization, and local transport. Pin the toolchain/MSRV and Cargo.lock.

Preferred dependencies: `redb`, `serde`, `serde_json`, `tokio`, `reqwest` with default features disabled, `rustls`, `flate2` with a Rust backend, `blake3`, `clap`, `tracing`, `time`, and a compact versioned binary codec such as `postcard`.

The pure-Rust requirement extends to the selected dependency backends. In particular, `rustls` alone **does not guarantee** that the cryptographic provider has no C/assembly: do not automatically treat `ring` and `aws-lc` as suitable. Validate a compatible RustCrypto provider or another maintained Rust-only TLS stack; do not implement TLS yourself or disable certificate validation. System calls and FFI to standard operating-system APIs are allowed.

Deliverables for this milestone: a successful minimal HTTPS request and redb transaction, the enabled feature list, `cargo tree`, and verification of the actual cryptographic provider and compressor. If a stable, reliable TLS option without native crypto is unavailable, document the specific incompatibility and request a decision only on that constraint; continue the independent work. Do not silently substitute a different choice.

Do not add speculative dependencies, an ORM, a plugin framework, a custom executor, or a distributed queue.

## 4. Code organization

Start with one Cargo package containing a library and a binary. Example modules:

```text
src/
  lib.rs, main.rs
  config.rs, error.rs
  model/       # types, units, time, keys, versions
  archive/     # persistence, manifests, verification, and replay
  storage/     # redb, tables, writer, migrations
  polar/       # OAuth, HTTP, endpoints, streaming decoders, normalization
  sync/        # scheduling, checkpoints, retries, source capabilities
  query/       # ranges, aggregates, pagination
  api/         # HTTP/1.1 over a Unix socket
  cli/         # commands and streaming exports
tests/fixtures/polar/
benches/ or a dedicated benchmark command
```

Agree on module contracts before implementing them in parallel. A single enormous `Provider` trait covering every possible operation is unnecessary: a source identifier, resource list, request plan, and archived-object decoder are sufficient.

## 5. Full Polar scope

The current official documentation is the source of truth, not a third-party library:

- https://www.polar.com/accesslink-api/
- https://www.polar.com/accesslink-api/swagger.yaml

The HTML documentation and Swagger may differ: record any discrepancies. Verify endpoints, query flags, dates, formats, and availability before implementation. Do not use the legacy transaction resources.

### 5.1 Authorization and account data

- Implement the OAuth authorization code flow, `state`, exact redirect URI matching, and user registration through `POST /v3/users`.
- Retrieve the profile through `GET /v3/users/{user-id}` and physical settings through `GET /v3/users/physical-info`.
- Preserve every returned field. Treat VO₂max, thresholds, and heart-rate settings as settings/profile data unless the API identifies them as measurements.
- Do not assume that a refresh token exists or assume a token lifetime: follow the actual response and official contract.
- Do not resubmit an OAuth code after an ambiguous result: Polar warns that reuse may revoke issued tokens.
- Do not revoke an existing token or delete the user during ordinary program shutdown.

### 5.2 Resources and required detail

| Resource | Requests and required preservation |
|---|---|
| Exercises | `/v3/exercises`, and `/{exerciseId}` when needed; enable `samples=true`, `zones=true`, `route=true`. Preserve summaries, time series of every returned type, recording rate, zones, routes, loads, energy-source proportions, device identifiers, and all additional fields |
| Exercise exports | `/v3/exercises/{exerciseId}/fit`, `/tcx`, `/gpx`; preserve available files. GPX applies when a route exists. Do not represent a missing export as an existing empty file |
| Daily activity | `/v3/users/activities` with `from`, `to`, `steps=true`, `activity_zones=true`, `inactivity_stamps=true`; use `/samples` or a date request only when needed to obtain missing detail |
| Continuous HR | `/v3/users/continuous-heart-rate` and/or `/{date}`; preserve all returned values and original times. Collect exercise heart rate separately |
| Sleep | `/v3/users/sleep`, `/{date}`, `/available` as needed; preserve all scores, durations, goals, hypnograms, unknown stages, and heart rate |
| Nightly Recharge | `/v3/users/nightly-recharge`, `/{date}`; preserve averages, statuses, ANS Charge, five-minute `hrv_samples` and `breathing_samples` |
| SleepWise | alertness and circadian-bedtime, including `/date` variants; preserve scores, classifications, intervals, validity, hourly data, and sleep windows |
| Cardio Load | `/v3/users/cardio-load` and documented date/date-range/history variants; preserve load, strain, tolerance, ratio, level, and status |

Do not plan to collect ECG, SpO₂, or temperature from Loop through AccessLink without confirmed official support or an actual response. Do not transfer other devices' capabilities to Loop. Do not continually request every Elixir resource when no data is expected.

Every unfamiliar field remains in the archive. The decoder must tolerate unknown enum codes and sample types without crashing or losing their original values. An unknown metric may remain in a document under its provider-specific name until a normalizer is added.

### 5.3 History, polling, and capabilities

- Exercises: a 30-day window based on upload time; normally only uploads made after registration with our client are available. Do not promise to import the full historical exercise record.
- Activity: documented history is 365 days, with a maximum request range of 28 days.
- Sleep and Nightly Recharge: lists cover the last 28 days. Do not treat date endpoints as unlimited history without verification.
- SleepWise: lists cover 28 days and ranges are at most 28 days; establish history depth separately.
- Verify Continuous HR and Cardio Load limits independently. Do not apply one limit to the entire API.
- Allow one request at a time per resource; initial global HTTP concurrency is 2. Split long history into documented windows.
- Poll every 30 minutes by default. Reread the last 7 days for resources subject to revision, and treat current-day totals as provisional. Reconcile the entire available short window weekly. Make intervals configurable; do not queue missed ticks after downtime.
- On the initial run, retrieve all available documented history rather than only the last day.
- Track `available`, `no_data`, `unsupported`, `needs_auth`, `transient_error`, and `unknown` separately. An empty day or a single 404 does not establish that a resource is unsupported.
- A `401` changes the source state to `needs_auth`. Diagnose a `403` according to the contract; do not interpret it as a missing metric. On `429`, honor Polar headers and Retry-After, respecting both rate-limit windows.
- Use bounded retries with backoff and jitter for transient errors; do not retry permanent 4xx errors indefinitely.
- Webhooks are not required for the first release: the local service has no public address. Start with reliable polling; allow later notifications that trigger an unscheduled poll.

## 6. Data model and precision

For each object, store: `schema_version`, `user_id`, `source`, `resource`, provider external ID, source device ID when available, event time/interval, fetched time, archive reference, revision/hash, and time quality/provenance.

A numeric record contains: `metric_id`, `value`, `unit`, `start`, `end`, `aggregation` (`instant`, `mean`, `sum`, `provider_score`), context (`daily`, `sleep`, `exercise`), and parent ID when available.

Rules:

- Store UTC as integer microseconds since the Unix epoch. Also preserve dates and local times. Do not discard explicitly supplied offsets in ISO timestamps. If the source has finer precision than microseconds, retain the exact original in the archive.
- Do not invent precise UTC timestamps for local times without an offset. Preserve the local date/time with status `unresolved`; resolve it only from verified context in the same record/session or an IANA timezone explicitly configured by the user. Mark inferred assumptions. DST tests are mandatory.
- Recharge HRV is RMSSD in ms, not SDNN. Breathing rate is breaths/min; heart rate is beats/min. Do not conflate them based on the string `bpm`.
- Do not present a five-minute average as an instantaneous measurement. Preserve an overnight average with Polar's stated calculation context rather than as a measurement at midnight.
- Zero, a missing field, `null`, an unknown enum, and no data are distinct states.
- Preserve unknown sleep stages. N1/N2 may be represented by a common light stage, but the original stage code is required.
- Preserve SleepWise and ANS Charge in their own scales; do not turn them into a generic health percentage.
- For glucose, support canonical mg/dL while preserving the original unit; document and test mmol/L conversion.
- Preserve the ID, direction/trend, source timestamp, quality, and modification/deletion status of future Nightscout records.
- AccessLink may not report deletions made in Polar Flow. A record's absence from a subsequent response does not prove deletion.

## 7. redb format and indexes

Version the database format and every binary value. Select the codec after a small prototype; do not use JSON as the primary format for dense numeric time series.

Core tables:

| Table | Purpose |
|---|---|
| `meta` | Schema version, database settings, migrations |
| `users`, `sources`, `metrics` | Small lookup catalogs with numeric IDs |
| `records` | Typed sessions, events, documents, and provider-specific structures |
| `series` | Measurements sorted by user, source, metric, context, time, and stable ID |
| `external_index` | Lookup and updates by provider/resource/external ID; child-record relationships |
| `daily` | Aggregates and separate original Polar daily results |
| `archives` | Hash, path, resource, time, processing status, and diagnostics |
| `sync_state` | Checkpoints, scheduling, attempts, and authorization state |

Prefer small tuple/integer keys. If a key is packed into bytes, its ordering and signed timestamp encoding must preserve chronological order; cover this with tests. Do not repeat long source and metric names in every point.

Do not build a universal index over every field. Primary queries are: one or more metrics over a time range, sessions over a range, an object by external ID, the current profile, and synchronization state. Add an index only for a concrete query.

Start with one record per measurement. Point blocks, delta encoding, and a separate analytical store may be added later only if benchmarks demonstrate a need. Do not complicate corrections and updates before measuring.

### 7.1 Database ownership and updates

- Use one dedicated writer thread. It receives byte-bounded batches and performs synchronous redb transactions without blocking the async executor.
- Initial batch limit: 512 KiB or 1000 records, whichever comes first. Use `Immediate` or equivalent reliable durability by default. Do not disable fsync for benchmarks.
- Do not hold a read transaction while sending data to a slow network client. Read a page, close the snapshot, send it, then read the next page.
- Use keyset pagination, not OFFSET. Define consistency semantics across pages during concurrent updates; do not promise a single snapshot for an entire export.
- Updating a workout/sleep record replaces that version's child series/stages, including points omitted from the new version. Store relationships explicitly.
- Process large responses across multiple transactions. Advance the source's `sync_state` checkpoint only after the resource/window is fully processed; retrying after a crash must not create duplicates.
- For large parent objects with child series, use versions: prepare the new version in batches, atomically make it visible, then remove the old version in batches. Queries see one completed version. Small objects may use one transaction.
- Recompute derived daily aggregates for affected days after changes rather than adding them again during replay. Keep provider daily totals separate from point sums; do not sum overlapping totals and detail.
- Migrations must be interruptible and repeatable, or use a new file with atomic replacement after verification. Do not change an existing format without a version.

## 8. Archive and recovery

Example layout: `config.toml`, `health.redb`, `archive/`, `tmp/`, `run/`. The directory is private to the current user. The data path is configurable.

Algorithm for one response:

1. Read the HTTP body in chunks, compute BLAKE3 over the original decoded body, and stream gzip compression through a Rust backend into a temporary file on the same filesystem.
2. Bound the queue between HTTP and the compressor by bytes; split large network chunks rather than measuring queue size only by message count.
3. Finish the encoder, flush/fsync the file, rename atomically, and fsync the directory when needed. Use a content-addressed name based on the checksum; do not duplicate identical objects. For binary FIT/GPX/TCX exports, checksum the file body and do not try to parse it as JSON.
4. Durably publish a small sidecar descriptor for each retrieval using temp + rename + fsync, then register the retrieval in redb as `pending`. The descriptor contains source/resource/parameters without secrets, HTTP status, fetched_at, sizes, and hash. The sidecar allows retrieval recovery after a crash between filesystem publication and database registration. One content hash may belong to multiple retrievals with different parameters: body deduplication must not erase request history or provenance.
5. Read gzip through a bounded BufReader, decode and normalize as a stream, and write in batches. On success, mark it `processed`, then advance the completed window's checkpoint. Binary exports only require archiving and an index with the parent exercise ID: the first release does not need to normalize again the points already obtained through JSON.

Do not retain the entire body as a `String`, `Vec<u8>`, or `serde_json::Value`. For arrays, use `DeserializeSeed`/Visitor or equivalent streaming reads, including nested samples. Do not write a custom JSON parser.

Allow a configurable maximum size for **one scalar/string/object** that the chosen codec cannot decode as a stream. The initial limit is 4 MiB. If exceeded, preserve the archive, record an error, leave the checkpoint unchanged, and do not silently skip data. A complete HTTP response may be much larger than RAM; configure its on-disk size limit separately.

After a failure:

- Incomplete `.part` files are not archives.
- A completed file without a database entry is detected by an orphan-file check and may be registered or removed through a separate explicit operation.
- `pending` and `failed` objects can be replayed safely.
- Test archive/database boundaries with fault injection.
- Disk exhaustion stops processing with diagnostics and without advancing the cursor.

Do not archive responses containing OAuth secrets. Do not include tokens or Authorization headers in manifests/logs. The archive contains sensitive user data; do not send it to third-party services.

## 9. Memory and performance

Initial `low-memory` profile:

| Parameter | Initial value |
|---|---:|
| redb cache | 8 MiB, set explicitly; the current redb default may be 1 GiB |
| Global HTTP/decoder/writer queues | Combined byte budget of 4 MiB |
| HTTP concurrency | 2 |
| Writer threads | 1 |
| CPU-heavy parsing/compression jobs | 1 at a time |
| Read page limit | 1000 records, plus a byte limit |
| Runtime | Current-thread Tokio with explicitly bounded blocking workers |

All values must be configurable and validated. Cache and queue settings **are not a hard RSS limit**: account for TLS, redb write buffers, HTTP, decoders, the allocator, and the OS filesystem cache. Archive size must not determine memory use. Add a `balanced` profile only after measurements.

Use streaming JSONL/CSV for large queries; return a bounded page for JSON. Charts may request bucketed first/min/max/last downsampling that preserves actual extrema. Do not modify original values. Cancel long reads when the client disconnects.

The benchmark suite uses synthetic data, a fixed seed, and a published generator:

- 5 years of glucose at five-minute intervals: 525600 points for 365-day years.
- 5 years of heart rate at five-minute intervals plus synthetic exercise points.
- 5 years of daily sleep/recharge/activity, with hypnograms at 30-second intervals.
- Updates to existing records, late points, repeated imports, and erroneous/incomplete responses.
- A data set enlarged 10-fold to test how memory use depends on history size.

Measure release builds: idle RSS, peak RSS during backfill/replay, records/s with durable commits, p50/p95 query latency, archive and database size, and cold/warm reads separately. Do not load the complete benchmark data set into the generator's memory.

Initial engineering targets, **not promised results**: idle RSS at most 32 MiB, peak at most 64 MiB in low-memory mode on standard fixtures; p95 for a 30-day single-metric query at most 100 ms with a warm cache on SSD. Do not hide overruns: provide causes, measurements, and a bounded remediation plan. The main requirement is bounded memory that does not grow in proportion to the full history.

## 10. Local API and CLI

Use HTTP/1.1 JSON over a Unix socket with socket file permissions `0600`. No public TCP listener by default. Remove a stale socket only after verifying that no live owner exists. A second `serve` must fail with a clear error.

Initial read-only endpoints:

```text
GET /v1/health
GET /v1/sources
GET /v1/sync/status
GET /v1/metrics
GET /v1/series?metric=...&source=...&from=...&to=...&limit=...&cursor=...
GET /v1/records?kind=...&source=...&from=...&to=...&limit=...&cursor=...
GET /v1/records/{id}
GET /v1/daily?from=...&to=...
GET /v1/export?metric=...&from=...&to=...&format=jsonl|csv
```

Document the JSON schema with examples, including units, provenance, quality/time resolution, and next_cursor. UTC ranges are half-open `[from,to)`. Local dates require an explicit timezone; unresolved times remain accessible through records rather than fabricated UTC series. Validate limits and ranges. TCP and network authentication are outside the first release.

CLI:

```text
rust-polar init
rust-polar auth polar
rust-polar serve
rust-polar sync polar [--from YYYY-MM-DD] [--to YYYY-MM-DD]
rust-polar status
rust-polar query ...
rust-polar export ...
rust-polar archive verify
rust-polar archive replay [--resource ...]
rust-polar backup DEST
```

`serve` is the only resident redb owner and scheduler, with a default sync interval of 30 minutes. The scheduler lives inside that process, not in a separate systemd timer or second writer process. When the process is active, query/status/sync/replay/backup commands use IPC; mutation commands may use separate local POST admin endpoints with the same socket permissions. Without an active process, commands may open the database exclusively for a one-shot operation. Do not try to open redb simultaneously from the CLI and daemon. Keep the read API read-only; document admin routes separately.

The process keeps redb open and serves future local applications through a Unix domain socket. This does not mean pinning the complete database file in physical RAM: the working set consists of the redb cache and OS page-cache pages, which the OS may reclaim under memory pressure. Do not `mlock` the entire database; the SSD remains the persistent store.

`auth` shows the URL to the user; the agent does not control the browser itself. A localhost callback is allowed only for OAuth with an explicitly registered redirect URI, on loopback, and with state validation. Read secrets from a hidden prompt or an explicitly configured file/environment. Do not include them in argv. The system Keychain is acceptable on macOS; a portable file requires `0600` and honest documentation that it is unencrypted. Do not claim protection from an encryption key stored alongside its ciphertext.

Backup: use a consistent snapshot through redb's mechanism or a writer-coordinated backup, plus a manifest of required archives. Do not copy an open database file with ordinary `cp` and call it a consistent backup. Verify restoration into a new directory.

## 11. Nightscout: next phase

Implement model/query/storage now so that the following can be added later:

- Glucose entries with interval/time, trend/direction, source, and ID.
- Treatments: insulin/carbs and additional original fields without losing their meaning.
- Device status and profiles as versioned documents.
- Changes and deletions, if the chosen Nightscout API permits reliable detection.

Do not tie the tables exclusively to Polar or use `polar_*` field names in the shared core. Provider-specific fields belong in adapters/documents. Determine the Nightscout API version and authentication when implementing the actual connector. The current definition of done includes a synthetic glucose data set and common queries against it, not access to the user's real Nightscout instance.

## 12. Milestones and verifiable results

### M0 — dependencies and contracts

Minimal build, Rust-only backend verification, key/value formats, time model, archive/storage/source interfaces, and a recorded plan. Database round-trip test. No real secrets.

### M1 — local vertical slice

Polar fixture → archive → streaming decoder → redb → CLI query/export. Replay does not create duplicates; data survives restart. Complete one resource end to end, then expand.

### M2 — Polar completeness

All resources from §5, profile and physical settings, all extra flags, exports, unknown values, and detailed sleep/recovery data. Coverage matrix: endpoint → fields → storage → query → fixture/test. Every cell has an implemented/archived-only/unavailable status and a reason.

### M3 — live synchronization

OAuth, registration, incremental sync/backfill, retries/rate limits, cursors, record corrections, and graceful shutdown. A fake HTTP server covers HTTP behavior; live requests require the user's Polar authorization. Missing credentials must not prevent completion of the offline/fake-server implementation.

### M4 — other applications

Unix-socket API, CLI client, pagination, exports, exclusive ownership, disconnect cancellation, and consistent backup/restore. No public TCP listener.

### M5 — measurement and completion

Synthetic Polar + glucose data set, benchmark report, low-memory tuning, fixes for identified failures, and operating instructions and limitations.

Do not declare the project complete if it implements only the happy path or half the resources. If any data remains archived-only, state and explain that explicitly; known resources in §5 require normalization wherever the measurement/session/document model applies.

## 13. Required checks

- `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test --all-targets`; adapt commands to the workspace if needed.
- Fixture golden tests: write expected values independently of the implementation. Check units, times, missing values, unknown enums, and every resource, not just record counts.
- Fake server: pagination/date windows, flags, chunked bodies, large responses, 204/400/401/403/404/429/5xx, Retry-After/rate-limit headers, interrupted bodies, and cancellation.
- Sleep across midnight/DST, local time without an offset, repeated time during the autumn clock change, and record correction that changes the start and reduces the number of points.
- Crash/replay at archive/registration/batch/checkpoint/version-activation boundaries, disk exhaustion, repeated imports, and corrupted gzip/checksums.
- Queries must not combine daily totals with intraday samples; repeated downloads must not double aggregates.
- Multiple simultaneous clients, bounded requests/queues, a slow client, a second process, and socket permissions.
- Backup/restore with logical-record comparison.
- Performance report with environment, seed, point count, configuration, durability, and reproduction commands.

## 14. Implementation agent and subagents

The user **explicitly authorizes and requests active use of subagents wherever this speeds up delivery or improves verification**. The primary agent owns integration and the complete definition of done. Context/model limits are a reason to split tasks, not omit requirements.

Recommended sequence:

1. The primary agent reads the specification, checks the workspace and its rules, creates a short M0–M5 backlog, and agrees on shared Rust types/interfaces.
2. In M0, delegate independent TLS/dependency and AccessLink-completeness checks; establish the model/contracts directly.
3. After M0, parallelize storage and archive work, assigning separate files to each agent. The primary agent assembles the vertical slice.
4. After M1, divide Polar into independent groups: activity/HR, sleep/recharge/SleepWise, and exercise/cardio/profile. Do not let every agent edit the shared `mod.rs`, Cargo.toml, and model simultaneously.
5. Delegate API/CLI and benchmark/fixtures after the storage/query contracts stabilize.
6. Before completion, assign a separate subagent to review data completeness, time handling, crash consistency, and memory. A reviewer must not review only their own code.

With a four-slot limit: the primary agent plus at most three simultaneously active subagents. Use available slots for independent tasks; do not keep agents idle or create an agent for every trivial change. Inherit the model by default; do not select other models without the user's instruction.

Every subagent assignment must include: goal, owned files, input/output interfaces, completion criteria, required tests, and a prohibition on uncoordinated changes to other agents' contracts. Deliverables: file list, implemented behavior, verification commands, and known limitations. Reviews may be read-only.

Changes in the shared checkout are visible to everyone. Do not delete or overwrite others' work; the primary agent integrates shared files. Do not replace subagents with new user-facing chats. Do not wait for an agent when independent integration work can proceed.

## 15. Repository deliverables

- A working Rust package and Cargo.lock.
- A README covering installation, OAuth, configuration, serve/sync/query/export, recovery, and limitations.
- A secret-free example configuration, private-by-default permissions, and a `.gitignore` for data/tokens.
- Documentation of the data schema/migrations and local API.
- A Polar-completeness matrix, fixtures, and tests.
- A benchmark report and reproduction commands.
- An explicit list of unverified aspects of the real Loop account. A successful fake-server test must not substitute for integration verification with the user.

The final report must state what builds, what is queryable, what is archive-only, what was actually verified, memory/performance results, and remaining limitations. Do not create implementation code while preparing this specification.
