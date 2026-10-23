# rust-polar

Local-first Rust archive for Polar AccessLink data. It includes OAuth authorization, a resident Unix-socket service, scheduled sync planning, durable source archives, and replay into redb. Real sync still requires credentials and has not been tested against a Polar account.

## Build and verify

Requires Rust 1.90 or newer.

```sh
cargo test --offline --all-targets
cargo clippy --offline --all-targets -- -D warnings
```

The build uses RustCrypto for TLS and the Rust backend for gzip. It does not use BLE, Polar Flow private endpoints, or raw sensor streams.

## Local archive commands

```sh
cargo run -- init
cargo run -- status
cargo run -- auth polar --member-id YOUR_UNIQUE_MEMBER_ID
cargo run -- auth import --user-id POLAR_ACCESSLINK_USER_ID
cargo run -- sync polar
cargo run -- sync polar --from 2026-10-01 --to 2026-10-21
cargo run -- archive verify
cargo run -- archive replay
cargo run -- query --limit 100
cargo run -- export --metric calories --from 2026-10-01T00:00:00Z \
  --to 2026-11-01T00:00:00Z --format csv
cargo run -- backup /mnt/backup/rust-polar-2026-10-21
cargo run -- --data-dir /var/lib/rust-polar-restored restore /mnt/backup/rust-polar-2026-10-21
cargo run -- serve
```

Set `RUST_POLAR_DATA_DIR` or pass `--data-dir` to select the local data directory. It contains `health.redb` and content-addressed compressed source responses. Unix permissions are restricted to the current user.

An environment template for the resident service is in [rust-polar.env.example](rust-polar.env.example). `RUST_POLAR_DB_CACHE_BYTES` accepts 1–256 MiB and defaults to 8 MiB. `RUST_POLAR_SYNC_INTERVAL_SECONDS` accepts 30 seconds to 30 days and defaults to 1,800 seconds. `RUST_POLAR_MIN_FREE_BYTES` sets the disk-space reserve before an archive write and defaults to 512 MiB.

Parsed provider records and normalized measurements are written durably to `health.redb`; replay is idempotent across restarts. The record documents retain provider fields and timestamps even when a local clock value cannot safely be converted to UTC. Only UTC-resolved measurements enter time-series queries. The compressed source-response archive is retained for recovery and future reprocessing; no intermediate JSON files are needed. `query` emits a bounded JSON Lines page of stored records.

`serve` keeps the one redb handle open and exposes the read-only local API at `DATA_DIR/run/rust-polar.sock`. Its in-process scheduler defaults to 30 minutes (`--sync-interval-seconds`, or `RUST_POLAR_SYNC_INTERVAL_SECONDS`). With credentials present it runs archive-first sync cycles, then imports each successful response into durable records and measurements. The database file remains on disk; redb's bounded cache and OS page cache keep frequently used data readily available without pinning the whole database in RAM.

`sync polar` runs a one-shot cycle. Add inclusive `--from` and `--to` dates together to request a bounded window; activity history is capped at the documented 365 days, and shorter resource history limits still apply. When the daemon is running, the CLI asks it to perform the cycle over the private socket, serialized with its scheduled cycle. Otherwise the CLI opens redb exclusively for that operation.

`export` reads one normalized metric over a half-open UTC interval and writes bounded JSONL or CSV pages. It uses the resident daemon when available, otherwise opens redb for the duration of the command. Pass `--user-id` when querying a local import or a user other than the authorized Polar account.

While `serve` is running, `query`, `archive replay`, and `backup` use its Unix socket instead of opening a second redb handle. Backup creates a consistent snapshot plus archived source responses; it excludes credentials. Restore verifies a backup and requires a new data directory with the service stopped. See [API.md](API.md) for socket routes and response fields.

`auth polar` expects `POLAR_CLIENT_ID` and `POLAR_CLIENT_SECRET` in the environment. Register `http://127.0.0.1:8765/callback` as the OAuth redirect URI in the Polar AccessLink application. The command prints an authorization URL for you to open, waits up to five minutes for the loopback callback, checks OAuth state, exchanges the code once, registers the supplied member ID, and saves credentials under `DATA_DIR/credentials/polar.json` with Unix mode `0600` in a `0700` directory. The file is not encrypted; protect the data directory and SSD backups. The command never opens a browser and does not print the callback code or token.

If you already have a Polar AccessLink Bearer token, `auth import --user-id POLAR_ACCESSLINK_USER_ID` reads it from a hidden terminal prompt, checks it with one read-only user-profile request (discarding the response body), and saves it with the same private file permissions. Use the Polar-issued AccessLink `user-id` returned by registration; a Polar username or app-chosen `member-id` is not interchangeable. The importer does not store Client ID/Secret and does not renew expiring tokens, so verify the token's remaining lifetime before relying on unattended collection. OAuth and import credentials both use the same storage format; reauthorize if an imported token expires.

For Raspberry Pi SSD, restart, low-memory and backup guidance, see [RASPBERRY_PI.md](RASPBERRY_PI.md). A systemd unit and a diagnostics bundle script for overnight observations are in `deploy/systemd/` and `scripts/`. The service is designed to restart safely and replay pending archived responses; backup/restore tooling is available through the CLI and daemon API.

## Current limits

No OAuth credentials are needed for offline replay or backup restore. Several data-coverage, recovery, and performance requirements remain pending. See [IMPLEMENTATION_STATUS.md](IMPLEMENTATION_STATUS.md) for remaining work and [SPEC.md](SPEC.md) for the target behavior.
