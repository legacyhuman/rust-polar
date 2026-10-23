# rust-polar

**Polar health data, kept local.**

A lightweight Rust service that collects data from the official Polar AccessLink API
and stores it on your own machine. Designed for Raspberry Pi and other small Linux
hosts with local SSD storage.

- **Health history** — activity, heart rate, sleep, recovery, and training records.
- **Local access** — an embedded [redb](https://github.com/cberner/redb) database,
  a Unix-socket API, and CSV / JSONL exports.
- **Automatic collection** — scheduled synchronization every 30 minutes by default.
- **Durable storage** — parsed records, replayable source archives, consistent
  backups, and an 8 MiB database cache by default.

## Quick start

Requires **Rust 1.90+** and a valid Polar AccessLink token with its Polar-issued user ID.

```sh
cargo build --release --locked --bin rust-polar
export RUST_POLAR_DATA_DIR="$PWD/data"

./target/release/rust-polar init
./target/release/rust-polar auth import --user-id YOUR_POLAR_USER_ID
./target/release/rust-polar sync polar
./target/release/rust-polar serve
```

The token is entered through a hidden prompt. For OAuth setup and other commands,
see the [usage guide](USAGE.md). Available data depends on your device and AccessLink permissions.

## Documentation

[Usage](USAGE.md) · [Local API](API.md) · [Raspberry Pi](RASPBERRY_PI.md) · [Data coverage](POLAR_COVERAGE.md)

---

**AI disclosure:** This repository, including its source code, tests, and documentation
(including this README), was created by AI agents under human direction.
Review and validate the implementation before relying on it.
