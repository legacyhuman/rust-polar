# Raspberry Pi deployment plan

This plan targets a Raspberry Pi 4 Model B with DietPi 10.7.2 (64-bit aarch64) and one persistent SSD. The binary has a resident `serve` process that keeps the redb handle open for local clients, serves a Unix-socket API, and runs an archive-first Polar sync cycle every 30 minutes by default. The first install and live Polar behavior are still being verified on the device. Backup/restore commands exist, and the scheduler removes stale archive artifacts after its first cycle and then at most daily.

## Storage and boot

- Boot Raspberry Pi OS from the SSD and keep both the OS and `/var/lib/rust-polar` on that drive. Use ext4 and mount by filesystem UUID; do not put the live redb database, archive, or temporary archive files on a network filesystem or `tmpfs`. A dedicated data partition is useful for isolation, but is not required when the whole root filesystem already lives on the SSD.
- Put application state under `/var/lib/rust-polar` with directory mode `0700` and files `0600`. The current store/archive code applies these Unix permissions itself.
- When data is a separate mount, define a real UUID-backed mount for the data directory and make the service require it. `RequiresMountsFor=` alone can resolve to the root filesystem if no dedicated mount is configured, so pair it with `ConditionPathIsMountPoint=` (or an equivalent explicit mount-unit dependency). This prevents a missing SSD/partition from silently creating a second database underneath the mountpoint. If `/var/lib/rust-polar` is simply on the root SSD, the root filesystem itself is the required mount.
- Keep archive temporary files inside the archive tree so atomic renames stay on one filesystem. On boot, ignore unfinished `.part` files, recover published sidecars, and replay unprocessed receipts before advancing any sync checkpoint.
- Do not disable filesystem barriers, fsync, or redb durability to reduce latency. Current archive files and descriptors are fsynced before publication; redb writes now explicitly use `Durability::Immediate`.

## Process supervision

Use one dedicated unprivileged service account and one long-running process that owns redb. Its in-process scheduler starts a sync cycle every 30 minutes by default; configure the interval rather than adding a separate systemd timer that would start a second database writer. The same process serves future local applications over a Unix domain socket. The maintained unit is [`deploy/systemd/rust-polar.service`](deploy/systemd/rust-polar.service); it restarts failures and grants write access only to the state directory. Configure an explicit mount dependency when the data directory has a dedicated mountpoint. Do not run CLI maintenance commands concurrently with the service; route their requests through the owner process or stop it first.

The service handles termination by stopping active sync work and closing the local socket. Database operations run in bounded blocking tasks; a dedicated writer queue remains a future optimization. A replay after restart must remain idempotent. `Restart=on-failure` is a recovery mechanism, not a substitute for those application-level invariants.

Run `auth polar` as the service account before enabling unattended collection; the registered Polar redirect URI is `http://127.0.0.1:8765/callback`. Credentials are stored unencrypted with mode `0600` under the service data directory. Verify the account, device capabilities, and operational behavior before relying on unattended collection.

When `/var/lib/rust-polar` is a dedicated mountpoint, add `ConditionPathIsMountPoint=/var/lib/rust-polar` and configure the actual UUID-backed mount unit. The checked-in unit assumes the data directory is on the root SSD by default.

For the first overnight observation, install the release binary and unit, authorize Polar as the service account, start the service, then run one manual sync before bed. The scheduled timer waits one full interval after service start before its first automatic run. The installer builds on the Pi with one Cargo job, installs the service and diagnostics script, and intentionally leaves the service stopped:

```sh
sudo ./scripts/pi-install.sh
```

For a new OAuth authorization, use the AccessLink app's **Client ID** and **Client Secret** from Polar Developer Center; the command also needs the app-chosen `member-id`. Do not send secrets in chat. If you already have a valid AccessLink Bearer token and the Polar-issued AccessLink `user-id` from a successful registration, you can import it without repeating OAuth. A username or app-chosen `member-id` is not a substitute for that server-issued `user-id`. The importer validates the token with one read-only profile request and discards the response; it does not refresh expiring tokens.

To import an existing token, run this as the service account. The token prompt does not echo the value or put it in shell history:

```sh
sudo -u rust-polar env RUST_POLAR_DATA_DIR=/var/lib/rust-polar /usr/local/bin/rust-polar auth import --user-id POLAR_ACCESSLINK_USER_ID
```

Continue to the service start and first-sync commands below only after the import reports success. If you do not know the exact Polar-issued `user-id`, use the OAuth flow instead of guessing from a username.

Register exactly `http://127.0.0.1:8765/callback` as the app's redirect URI. Keep an SSH local-forward open from your computer while running OAuth on the Pi, so the browser callback to your computer's loopback is forwarded to the Pi's loopback:

```sh
# On your computer, in a second terminal; leave this running during OAuth:
ssh -L 8765:127.0.0.1:8765 <user>@<raspberry-pi-host>
```

In the Pi shell, run the following interactive helper. It prompts without echoing either app credential, keeps them only in the process environment, and invokes the command as the unprivileged service account. Replace `YOUR_MEMBER_ID` with the member ID registered for the app. Then open the printed Polar authorization URL on your computer while the SSH forward is active; the browser returns through the tunnel to the Pi. Do not expose port 8765 on the LAN.

````sh
sudo -u rust-polar python3 - <<'PY'
import getpass, os

env = os.environ.copy()
env["RUST_POLAR_DATA_DIR"] = "/var/lib/rust-polar"
env["POLAR_CLIENT_ID"] = getpass.getpass("Polar Client ID: ")
env["POLAR_CLIENT_SECRET"] = getpass.getpass("Polar Client Secret: ")
os.execve("/usr/local/bin/rust-polar", ["rust-polar", "auth", "polar", "--member-id", "YOUR_MEMBER_ID"], env)
PY
````

After OAuth succeeds, enable the resident service and request the first sync. The manual command is serialized through the daemon; it avoids mistaking the initial 30-minute scheduler delay for a stalled service:

```sh
sudo systemctl enable --now rust-polar.service
sudo -u rust-polar env RUST_POLAR_DATA_DIR=/var/lib/rust-polar /usr/local/bin/rust-polar sync polar
```

A successful manual sync should report its result; check `GET /v1/sync/status` over the local socket before leaving the service overnight. The scheduler persists `running` before network work and turns it into `interrupted` after an unclean restart, preserving the prior completed run's counters.

In the morning, run `/usr/local/sbin/rust-polar-diagnostics` as root. It creates a mode-0600 tarball in `/var/tmp` with the last 24 hours of service journal, sync status, process restarts, memory and disk counters, kernel tail, and archive diagnose/verify results. Review it locally before sharing; it does not collect credentials, but logs may contain account identifiers or paths.

Do not set `MemoryMax` until release builds have been measured on the actual Pi model and workload. A too-low hard cap can kill replay repeatedly. Once measured, set a modest `MemoryHigh` and a `MemoryMax` above the observed peak with room for the OS and filesystem cache; alert on `memory.events` OOM counters.

## Memory and performance profile

Keep the existing explicit 8 MiB redb cache, one database writer, byte-bounded queues, 64 KiB archive copy buffer, and bounded database pages. The resident process keeps the redb database open for low-latency local queries, but it should not lock the complete database into physical RAM: redb cache plus hot OS page-cache pages form the working set, and the kernel may reclaim page cache when needed. The replay path iterates sidecars instead of retaining the complete receipt catalog in RAM and groups provider writes into at most 1000 rows/512 KiB per durable transaction (one row can use the 4 MiB record limit). Request and response handling must continue streaming; avoid a `Vec`/`Value` for an entire history.

For this DietPi installation, use the existing 64-bit image, disable unnecessary services, build the optimized binary for `aarch64-unknown-linux-gnu` on the Pi with one Cargo job, and run release mode. Do not use debug builds for performance measurements. Keep concurrency at one CPU-heavy decode/compression job and the current two-request global cap when live sync is added. Add `zram` only if actual memory pressure warrants its CPU cost; do not make swap a prerequisite for normal operation. Archive writes refuse to start when less than 512 MiB is free; adjust this reserve with `RUST_POLAR_MIN_FREE_BYTES` to match the SSD and expected response size.

Host release measurements are recorded in `BENCHMARK.md`: the latest five-year synthetic run completed at about 19,377 initial normalized rows/s, and the 30-day range query measured p95 around 139 ms, above the SPEC's 100 ms target. Prior host runs reported 18–19 MiB peak RSS for query-only; no peak RSS was captured for the latest full run. These are not Raspberry Pi measurements and do not establish an idle or replay peak for the Pi. Measure idle and peak RSS, replay throughput, archive compression CPU, redb write throughput, and p95 query latency on the Pi SSD before choosing any cache or compression changes. The current 8 MiB cache is a starting point, not a measured optimum. Avoid per-record fsync: normalize in bounded batches and commit each batch durably. If RSS is tight, first reduce cache size in a benchmarked config change and limit replay/import concurrency to one; do not trade away durable commits.

## Persistence and recovery limits

An SSD and durable commits should preserve state across an orderly restart and recover the latest consistent redb transaction after process or power interruption. The archive uses atomic publication plus fsync so complete objects and their sidecars can be rediscovered after a crash. An interrupted file may leave a `.part`; it is not a valid archive object. `archive diagnose` reports stale `.part` files and orphan objects; manual `archive cleanup` requires an explicit `--apply` after its dry run. The resident scheduler also performs bounded cleanup after its first 30-minute cycle and then at most daily, removing only orphan objects and `.part` files older than seven days. A blind recursive delete must never be used against the archive.

One SSD cannot protect against SSD failure, filesystem damage, theft, or controller failure. Schedule `rust-polar backup DEST` to a separate destination (for example, a manually attached drive or another machine on the LAN), then periodically verify by restoring into a new directory with `rust-polar --data-dir NEW_DIR restore DEST`. Backup includes a consistent redb snapshot and the response archive, but excludes OAuth credentials; authorize Polar again after restore. Never copy a live redb file with plain `cp`. Keep the live database local on the SSD even if a backup destination is remote.

Use a stable power supply and a powered SSD enclosure/adapter appropriate for the Pi. Low voltage or abrupt power loss can corrupt the filesystem or the SSD's own volatile write cache even when software calls `fsync`; a small UPS is the strongest protection if outages are common. Monitor free space and SSD health. When the SSD is full, stop sync/checkpoint advancement rather than deleting the only copy of archived health data.

## Implementation gates before unattended deployment

1. Prove bounded replay memory and throughput over a synthetic multi-year archive on both a development host and the target Pi.
2. Complete fake-server coverage for remaining sync failure/cancellation boundaries. Status/admin commands already route through the resident database owner.
3. Run restart/power-cut fault-injection tests on Linux ARM64. A systemd unit and morning diagnostics script are now included, but have not been exercised on the Pi.
4. Run backup/restore on the target Pi and compare logical counts and archive verification results.
5. Benchmark on the exact Raspberry Pi model, RAM size, SSD and enclosure before setting service memory limits.
