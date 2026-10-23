#!/usr/bin/env bash
set -u

BIN=${RUST_POLAR_BIN:-/usr/local/bin/rust-polar}
DATA_DIR=${RUST_POLAR_DATA_DIR:-/var/lib/rust-polar}
SERVICE=${RUST_POLAR_SERVICE:-rust-polar.service}
OUTPUT_ROOT=${RUST_POLAR_DIAG_ROOT:-/var/tmp}
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUT="$OUTPUT_ROOT/rust-polar-diagnostics-$STAMP"

umask 077
mkdir -p "$OUT" || exit 1

run() {
    local name=$1
    shift
    {
        printf 'command:'
        printf ' %q' "$@"
        printf '\n\n'
        "$@"
        local result=$?
        printf '\nexit_status=%s\n' "$result"
        return 0
    } >"$OUT/$name.txt" 2>&1
}

run system uname -a
run system uptime
run system free -h
run disk df -h "$DATA_DIR"
run service systemctl status --no-pager "$SERVICE"
run service-properties systemctl show "$SERVICE" \
    --property=ActiveState,SubState,Result,ExecMainCode,ExecMainStatus,NRestarts,MemoryCurrent,MemoryPeak,CPUUsageNSec,ActiveEnterTimestamp,InactiveEnterTimestamp
run journal journalctl --no-pager --since '-24 hours' -n 2000 -u "$SERVICE"
run sync-status curl --silent --show-error --max-time 10 \
    --unix-socket "$DATA_DIR/run/rust-polar.sock" http://localhost/v1/sync/status
run local-status runuser -u rust-polar -- "$BIN" --data-dir "$DATA_DIR" status
run archive-diagnose runuser -u rust-polar -- "$BIN" --data-dir "$DATA_DIR" archive diagnose --older-than-seconds 300
run archive-verify runuser -u rust-polar -- "$BIN" --data-dir "$DATA_DIR" archive verify

if command -v dmesg >/dev/null 2>&1; then
    run kernel-log sh -c 'dmesg --ctime 2>&1 | tail -n 300'
fi

CGROUP="/sys/fs/cgroup/system.slice/$SERVICE"
for file in memory.current memory.peak memory.events cpu.stat; do
    if [ -r "$CGROUP/$file" ]; then
        run "cgroup-${file//./-}" cat "$CGROUP/$file"
    fi
done

ARCHIVE="$OUT.tar.gz"
if tar -czf "$ARCHIVE" -C "$OUTPUT_ROOT" "${OUT##*/}"; then
    chmod 600 "$ARCHIVE"
    printf 'Diagnostics saved to %s\n' "$ARCHIVE"
else
    printf 'Could not package diagnostics; files remain in %s\n' "$OUT" >&2
    exit 1
fi
