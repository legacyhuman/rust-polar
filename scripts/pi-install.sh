#!/usr/bin/env bash
set -euo pipefail

if [ "$(id -u)" -ne 0 ]; then
    echo "Run this installer as root on 64-bit Raspberry Pi OS or DietPi." >&2
    exit 1
fi
if [ "$(uname -m)" != aarch64 ]; then
    echo "Expected a 64-bit ARM system (aarch64), got: $(uname -m)" >&2
    exit 1
fi
if [ ! -f Cargo.toml ] || [ ! -f Cargo.lock ]; then
    echo "Run this script from the rust-polar source directory." >&2
    exit 1
fi

if systemctl is-active --quiet rust-polar.service; then
    echo "rust-polar.service is already active; stopping to avoid replacing a live daemon." >&2
    exit 1
fi
if systemctl cat rust-polar.service >/dev/null 2>&1; then
    echo "rust-polar.service already exists; inspect it before upgrading." >&2
    exit 1
fi
if [ -e /var/lib/rust-polar ]; then
    echo "/var/lib/rust-polar already exists; preserve and inspect it before installing." >&2
    exit 1
fi
if [ -e /usr/local/bin/rust-polar ]; then
    echo "/usr/local/bin/rust-polar already exists; inspect it before replacing." >&2
    exit 1
fi

if ! id rust-polar >/dev/null 2>&1; then
    useradd --system --home-dir /var/lib/rust-polar --shell /usr/sbin/nologin rust-polar
fi

echo "Building the locked release binary with one compiler job to limit peak RAM..."
CARGO_BUILD_JOBS=1 cargo build --release --locked --bin rust-polar

install -D -o root -g root -m 0755 target/release/rust-polar /usr/local/bin/rust-polar
install -D -o root -g root -m 0644 deploy/systemd/rust-polar.service /etc/systemd/system/rust-polar.service
install -D -o root -g root -m 0755 scripts/pi-diagnostics.sh /usr/local/sbin/rust-polar-diagnostics
install -d -o rust-polar -g rust-polar -m 0700 /var/lib/rust-polar
systemctl daemon-reload

echo "Installed rust-polar $(/usr/local/bin/rust-polar --version)."
echo "State is new and empty. Authorize Polar as rust-polar, then run:"
echo "  systemctl enable --now rust-polar.service"
echo "  sudo -u rust-polar env RUST_POLAR_DATA_DIR=/var/lib/rust-polar /usr/local/bin/rust-polar sync polar"
echo "Morning diagnostics: /usr/local/sbin/rust-polar-diagnostics"
