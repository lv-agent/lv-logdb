#!/usr/bin/env bash
# logdb — Build release binaries for deployment (no-Rust target machines).
#
# Environment variables:
#   FEATURES  — comma-separated Cargo features (default: none)
#               e.g. FEATURES="hash-chain,compression"
#   TARGET    — cross-compilation target triple (default: native)
#               e.g. TARGET=aarch64-unknown-linux-gnu
#
# Cross-compilation example (x86 → arm64):
#   # one-time setup
#   rustup target add aarch64-unknown-linux-gnu
#   sudo apt install gcc-aarch64-linux-gnu   # Debian/Ubuntu
#   # or on Fedora:
#   sudo dnf install gcc-aarch64-linux-gnu
#
#   TARGET=aarch64-unknown-linux-gnu FEATURES="hash-chain" ./scripts/build.sh
#
# Produces in target/<triple>/release/examples/:
#   perf             — append throughput / latency
#   scan_perf        — range-scan throughput
#   read_perf        — point-read throughput + read_batch
#   soak             — long-running stability test
#   crash_test       — crash recovery helper
#   testsuite        — internal smoke tests (needs --features testing)
#   sharding         — multi-shard write + scan
#   tailer_consumer  — named tailer with commit + reopen
#
# Copy the resulting binaries to the target machine and run them directly
# (no Rust toolchain needed; only a standard Linux glibc).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKSPACE_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$WORKSPACE_DIR"

FEATURES="${FEATURES:-}"
TARGET="${TARGET:-}"

# ── Prerequisites for cross-compilation ────────────────────────────────────

if [ -n "$TARGET" ]; then
    # Verify the Rust target is installed.
    if ! rustup target list --installed 2>/dev/null | grep -qF "$TARGET"; then
        echo "ERROR: target '$TARGET' is not installed."
        echo "  Install it:  rustup target add $TARGET"
        exit 1
    fi

    # Select the right linker for common targets.  Without this, rustc may
    # fall back to lld, which is often not installed.
    case "$TARGET" in
        aarch64-unknown-linux-gnu)
            LINKER="${LINKER:-aarch64-linux-gnu-gcc}"
            ;;
        aarch64-unknown-linux-musl)
            LINKER="${LINKER:-aarch64-linux-gnu-gcc}";;
        armv7-unknown-linux-gnueabihf)
            LINKER="${LINKER:-arm-linux-gnueabihf-gcc}";;
        riscv64gc-unknown-linux-gnu)
            LINKER="${LINKER:-riscv64-linux-gnu-gcc}";;
        *)
            LINKER="${LINKER:-}";;
    esac

    if [ -n "$LINKER" ] && ! command -v "$LINKER" &>/dev/null; then
        echo "ERROR: cross-linker '$LINKER' not found."
        echo "  On Debian/Ubuntu:  sudo apt install gcc-${TARGET%-unknown-*}"
        echo "  On Fedora:         sudo dnf install gcc-${TARGET%-unknown-*}"
        echo "  Or set LINKER env to your cross-gcc path."
        exit 1
    fi

    if [ -n "$LINKER" ]; then
        export "CARGO_TARGET_$(echo "$TARGET" | tr '[:lower:]-' '[:upper:]_')_LINKER=$LINKER"
        echo "cross-linker: $LINKER"
    fi
fi

# Enable host-CPU-specific optimizations (e.g. ARMv8 LSE atomic instructions
# which can measurably speed up CAS-heavy code like the ring buffer).
# Only set for native builds; cross-compilation leaves RUSTFLAGS up to the
# caller (target-cpu=native would optimise for the wrong machine).
if [ -z "$TARGET" ] && [ -z "${RUSTFLAGS:-}" ]; then
    export RUSTFLAGS="-C target-cpu=native"
fi

echo "=== logdb Build ==="
echo "rustc:    $(rustc --version)"
echo "cargo:    $(cargo --version)"
echo "host:     $(rustc -vV | grep host | awk '{print $2}') (build machine)"
echo "target:   ${TARGET:-$(rustc -vV | grep host | awk '{print $2}') (native)}"
echo "RUSTFLAGS: ${RUSTFLAGS:-<none>}"
echo "features: '${FEATURES}'"
echo ""

# Compute the binary output directory; cross-compilation puts binaries under
# target/<triple>/release/ instead of target/release/.
if [ -n "$TARGET" ]; then
    CARGO_TARGET_FLAG="--target $TARGET"
    TARGET_DIR="target/$TARGET/release"
else
    CARGO_TARGET_FLAG=""
    TARGET_DIR="target/release"
fi

echo "Building performance examples (features: '${FEATURES:-none}')..."
echo "  cargo build --release -p logdb --features \"$FEATURES\" $CARGO_TARGET_FLAG --example perf ..."
cargo build --release -p logdb --features "$FEATURES" $CARGO_TARGET_FLAG \
    --example perf --example scan_perf --example read_perf \
    --example sharding --example tailer_consumer

echo "Building soak / crash_test / testsuite..."
echo "  cargo build --release -p logdb --features testing $CARGO_TARGET_FLAG --example soak ..."
cargo build --release -p logdb --features testing $CARGO_TARGET_FLAG \
    --example soak --example crash_test --example testsuite

echo ""
echo "Binaries:"
BIN_DIR="$TARGET_DIR/examples"
ls -lh "$BIN_DIR"/{perf,scan_perf,read_perf,sharding,tailer_consumer,soak,crash_test,testsuite}
echo ""
echo "=== Build complete ==="
echo ""
echo "To run on the target machine (no Rust needed):"
echo "  scp $BIN_DIR/{perf,scan_perf,read_perf} user@target:/tmp/"
echo "  ssh user@target /tmp/perf"
echo "  ssh user@target /tmp/scan_perf"
echo "  ssh user@target /tmp/read_perf"
echo ""
