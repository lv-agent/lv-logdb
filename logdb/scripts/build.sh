#!/usr/bin/env bash
# logdb — Build release binaries for deployment (no-Rust target machines).
#
# Set FEATURES to select optional crate features (comma-separated):
#   FEATURES="hash-chain" ./scripts/build.sh
#   FEATURES="hash-chain,compression" ./scripts/build.sh
#   FEATURES="" ./scripts/build.sh              # default (no optional features)
#
# Produces in target/release/examples/:
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

echo "=== logdb Build ==="
echo "rustc: $(rustc --version)"
echo "cargo: $(cargo --version)"
echo "target: $(rustc -vV | grep host | awk '{print $2}')"
echo "features: '${FEATURES}'"
echo ""

echo "Building performance examples (features: '${FEATURES:-none}')..."
cargo build --release -p logdb --features "$FEATURES" \
    --example perf --example scan_perf --example read_perf \
    --example sharding --example tailer_consumer

echo "Building soak / crash_test / testsuite..."
cargo build --release -p logdb --features testing \
    --example soak --example crash_test --example testsuite

echo ""
echo "Binaries:"
ls -lh target/release/examples/{perf,scan_perf,read_perf,sharding,tailer_consumer,soak,crash_test,testsuite}
echo ""
echo "=== Build complete ==="
echo ""
echo "To run on the target machine (no Rust needed):"
echo "  scp target/release/examples/{perf,scan_perf,read_perf} user@target:/tmp/"
echo "  ssh user@target /tmp/perf"
echo "  ssh user@target /tmp/scan_perf"
echo "  ssh user@target /tmp/read_perf"
echo ""
