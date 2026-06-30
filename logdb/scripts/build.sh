#!/usr/bin/env bash
# logdb — Build all release binaries for deployment
#
# Produces:
#   target/release/examples/perf       — performance benchmark
#   target/release/examples/soak       — soak test
#   target/release/examples/crash_test — crash recovery helper
#
# Usage:
#   ./scripts/build.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

echo "=== logdb Build ==="
echo "rustc: $(rustc --version)"
echo "cargo: $(cargo --version)"
echo "target: $(rustc -vV | grep host | awk '{print $2}')"
echo ""

echo "Building release binaries..."
# testsuite exercises internal modules, gated behind the `testing` feature.
cargo build --release --features testing \
    --example perf --example soak --example crash_test --example testsuite

echo ""
echo "Binaries:"
ls -lh target/release/examples/{perf,soak,crash_test,testsuite}
echo ""
echo "Build complete."
