#!/usr/bin/env bash
# logdb — Package release binaries + test scripts into a deployable tarball.
#
# Produces:
#   logdb-bench-<target>-<timestamp>.tar.gz
#
# Contents:
#   bin/perf        — performance benchmark binary
#   bin/soak        — soak test binary
#   bin/crash_test  — crash recovery helper binary
#   scripts/        — test runner scripts (use bin/ instead of cargo run)
#   README.txt      — quick start instructions
#
# Usage:
#   ./scripts/build.sh && ./scripts/package.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
TARGET="${1:-$(rustc -vV 2>/dev/null | grep host | awk '{print $2}' || echo 'x86_64-unknown-linux-gnu')}"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
PKG_NAME="logdb-bench-${TARGET}-${TIMESTAMP}"
BUILD_DIR="$PROJECT_DIR/target/package/$PKG_NAME"

cd "$PROJECT_DIR"

# ── Check binaries exist ────────────────────────────────────────────────────
BIN_DIR="$PROJECT_DIR/target/release/examples"
for bin in perf soak crash_test testsuite; do
    if [[ ! -x "$BIN_DIR/$bin" ]]; then
        echo "Binary not found: $BIN_DIR/$bin"
        echo "Run ./scripts/build.sh first"
        exit 1
    fi
done

# ── Create package directory ────────────────────────────────────────────────
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR/bin"
mkdir -p "$BUILD_DIR/scripts"
mkdir -p "$BUILD_DIR/results"

# ── Copy binaries ───────────────────────────────────────────────────────────
for bin in perf soak crash_test testsuite; do
    cp "$BIN_DIR/$bin" "$BUILD_DIR/bin/"
done
chmod +x "$BUILD_DIR/bin/"*

# ── Copy scripts ────────────────────────────────────────────────────────────
# benchmark.sh, soak-test.sh, crash-recovery-test.sh auto-detect pre-built
# binaries (they check $SCRIPT_DIR/../bin/ first, fall back to cargo).
cp "$PROJECT_DIR/scripts/benchmark.sh"           "$BUILD_DIR/scripts/"
cp "$PROJECT_DIR/scripts/soak-test.sh"           "$BUILD_DIR/scripts/"
cp "$PROJECT_DIR/scripts/crash-recovery-test.sh"  "$BUILD_DIR/scripts/"
# Use the dedicated deployed runner (no cargo references, no sed needed)
cp "$PROJECT_DIR/scripts/run-all-deployed.sh"     "$BUILD_DIR/scripts/run-all.sh"
chmod +x "$BUILD_DIR/scripts/"*

# ── Write README ────────────────────────────────────────────────────────────
cat > "$BUILD_DIR/README.txt" << 'EOF'
logdb v0.1.0 — Qualification Test Suite
========================================

This package contains pre-built binaries and scripts for running
logdb performance benchmarks and stability tests on bare-metal Linux.

Requirements:
  - Linux kernel 5.15+
  - x86_64 CPU (8+ cores recommended)
  - NVMe SSD
  - No Rust toolchain needed (binaries are statically linked)

Quick Start:
  1. Run all tests (benchmark + crash recovery):
       ./scripts/run-all.sh

  2. Run individual tests:
       ./scripts/benchmark.sh              # ~1 minute
       ./scripts/crash-recovery-test.sh     # ~10 minutes (100 iterations)
       ./scripts/soak-test.sh 86400         # 24 hours

  3. Run binaries directly:
       ./bin/perf                           # performance benchmark
       ./bin/soak --duration-secs 3600      # 1-hour soak test
       ./bin/crash_test writer /tmp/data    # write test data
       ./bin/crash_test reader /tmp/data    # verify test data

Results:
  All test output is saved to the results/ directory.

Hardware Recommendations:
  - AWS i3.xlarge (NVMe SSD) or equivalent
  - Bare-metal workstation with Samsung 990 Pro or similar
  - Do NOT use network-attached storage (EBS gp3, NFS, etc.)
    fdatasync latency depends on local NVMe.
EOF

# ── Package ─────────────────────────────────────────────────────────────────
ARCHIVE="${PKG_NAME}.tar.gz"
cd "$PROJECT_DIR/target/package"
tar czf "$PROJECT_DIR/$ARCHIVE" "$PKG_NAME"

echo ""
echo "Package created: $ARCHIVE"
echo "Size: $(du -h "$PROJECT_DIR/$ARCHIVE" | cut -f1)"
echo ""
echo "Deploy to target machine:"
echo "  scp $ARCHIVE user@host:~/"
echo "  ssh user@host"
echo "  tar xzf $ARCHIVE"
echo "  cd $PKG_NAME"
echo "  ./scripts/run-all.sh"
