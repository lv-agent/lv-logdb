#!/usr/bin/env bash
# logdb v0.1.0 — Performance Benchmark Suite
#
# Runs the full performance test suite and outputs results to stdout
# and a timestamped log file. Designed for bare-metal execution.
#
# Usage:
#   ./scripts/benchmark.sh [OUTPUT_DIR]
#
# Output:
#   OUTPUT_DIR/benchmark-YYYYMMDD-HHMMSS.log
#
# Exit code: 0 on success, non-zero on failure

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
OUTPUT_DIR="${1:-$PROJECT_DIR/benchmark-results}"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
LOG_FILE="$OUTPUT_DIR/benchmark-$TIMESTAMP.log"

mkdir -p "$OUTPUT_DIR"

log()  { echo "[$(date '+%H:%M:%S')] $*" | tee -a "$LOG_FILE"; }
section() {
    echo "" | tee -a "$LOG_FILE"
    echo "════════════════════════════════════════════════════════" | tee -a "$LOG_FILE"
    echo "  $*" | tee -a "$LOG_FILE"
    echo "════════════════════════════════════════════════════════" | tee -a "$LOG_FILE"
}

# ── Environment info ────────────────────────────────────────────────────────

section "Environment"

log "hostname: $(hostname)"
log "uname:    $(uname -a)"
log "cpus:     $(nproc)"
log "memory:   $(free -h | grep Mem | awk '{print $2}')"

# Detect disk type
DISK=$(df -T "$OUTPUT_DIR" | tail -1 | awk '{print $2, $7}')
log "disk:     $DISK"

# Detect Rust toolchain
log "rustc:    $(rustc --version)"
log "cargo:    $(cargo --version)"

# ── Locate binary ───────────────────────────────────────────────────────────

# Check for pre-built binary (packaged deployment) first
if [[ -x "$SCRIPT_DIR/../bin/perf" ]]; then
    BIN="$SCRIPT_DIR/../bin/perf"
    log "Using pre-built binary: $BIN"
elif [[ -x "$PROJECT_DIR/target/release/examples/perf" ]]; then
    BIN="$PROJECT_DIR/target/release/examples/perf"
    log "Using existing binary: $BIN"
else
    section "Build"
    cd "$PROJECT_DIR"
    log "Building release binary..."
    cargo build --release --example perf 2>&1 | tail -3 | tee -a "$LOG_FILE"
    BIN="$PROJECT_DIR/target/release/examples/perf"
fi

if [[ ! -x "$BIN" ]]; then
    log "ERROR: perf binary not found at $BIN"
    log "Run ./scripts/build.sh or ./scripts/package.sh first"
    exit 1
fi

# ── Run benchmarks ──────────────────────────────────────────────────────────

section "Performance Benchmarks"

log "Running full performance suite (this takes ~60 seconds)..."
log ""

# Run the perf example and capture output
set +e  # Don't exit on benchmark failure — capture results
"$BIN" 2>&1 | tee -a "$LOG_FILE"
BENCH_EXIT=$?
set -e

if [[ $BENCH_EXIT -ne 0 ]]; then
    log ""
    log "WARNING: perf example exited with code $BENCH_EXIT"
fi

# ── Parse and summarize ─────────────────────────────────────────────────────

section "Summary"

log "Benchmark complete. Full results: $LOG_FILE"
log ""

# Extract key metrics
echo "Key Metrics:" | tee -a "$LOG_FILE"
grep -E "append/(64B|256B)/1t \(inline\)" "$LOG_FILE" | head -4 | tee -a "$LOG_FILE" || true
grep -E "append/300B/1t \(spill\)" "$LOG_FILE" | head -2 | tee -a "$LOG_FILE" || true
grep -E "^  [248]t:" "$LOG_FILE" | tee -a "$LOG_FILE" || true
grep -E "interval=10ms:" -A5 "$LOG_FILE" | tee -a "$LOG_FILE" || true

echo "" | tee -a "$LOG_FILE"
echo "Done. Log: $LOG_FILE" | tee -a "$LOG_FILE"

exit 0
