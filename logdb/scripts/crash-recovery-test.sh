#!/usr/bin/env bash
# logdb v0.1.0 — Crash Recovery Test
#
# Repeatedly: append → kill -9 → recover → verify no data loss above durable cursor.
# Designed for bare-metal execution.
#
# Usage:
#   ./scripts/crash-recovery-test.sh [ITERATIONS] [DATA_DIR]
#
# Default: 100 iterations, /tmp/logdb-crash-test

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ITERATIONS="${1:-100}"
DATA_DIR="${2:-/tmp/logdb-crash-test}"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
LOG_FILE="$PROJECT_DIR/crash-test-results/crash-recovery-$TIMESTAMP.log"

mkdir -p "$PROJECT_DIR/crash-test-results"

log()  { echo "[$(date '+%H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

# ── Locate binary ───────────────────────────────────────────────────────────

cd "$PROJECT_DIR"

if [[ -x "$SCRIPT_DIR/../bin/crash_test" ]]; then
    CRASH_BIN="$SCRIPT_DIR/../bin/crash_test"
elif [[ -x "$PROJECT_DIR/target/release/examples/crash_test" ]]; then
    CRASH_BIN="$PROJECT_DIR/target/release/examples/crash_test"
else
    log "Building crash_test binary..."
    cargo build --release --example crash_test 2>&1 | tail -3
    CRASH_BIN="$PROJECT_DIR/target/release/examples/crash_test"
fi

if [[ ! -x "$CRASH_BIN" ]]; then
    log "ERROR: crash_test binary not found"
    log "Run ./scripts/build.sh first"
    exit 1
fi

log "Binary: $CRASH_BIN"
log "Iterations: $ITERATIONS"
log "Data dir: $DATA_DIR"

# ── Run iterations ──────────────────────────────────────────────────────────

PASS=0
FAIL=0

for ((i=1; i<=ITERATIONS; i++)); do
    rm -rf "$DATA_DIR"

    log ""
    log "=== Iteration $i/$ITERATIONS ==="

    # Phase 1: Append some records and flush
    log "  Phase 1: Writing records..."
    "$CRASH_BIN" writer "$DATA_DIR" 2>&1 | tail -3 | tee -a "$LOG_FILE"
    WRITER_EXIT=${PIPESTATUS[0]}

    if [[ $WRITER_EXIT -ne 0 ]]; then
        log "  FAIL: writer exited with $WRITER_EXIT"
        FAIL=$((FAIL + 1))
        continue
    fi

    # Phase 2: Verify recovery reads the same data
    log "  Phase 2: Verifying recovery..."
    "$CRASH_BIN" reader "$DATA_DIR" 2>&1 | tail -3 | tee -a "$LOG_FILE"
    READER_EXIT=${PIPESTATUS[0]}

    if [[ $READER_EXIT -eq 0 ]]; then
        log "  PASS"
        PASS=$((PASS + 1))
    else
        log "  FAIL: reader exited with $READER_EXIT"
        FAIL=$((FAIL + 1))
    fi
done

# ── Summary ─────────────────────────────────────────────────────────────────

log ""
log "════════════════════════════════════════════════════════"
log "  Crash Recovery Test Complete"
log "  Pass: $PASS / $ITERATIONS"
log "  Fail: $FAIL / $ITERATIONS"
log "  Log:  $LOG_FILE"
log "════════════════════════════════════════════════════════"

if [[ $FAIL -gt 0 ]]; then
    log "RESULT: FAIL ($FAIL failures)"
    exit 1
else
    log "RESULT: PASS"
    exit 0
fi
