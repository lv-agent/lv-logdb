#!/usr/bin/env bash
# logdb v0.1.0 — Master Test Runner (Deployed)
#
# For use with pre-built binaries (no Rust toolchain required).
# Runs: testsuite → benchmark → crash recovery → (optional) soak
#
# Usage:
#   ./scripts/run-all-deployed.sh [--soak] [--soak-duration 86400] [--iterations 100]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PACKAGE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
RESULTS_DIR="$PACKAGE_DIR/results/$TIMESTAMP"

RUN_SOAK=false
SOAK_DURATION=3600
CRASH_ITERATIONS=100

while [[ $# -gt 0 ]]; do
    case $1 in
        --soak) RUN_SOAK=true; shift ;;
        --soak-duration) SOAK_DURATION="$2"; shift 2 ;;
        --iterations) CRASH_ITERATIONS="$2"; shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

mkdir -p "$RESULTS_DIR"

log() { echo "[$(date '+%H:%M:%S')] $*" | tee -a "$RESULTS_DIR/summary.log"; }

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║       logdb v0.1.0 — Qualification Tests (Deployed)         ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
log "Package dir: $PACKAGE_DIR"
log "Results dir: $RESULTS_DIR"
log "Soak test:   $RUN_SOAK (${SOAK_DURATION}s)"
log "Crash iter:  $CRASH_ITERATIONS"
log ""

# ── Step 1: Test Suite ─────────────────────────────────────────────────────

log "══════ Step 1/4: Test Suite ══════"

if [[ -x "$PACKAGE_DIR/bin/testsuite" ]]; then
    if "$PACKAGE_DIR/bin/testsuite" 2>&1 | tee "$RESULTS_DIR/testsuite.log"; then
        log "  Test suite: PASS"
    else
        log "  Test suite: FAIL"
        exit 1
    fi
else
    log "  ERROR: testsuite binary not found at $PACKAGE_DIR/bin/testsuite"
    exit 1
fi

# ── Step 2: Performance Benchmarks ──────────────────────────────────────────

log ""
log "══════ Step 2/4: Performance Benchmarks ══════"

if [[ -x "$PACKAGE_DIR/bin/perf" ]]; then
    "$PACKAGE_DIR/bin/perf" 2>&1 | tee "$RESULTS_DIR/benchmark.log"
    log "  Benchmark: PASS (results in $RESULTS_DIR)"
else
    log "  ERROR: perf binary not found"
    exit 1
fi

# ── Step 3: Crash Recovery Test ─────────────────────────────────────────────

log ""
log "══════ Step 3/4: Crash Recovery Test ($CRASH_ITERATIONS iterations) ══════"

CRASH_BIN="$PACKAGE_DIR/bin/crash_test"
DATA_DIR="/tmp/logdb-qual-crash"

if [[ ! -x "$CRASH_BIN" ]]; then
    log "  ERROR: crash_test binary not found"
    exit 1
fi

PASS=0
FAIL=0

for ((i=1; i<=CRASH_ITERATIONS; i++)); do
    rm -rf "$DATA_DIR"

    log "  [$i/$CRASH_ITERATIONS] Writing..."
    "$CRASH_BIN" writer "$DATA_DIR" 2>&1 | tail -1 | tee -a "$RESULTS_DIR/crash-recovery.log"
    WRITER_EXIT=${PIPESTATUS[0]}

    if [[ $WRITER_EXIT -ne 0 ]]; then
        log "  [$i/$CRASH_ITERATIONS] FAIL: writer exit=$WRITER_EXIT"
        FAIL=$((FAIL + 1))
        continue
    fi

    log "  [$i/$CRASH_ITERATIONS] Verifying..."
    "$CRASH_BIN" reader "$DATA_DIR" 2>&1 | tail -1 | tee -a "$RESULTS_DIR/crash-recovery.log"
    READER_EXIT=${PIPESTATUS[0]}

    if [[ $READER_EXIT -eq 0 ]]; then
        PASS=$((PASS + 1))
    else
        log "  [$i/$CRASH_ITERATIONS] FAIL: reader exit=$READER_EXIT"
        FAIL=$((FAIL + 1))
    fi
done

log "  Crash recovery: $PASS/$CRASH_ITERATIONS passed"

if [[ $FAIL -gt 0 ]]; then
    log "  Crash recovery: FAIL ($FAIL failures)"
    exit 1
fi
log "  Crash recovery: PASS"

# ── Step 4: Soak Test (optional) ────────────────────────────────────────────

if $RUN_SOAK; then
    log ""
    log "══════ Step 4/4: Soak Test (${SOAK_DURATION}s) ══════"

    if [[ -x "$PACKAGE_DIR/bin/soak" ]]; then
        "$PACKAGE_DIR/bin/soak" --duration-secs "$SOAK_DURATION" --data-dir "/tmp/logdb-qual-soak" \
            2>&1 | tee "$RESULTS_DIR/soak-test.log"
        log "  Soak test: PASS"
    else
        log "  ERROR: soak binary not found"
        exit 1
    fi
else
    log ""
    log "══════ Step 4/4: Soak Test — SKIPPED (use --soak to enable) ══════"
fi

# ── Done ────────────────────────────────────────────────────────────────────

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║                   All Tests Passed                          ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
log "Results: $RESULTS_DIR"
log ""

exit 0
