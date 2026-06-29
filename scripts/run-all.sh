#!/usr/bin/env bash
# logdb v0.1.0 — Master Qualification Test Runner
#
# Runs all qualification tests in order:
#   1. Unit + integration tests
#   2. Performance benchmarks
#   3. Crash recovery test
#   4. Soak test (optional, use --soak to enable)
#
# Usage:
#   ./scripts/run-all.sh [--soak] [--soak-duration 86400] [--iterations 100]
#
# Output: results/ directory with timestamped logs

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
RESULTS_DIR="$PROJECT_DIR/qualification-results/$TIMESTAMP"

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

cd "$PROJECT_DIR"

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║         logdb v0.1.0 — Qualification Test Suite             ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
log "Results dir: $RESULTS_DIR"
log "Soak test:   $RUN_SOAK (${SOAK_DURATION}s)"
log "Crash iter:  $CRASH_ITERATIONS"
log ""

# ── Step 1: Unit + Integration Tests ───────────────────────────────────────

log "══════ Step 1/4: Unit + Integration Tests ══════"

# Use testsuite binary if available (deployed), otherwise fall back to cargo
if [[ -x "$SCRIPT_DIR/../bin/testsuite" ]]; then
    log "Running test suite binary..."
    if "$SCRIPT_DIR/../bin/testsuite" 2>&1 | tee "$RESULTS_DIR/unit-tests.log"; then
        log "  Test suite: PASS"
    else
        log "  Test suite: FAIL"
        exit 1
    fi
elif [[ -x "$PROJECT_DIR/target/release/examples/testsuite" ]]; then
    log "Running test suite binary (dev)..."
    if "$PROJECT_DIR/target/release/examples/testsuite" 2>&1 | tee "$RESULTS_DIR/unit-tests.log"; then
        log "  Test suite: PASS"
    else
        log "  Test suite: FAIL"
        exit 1
    fi
elif command -v cargo &>/dev/null; then
    log "Building and running tests via cargo..."
    if cargo test --lib 2>&1 | tee "$RESULTS_DIR/unit-tests.log" | tail -3; then
        log "  Unit tests: PASS"
    else
        log "  Unit tests: FAIL"
        exit 1
    fi
    if cargo test --test integration 2>&1 | tee "$RESULTS_DIR/integration-tests.log" | tail -3; then
        log "  Integration tests: PASS"
    else
        log "  Integration tests: FAIL"
        exit 1
    fi
else
    log "  ERROR: No test suite binary or cargo found"
    exit 1
fi

# ── Step 2: Performance Benchmarks ──────────────────────────────────────────

log ""
log "══════ Step 2/4: Performance Benchmarks ══════"

bash "$SCRIPT_DIR/benchmark.sh" "$RESULTS_DIR"
log "  Benchmark: PASS (results in $RESULTS_DIR)"

# ── Step 3: Crash Recovery Test ─────────────────────────────────────────────

log ""
log "══════ Step 3/4: Crash Recovery Test ($CRASH_ITERATIONS iterations) ══════"

DATA_DIR="/tmp/logdb-qual-crash"

if bash "$SCRIPT_DIR/crash-recovery-test.sh" "$CRASH_ITERATIONS" "$DATA_DIR" 2>&1 \
    | tee "$RESULTS_DIR/crash-recovery.log" | tail -3; then
    log "  Crash recovery: PASS"
else
    log "  Crash recovery: FAIL"
    exit 1
fi

# ── Step 4: Soak Test (optional) ────────────────────────────────────────────

if $RUN_SOAK; then
    log ""
    log "══════ Step 4/4: Soak Test (${SOAK_DURATION}s) ══════"

    if bash "$SCRIPT_DIR/soak-test.sh" "$SOAK_DURATION" "/tmp/logdb-qual-soak" 2>&1 \
        | tee "$RESULTS_DIR/soak-test.log" | tail -3; then
        log "  Soak test: PASS"
    else
        log "  Soak test: FAIL"
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
