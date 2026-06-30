#!/usr/bin/env bash
# logdb v0.1.0 — Soak Test Runner
#
# Runs the soak test binary with configurable duration.
# Designed for bare-metal execution.
#
# Usage:
#   ./scripts/soak-test.sh [--duration-secs 86400] [--data-dir /tmp/logdb-soak]
#
# Default: 3600 seconds (1 hour)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DURATION="${1:-3600}"
DATA_DIR="${2:-/tmp/logdb-soak}"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
LOG_FILE="$PROJECT_DIR/soak-results/soak-$TIMESTAMP.log"

mkdir -p "$PROJECT_DIR/soak-results"

echo "=== logdb Soak Test ==="
echo "duration: ${DURATION}s"
echo "data_dir: $DATA_DIR"
echo "log:      $LOG_FILE"
echo ""

cd "$PROJECT_DIR"

# Locate binary
if [[ -x "$SCRIPT_DIR/../bin/soak" ]]; then
    BIN="$SCRIPT_DIR/../bin/soak"
elif [[ -x "$PROJECT_DIR/target/release/examples/soak" ]]; then
    BIN="$PROJECT_DIR/target/release/examples/soak"
else
    cargo build --release --example soak 2>&1 | tail -3
    BIN="$PROJECT_DIR/target/release/examples/soak"
fi

echo "Starting soak test (binary: $BIN)..."
"$BIN" \
    --duration-secs "$DURATION" \
    --data-dir "$DATA_DIR" 2>&1 | tee "$LOG_FILE"

EXIT_CODE=$?

echo ""
echo "Soak test complete. Exit code: $EXIT_CODE"
echo "Log: $LOG_FILE"

exit $EXIT_CODE
