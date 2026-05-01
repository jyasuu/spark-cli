#!/usr/bin/env bash
# scripts/test-integration.sh
#
# Bring up the full docker-compose stack, wait for services to be healthy,
# run unit + integration tests, then tear down.
#
# Usage:
#   ./scripts/test-integration.sh                  # run everything
#   ./scripts/test-integration.sh --keep-up        # leave stack running after tests
#   ./scripts/test-integration.sh --unit-only       # unit tests only (no docker)
#   FILTER=iceberg ./scripts/test-integration.sh   # run only tests matching "iceberg"

set -euo pipefail

# ── colour helpers ──────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info()  { echo -e "${GREEN}[info]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[warn]${NC}  $*"; }
error() { echo -e "${RED}[error]${NC} $*" >&2; }

KEEP_UP=false
UNIT_ONLY=false
TEST_FILTER="${FILTER:-}"

for arg in "$@"; do
  case $arg in
    --keep-up)    KEEP_UP=true ;;
    --unit-only)  UNIT_ONLY=true ;;
    *) warn "unknown argument: $arg" ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

# ── unit tests (always fast, no docker) ────────────────────────────────────
info "Running unit tests (no docker required)..."
if [ -n "$TEST_FILTER" ]; then
  cargo test --lib --bins -- "$TEST_FILTER" --nocapture
else
  cargo test --lib --bins
fi
info "Unit tests passed."

if $UNIT_ONLY; then
  info "Unit-only mode — skipping integration stack."
  exit 0
fi

# ── docker-compose stack ────────────────────────────────────────────────────
cleanup() {
  if ! $KEEP_UP; then
    info "Tearing down docker-compose stack..."
    docker compose down -v 2>/dev/null || true
  else
    info "Stack left running (--keep-up). Stop with: docker compose down -v"
  fi
}
trap cleanup EXIT

info "Starting docker-compose services..."
docker compose up -d --wait

# ── upload PySpark scripts to MinIO ────────────────────────────────────────
# Required by curate_submit_reaches_success_state (batch job test).
info "Uploading PySpark scripts to MinIO..."
docker exec mc mc cp /home/iceberg/scripts/gold_daily_revenue.py \
  minio/warehouse/scripts/gold_daily_revenue.py
docker exec mc mc cp /home/iceberg/scripts/silver_orders.py \
  minio/warehouse/scripts/silver_orders.py
info "Scripts ready in MinIO."

# ── wait for iceberg-init ──────────────────────────────────────────────────
info "Waiting for iceberg-init seed container..."
for i in $(seq 1 36); do  # 36 × 5s = 3 minutes
  STATUS=$(docker inspect --format='{{.State.Status}}' iceberg-init 2>/dev/null || echo "missing")
  if [ "$STATUS" = "exited" ]; then
    EXIT_CODE=$(docker inspect --format='{{.State.ExitCode}}' iceberg-init)
    if [ "$EXIT_CODE" = "0" ]; then
      info "iceberg-init seed completed."
      break
    else
      error "iceberg-init exited with code $EXIT_CODE"
      docker logs iceberg-init
      exit 1
    fi
  fi
  printf '.'
  sleep 5
done
echo ""

# ── show quick status ───────────────────────────────────────────────────────
info "Service status:"
docker compose ps --format "table {{.Name}}\t{{.Status}}"

info "Iceberg namespaces:"
curl -s http://localhost:8181/v1/namespaces | python3 -m json.tool 2>/dev/null || true

# ── run integration tests ───────────────────────────────────────────────────
info "Running integration tests (feature = integration)..."
export SPARK_CTRL_INTEGRATION=1
export LIVY_URL=http://localhost:8998
export ICEBERG_URL=http://localhost:8181
export MINIO_URL=http://localhost:9000
export POSTGRES_DSN="host=localhost port=5432 dbname=shop user=app password=secret sslmode=disable"
export SPARK_THRIFT=localhost:10000

if [ -n "$TEST_FILTER" ]; then
  cargo test --features integration -- "$TEST_FILTER" --nocapture
else
  cargo test --features integration -- --nocapture
fi

info "All tests passed!"