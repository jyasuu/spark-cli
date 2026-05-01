#!/bin/bash
# start-with-livy.sh
#
# Starts Apache Livy in the background, waits for it to be ready,
# then exec's the original tabulario/spark-iceberg start script so
# Spark master/worker, Thrift Server, history server, and Jupyter
# all start exactly as they would without this wrapper.

set -euo pipefail

LIVY_HOME=${LIVY_HOME:-/opt/livy}

# ── Find the original spark-iceberg start script ──────────────────────────────
# tabulario/spark-iceberg uses /bin/start-spark.sh as its CMD.
# We exec it at the end so this process becomes Spark (PID inherits).
SPARK_START=/bin/start-spark.sh
if [ ! -x "$SPARK_START" ]; then
  echo "[livy] ERROR: cannot find base start script at $SPARK_START"
  echo "[livy] Available scripts in /bin:"
  ls /bin/start-*.sh 2>/dev/null || true
  exit 1
fi

# ── Start Livy ────────────────────────────────────────────────────────────────
echo "[livy] Starting Livy server..."
${LIVY_HOME}/bin/livy-server start

# ── Wait for Livy HTTP port ───────────────────────────────────────────────────
echo "[livy] Waiting for Livy on :8998 (up to 60s)..."
for i in $(seq 1 60); do
  if curl -sf http://localhost:8998/batches > /dev/null 2>&1; then
    echo "[livy] Ready after ${i}s"
    break
  fi
  if [ "$i" -eq 60 ]; then
    echo "[livy] ERROR: Livy did not become ready in 60s"
    echo "[livy] --- Livy logs ---"
    find /tmp /var/log -name "livy*.log" 2>/dev/null | head -3 | xargs cat 2>/dev/null || true
    cat ${LIVY_HOME}/logs/*.out 2>/dev/null | tail -30 || true
    exit 1
  fi
  sleep 1
done

# ── Hand off to Spark ─────────────────────────────────────────────────────────
echo "[livy] Handing off to $SPARK_START ..."
exec "$SPARK_START"
