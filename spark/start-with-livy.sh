#!/bin/bash
# start-with-livy.sh
#
# Starts Apache Livy in the background, then exec's the original
# tabulario/spark-iceberg entrypoint so all existing Spark/notebook
# functionality is preserved unchanged.

set -euo pipefail

LIVY_HOME=${LIVY_HOME:-/opt/livy}
LIVY_LOG=/tmp/livy.log

echo "[livy] Starting Livy server (log: ${LIVY_LOG})..."
${LIVY_HOME}/bin/livy-server start > "${LIVY_LOG}" 2>&1 &

# Wait up to 60 s for Livy's HTTP port to accept connections
echo "[livy] Waiting for Livy on :8998..."
for i in $(seq 1 60); do
  if curl -sf http://localhost:8998/batches > /dev/null 2>&1; then
    echo "[livy] Livy ready after ${i}s"
    break
  fi
  sleep 1
done

# Hand off to the original entrypoint CMD
# tabulario/spark-iceberg uses /bin/start-spark.sh as its CMD
exec /bin/start-spark.sh
