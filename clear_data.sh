#!/usr/bin/env bash
# ==============================================================================
# Helper Script: Reset & Clear Simulation Data from MongoDB, HDFS, and Local Cache
# ==============================================================================

set -e
cd "$(dirname "$0")"

echo ">>> Dropping MongoDB 'telecom' database..."
mongosh telecom --quiet --eval "db.dropDatabase()" || true

echo ">>> Removing telecom directory from HDFS (/bda/telecom)..."
hdfs dfs -rm -r -f /bda/telecom || true

echo ">>> Cleaning local exports, cache, and logs..."
rm -rf exports last_export.txt results/spark_*.txt spark_errors.log spark-warehouse __pycache__
mkdir -p results

echo ""
echo "=========================================================="
echo "Reset complete. Ready for a brand new simulation cycle!"
echo "=========================================================="
