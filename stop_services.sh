#!/usr/bin/env bash
# ==============================================================================
# Helper Script: Gracefully Stop Hadoop HDFS and YARN Daemons
# ==============================================================================

echo ">>> Stopping Hadoop YARN Cluster Resource Manager..."
stop-yarn.sh || true

echo ">>> Stopping Hadoop Distributed File System (HDFS)..."
stop-dfs.sh || true

echo ""
echo "=========================================================="
echo "Hadoop daemons stopped successfully."
echo "Current Java processes (jps):"
jps
echo "=========================================================="
