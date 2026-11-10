#!/usr/bin/env bash
# ==============================================================================
# Helper Script: Start all Big Data Services (SSH, HDFS, YARN, MongoDB)
# ==============================================================================

set -e

echo ">>> Starting SSH service..."
sudo service ssh start

echo ">>> Starting Hadoop Distributed File System (HDFS)..."
start-dfs.sh

echo ">>> Starting Hadoop YARN Cluster Resource Manager..."
start-yarn.sh

echo ">>> Starting MongoDB 8.0 Daemon..."
sudo systemctl start mongod

echo ""
echo "=========================================================="
echo "                   ALL SERVICES STARTED                   "
echo "=========================================================="
echo "Active Java Daemons (jps):"
jps
echo ""
echo "MongoDB Status:"
mongosh --quiet --eval "db.runCommand({ping:1})" && echo "MongoDB is responding properly."
echo ""
echo "Available Web Interfaces:"
echo " - HDFS NameNode Web UI:       http://localhost:9870"
echo " - YARN Resource Manager UI:   http://localhost:8088"
echo " - Streamlit Analytics UI:     http://localhost:8502"
echo "=========================================================="
