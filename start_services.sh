#!/usr/bin/env bash
# ==============================================================================
# Helper Script: Start all Big Data Services (SSH, HDFS, YARN, MongoDB)
# ==============================================================================

echo ">>> Starting SSH service..."
sudo service ssh start || true

echo ">>> Starting Hadoop Distributed File System (HDFS)..."
nohup start-dfs.sh </dev/null >/tmp/hdfs-start.log 2>&1 || true

echo ">>> Starting Hadoop YARN Cluster Resource Manager..."
nohup start-yarn.sh </dev/null >/tmp/yarn-start.log 2>&1 || true
sleep 4

echo ">>> Starting MongoDB 8.0 Daemon..."
sudo systemctl start mongod || true

echo ""
echo "=========================================================="
echo "                   ALL SERVICES ACTIVE                    "
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
