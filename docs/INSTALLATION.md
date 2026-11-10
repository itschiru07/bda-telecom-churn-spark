# Complete Installation & Setup Guide

This guide walks you through installing and configuring all required software components from scratch on a Windows machine using **WSL2 (Ubuntu 22.04)**.

---

## 1. System Requirements & Stack

| Item | Version | Purpose |
|---|---|---|
| **OS** | Windows 10 / 11 with WSL2 | Host environment |
| **Linux Distro** | Ubuntu 22.04 LTS | WSL environment hosting Hadoop, Spark, MongoDB |
| **Java** | OpenJDK 1.8 (Java 8) | Required runtime for Hadoop 3.3.6 and Spark 3.5.3 |
| **Hadoop** | 3.3.6 | HDFS (distributed storage) + YARN (resource management) |
| **Apache Spark** | 3.5.3 | Distributed analytics & MLlib machine learning |
| **MongoDB** | 8.0 (+ mongosh) | Real-time event streaming and analytical result store |
| **Python** | 3.10 | Pipeline scripts, PySpark, PyMongo, Streamlit |

---

## 2. Set Up WSL2 & Ubuntu 22.04

Open **PowerShell as Administrator**:
```powershell
# Install WSL2 with Ubuntu 22.04
wsl --install -d Ubuntu-22.04

# Set Ubuntu 22.04 as default
wsl --set-default Ubuntu-22.04
```
> If prompted, restart Windows. When Ubuntu opens for the first time, set your username and password.

All remaining commands run **inside Ubuntu 22.04 terminal** (`user@machine:~$`).

---

## 3. Install OpenJDK 8 and Essential Utilities

```bash
sudo apt update && sudo apt install -y \
  openjdk-8-jdk python3 python3-pip python3-venv \
  ssh pdsh wget curl unzip gnupg net-tools

# Ensure OpenJDK 8 is set as the default system Java:
sudo update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java

# Verify Java version (must output 1.8.0_xxx)
java -version
```

---

## 4. Configure Passwordless SSH (Required by Hadoop)

```bash
# Generate SSH key pair
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys

# Start SSH service and test connection
sudo service ssh start
ssh localhost exit   # Type 'yes' on the first prompt; must NOT ask for a password
```

---

## 5. Download & Extract Apache Hadoop 3.3.6

```bash
cd ~
wget -c https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz && mv hadoop-3.3.6 hadoop
rm -f hadoop-3.3.6.tar.gz
```

---

## 6. Configure Environment Variables

Add the required Hadoop, Spark, and Java paths to `~/.bashrc`:

```bash
cat >> ~/.bashrc <<'EOF'

# ===== BDA Project: Java, Hadoop & Spark Environment =====
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=$HOME/hadoop
export SPARK_HOME=$HOME/.local/lib/python3.10/site-packages/pyspark
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PYSPARK_PYTHON=python3
export SPARK_LOCAL_IP=127.0.0.1
export PDSH_RCMD_TYPE=ssh
export PATH="$HOME/.local/bin:$SPARK_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
EOF

source ~/.bashrc

# Verify Hadoop installation
hadoop version
```

---

## 7. Configure Hadoop Cluster XML Files (Pseudo-Distributed)

Set `JAVA_HOME` in Hadoop's environment config:
```bash
echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh
mkdir -p ~/hadoop_data
```

### `core-site.xml`
```bash
cat > $HADOOP_HOME/etc/hadoop/core-site.xml <<EOF
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>$HOME/hadoop_data</value>
  </property>
</configuration>
EOF
```

### `hdfs-site.xml`
```bash
cat > $HADOOP_HOME/etc/hadoop/hdfs-site.xml <<EOF
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
</configuration>
EOF
```

### `mapred-site.xml`
```bash
cat > $HADOOP_HOME/etc/hadoop/mapred-site.xml <<EOF
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <property>
    <name>yarn.app.mapreduce.am.env</name>
    <value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value>
  </property>
  <property>
    <name>mapreduce.map.env</name>
    <value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value>
  </property>
  <property>
    <name>mapreduce.reduce.env</name>
    <value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value>
  </property>
</configuration>
EOF
```

### `yarn-site.xml`
```bash
cat > $HADOOP_HOME/etc/hadoop/yarn-site.xml <<EOF
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
</configuration>
EOF
```

### Format HDFS & Start Services
```bash
# Format NameNode (Run ONCE only; never run again after data is loaded)
hdfs namenode -format

# Start HDFS and YARN daemons
start-dfs.sh && start-yarn.sh

# Verify all 5 Hadoop daemons are running:
jps
```
Expected `jps` output:
- `NameNode`
- `DataNode`
- `SecondaryNameNode`
- `ResourceManager`
- `NodeManager`

---

## 8. Install & Start MongoDB 8.0

```bash
# Import MongoDB official GPG key
curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc | \
  sudo gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg --dearmor

# Add MongoDB APT repository
echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/8.0 multiverse" | \
  sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list

# Install MongoDB
sudo apt update && sudo apt install -y mongodb-org

# Start and enable MongoDB service
sudo systemctl start mongod
sudo systemctl enable mongod

# Verify connection
mongosh --eval "db.runCommand({ping:1})"
# Output should contain: { ok: 1 }
```

---

## 9. Install Python Libraries & PySpark 3.5.3

The fastest and most reliable way to install Spark 3.5.3 with PySpark and `spark-submit` is directly via `pip`:

```bash
pip3 install --upgrade pip
pip3 install -r requirements.txt
```
*(If pip warns about externally-managed-environment, add `--break-system-packages`)*

Verify `spark-submit`:
```bash
spark-submit --version
```
Expected output:
```text
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.5.3
      /_/
Using Scala version 2.12.18, OpenJDK 64-Bit Server VM, 1.8.0_xxx
```

---

## 10. Clone the Repository & Setup Dataset

```bash
mkdir -p ~/bda && cd ~/bda
git clone <YOUR_GITHUB_REPO_URL> spark_telecom
cd spark_telecom

# Make scripts executable
chmod +x *.py run_pipeline.sh
mkdir -p results telecom_dataset
```

Place `telecom_churn.csv` inside `telecom_dataset/`:
```bash
# If downloaded to Windows Downloads folder:
cp /mnt/c/Users/<YourWindowsUsername>/Downloads/archive.zip telecom_dataset/
cd telecom_dataset && unzip archive.zip && cd ..

# Verify dataset has 3,334 lines (header + 3,333 customers)
wc -l telecom_dataset/telecom_churn.csv
```

---

## 11. Routine Startup (Whenever You Open Ubuntu)

Whenever you open a new WSL terminal to work on the project, ensure services are running:
```bash
sudo service ssh start
start-dfs.sh && start-yarn.sh
sudo systemctl start mongod
jps
```

---

## Troubleshooting Guide

| Issue | Root Cause | Solution |
|---|---|---|
| `spark-submit: command not found` | `pyspark` not installed or PATH missing `~/.local/bin` | Run `pip3 install pyspark==3.5.3 py4j==0.10.9.7` and verify `~/.bashrc` has `$HOME/.local/bin` in `PATH`. |
| `UnsupportedClassVersionError` | Java 11/17/21 active instead of Java 8 | Run `sudo update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java`. |
| `jps` missing NameNode or DataNode | SSH service stopped or dirty shutdown | Run `stop-dfs.sh && sudo service ssh start && start-dfs.sh`. |
| `Name node is in safe mode` | HDFS checking block health during startup | Wait 30 seconds or force exit: `hdfs dfsadmin -safemode leave`. |
| `Connection refused localhost:9000` | HDFS NameNode is not running | Run `start-dfs.sh` and verify with `jps`. |
| `Could not find or load MRAppMaster` | YARN MapReduce environment variables missing | Ensure the 3 `*.env` properties are set in `mapred-site.xml`. |
| `pymongo Connection refused :27017` | MongoDB service stopped | Run `sudo systemctl start mongod`. |
