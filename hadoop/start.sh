#!/usr/bin/env bash
set -euo pipefail
cp /workspace/hadoop/conf/*.xml "$HADOOP_CONF_DIR/"
mkdir -p /hdfs/name /hdfs/data /hdfs/tmp /workspace/reports
export HADOOP_HEAPSIZE=256
export HDFS_NAMENODE_USER=root HDFS_DATANODE_USER=root
if [ ! -d /hdfs/name/current ]; then
  hdfs namenode -format -force -nonInteractive
fi
hdfs --daemon start namenode
hdfs --daemon start datanode
for attempt in $(seq 1 60); do
  if hdfs dfs -mkdir -p /banking/raw; then break; fi
  sleep 2
done
hdfs dfsadmin -safemode wait
export HADOOP_CLIENT_OPTS='-Xmx512m -Dhive.root.logger=WARN,console'
export HADOOP_OPTS='-Xmx512m'
cat > /opt/hive/conf/hive-site.xml <<'XML'
<?xml version="1.0"?>
<configuration>
  <property><name>javax.jdo.option.ConnectionURL</name><value>jdbc:derby:;databaseName=/hdfs/metastore_db;create=true</value></property>
  <property><name>javax.jdo.option.ConnectionDriverName</name><value>org.apache.derby.jdbc.EmbeddedDriver</value></property>
  <property><name>hive.metastore.warehouse.dir</name><value>hdfs://hadoop-hive:9000/user/hive/warehouse</value></property>
  <property><name>hive.execution.engine</name><value>mr</value></property>
  <property><name>hive.exec.mode.local.auto</name><value>true</value></property>
  <property><name>hive.server2.enable.doAs</name><value>false</value></property>
  <property><name>hive.server2.thrift.bind.host</name><value>0.0.0.0</value></property>
  <property><name>hive.server2.thrift.port</name><value>10000</value></property>
</configuration>
XML
if [ ! -d /hdfs/metastore_db ]; then
  /opt/hive/bin/schematool -dbType derby -initSchema --verbose
fi
# The container has a fresh process namespace after restart; an old PID 1 marker is stale.
rm -f /opt/hive/conf/hiveserver2.pid
exec /opt/hive/bin/hiveserver2
