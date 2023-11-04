#!/usr/bin/env bash
HADOOP_HOME=/export/servers/hadoop
SPARK_HOME=/export/servers/spark-2.2.0-bin-2.6.0-cdh5.14.0
HIVE_HOME=/export/servers/hive
OOZIE_HOME=/export/servers/oozie
HUE_HOME=/export/servers/hue

# 1.Start HDFS
echo "Starting HDFS Daemons...................................."
${HADOOP_HOME}/sbin/hadoop-daemon.sh start namenode
${HADOOP_HOME}/sbin/hadoop-daemon.sh start datanode

# 2.Start YARN
echo "Starting YARN Daemons...................................."
${HADOOP_HOME}/sbin/yarn-daemon.sh start resourcemanager
${HADOOP_HOME}/sbin/yarn-daemon.sh start nodemanager

# 3.Start MRHistoryServer
echo "Starting MapReduce HistoryServer...................................."
${HADOOP_HOME}/sbin/mr-jobhistory-daemon.sh start historyserver

# 4.Start Spark HistoryServer
echo "Starting Spark HistoryServer...................................."
${SPARK_HOME}/sbin/start-history-server.sh

# 5.Start Hive Service
DATE_STR=`/bin/date '+%Y%m%d%H%M%S'`
echo "Starting Hive MetaStore...................................."
HIVE_METASOTRE_LOG=${HIVE_HOME}/logs/hivemetastore-${DATE_STR}.log
/usr/bin/nohup ${HIVE_HOME}/bin/hive --service metastore >
${HIVE_METASOTRE_LOG} 2>&1 &
echo "Starting Hive Server2...................................."
HIVE_SERVER2_LOG=${HIVE_HOME}/logs/hiveserver2-${DATE_STR}.log
/usr/bin/nohup ${HIVE_HOME}/bin/hiveserver2 > ${HIVE_SERVER2_LOG} 2>&1 &

# 6. Start Oozie
echo "Starting Oozie Daemons...................................."
${OOZIE_HOME}/bin/oozie-start.sh

# 7. Start Hue
echo "Starting Hue Daemons...................................."
HUE_LOG=${HUE_HOME}/hue-${DATE_STR}.log
/usr/bin/nohup ${HUE_HOME}/build/env/bin/supervisor > ${HUE_LOG} 2>&1 &

# 1. Start Zookeeper
echo "Starting Zookeeper Daemons...................................."
ZOOKEEPER_HOME=/export/servers/zookeeper
cd ${ZOOKEEPER_HOME}
source /etc/profile
${ZOOKEEPER_HOME}/bin/zkServer.sh start
# 2. Start HBase Daemons
echo "Starting HBase Daemons...................................."
HBASE_HOME=/export/servers/hbase
${HBASE_HOME}/bin/hbase-daemon.sh start master
${HBASE_HOME}/bin/hbase-daemon.sh start regionserver