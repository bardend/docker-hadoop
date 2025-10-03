set -e

WORKER_DIR="${HADOOP_CONF_DIR}/workers"
rm -f "$WORKER_DIR"
touch "$WORKER_DIR"

$HADOOP_HOME/bin/hdfs datanode &
$HADOOP_HOME/bin/yarn nodemanager &

sleep infinity