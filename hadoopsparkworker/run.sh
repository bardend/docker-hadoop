set -e
#exec $HADOOP_HOME/bin/hdfs datanode
"$HADOOP_HOME/bin/hdfs" datanode &
DATANODE_PID=$!


. "${SPARK_HOME}/sbin/spark-config.sh"
. "${SPARK_HOME}/bin/load-spark-env.sh"

mkdir -p $SPARK_WORKER_LOG

ln -sf /dev/stdout $SPARK_WORKER_LOG/spark-worker.out

# Iniciar Spark Worker (foreground con exec)
exec "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.worker.Worker \
    --webui-port "${SPARK_WORKER_PORT}" \
    "spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT}"