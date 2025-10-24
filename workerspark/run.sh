#! bin/bash
set -e
. "${SPARK_HOME}/sbin/spark-config.sh"
. "${SPARK_HOME}/bin/load-spark-env.sh"

mkdir -p $SPARK_WORKER_LOG

ln -sf /dev/stdout $SPARK_WORKER_LOG/spark-worker.out

exec ${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.worker.Worker \
    --webui-port ${SPARK_WORKER_PORT} \
    spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT}
#  --webui-port $SPARK_WORKER_WEBUI_PORT $SPARK_MASTER >> $SPARK_WORKER_LOG/spark-worker.out
