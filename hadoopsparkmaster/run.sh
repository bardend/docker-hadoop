#!/bin/bash
#This part is for namenode
set -e
if [ -z "$MASTER_HOST" ]; then
    echo "We have created a bridge network to detect resolution dns, so set the CLUSTER_NAME :)"
    exit 2
fi

NAME_DIR="${HDFS_CONF_NAMENODE_DIR}"
NAME_DIR="${NAME_DIR#file://}"


ls -la "$NAME_DIR"
rm -rf $NAME_DIR/lost+found

if [ -z "$(ls -A "$NAME_DIR" 2>/dev/null)" ]; then 
    echo "Formatting NameNode directory: $NAME_DIR"
    "$HADOOP_HOME/bin/hdfs" namenode -format "$MASTER_HOST" -force -nonInteractive
fi

"${HADOOP_HOME}/bin/hdfs" namenode &
NAMENODE_PID=$!


#This part is for master spark
. "${SPARK_HOME}/sbin/spark-config.sh"
. "${SPARK_HOME}/bin/load-spark-env.sh"
mkdir -p ${SPARK_MASTER_LOG}

ln -sf /dev/stdout ${SPARK_MASTER_LOG}/spark-master.out

exec ${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.master.Master \
    --ip ${SPARK_MASTER_HOST} \
    --port ${SPARK_MASTER_PORT} \
    --webui-port ${SPARK_MASTER_WEBUI_PORT}
