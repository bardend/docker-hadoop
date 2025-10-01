#!/bin/bash
set -e
NAMENODE_HOST="${NAMENODE_HOST:-localhost}"
NAMENODE_PORT="${NAMENODE_PORT:-9000}"
export NAMENODE_HOST NAMENODE_PORT

echo "   NameNode Host: $NAMENODE_HOST"
echo "   NameNode Port: $NAMENODE_PORT"
echo "   Data Directory: /hadoop/dfs/data"


sed -i "s/\${NAMENODE_HOST:-localhost}/$NAMENODE_HOST/g" $HADOOP_CONF_DIR/core-site.xml
sed -i "s/\${NAMENODE_PORT:-9000}/$NAMENODE_PORT/g" $HADOOP_CONF_DIR/core-site.xml

# Verificar que cambió
echo "=== core-site.xml después del reemplazo ==="
cat $HADOOP_CONF_DIR/core-site.xml

#exec $HADOOP_HOME/bin/hdfs datanode
$HADOOP_HOME/bin/hdfs datanode &
sleep infinity

