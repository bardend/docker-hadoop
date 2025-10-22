FROM openjdk:8-jdk-slim


ARG HADOOP_VERSION
ARG HADOOP_CONFIG_DIR
ARG HADOOP_HOME
ARG HADOOP_TEMPLATE_TMP

ARG SPARK_HOME
ARG SPARK_CONFIG_DIR
ARG SPARK_TEMPLATE_TMP

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    net-tools \
    gettext-base \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*


ENV HADOOP_VERSION=${HADOOP_VERSION}
ENV HADOOP_HOME=${HADOOP_HOME}
ENV HADOOP_MAPRED_HOME=${HADOOP_HOME}
ENV HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar 
ENV HADOOP_TEMPLATE_TMP=${HADOOP_TEMPLATE_TMP}
ENV HADOOP_CONFIG_DIR=${HADOOP_CONFIG_DIR}


ENV SPARK_HOME=${SPARK_HOME}
ENV SPARK_CONFIG_DIR=${SPARK_CONFIG_DIR}
ENV SPARK_TEMPLATE_TMP=${SPARK_TEMPLATE_TMP}
#This rute is from the image openjdk:8-jdk-slim
ENV PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
ENV MULTIHOMED_NETWORK=1

RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
    rm hadoop-${HADOOP_VERSION}.tar.gz


RUN ln -sf /opt/hadoop/etc/hadoop ${HADOOP_CONFIG_DIR} && \
    mkdir -p /opt/hadoop/logs /hadoop-data

COPY templates/ ${HADOOP_TEMPLATE_TMP}

RUN wget -q https://dlcdn.apache.org/spark/spark-3.5.7/spark-3.5.7-bin-hadoop3-scala2.13.tgz && \
    tar -xzf spark-3.5.7-bin-hadoop3-scala2.13.tgz && \
    mv spark-3.5.7-bin-hadoop3-scala2.13 ${SPARK_HOME} && \
    rm spark-3.5.7-bin-hadoop3-scala2.13.tgz
    
COPY template-spark/ ${SPARK_TEMPLATE_TMP}
COPY config-hadoop.sh /config-hadoop.sh

RUN chmod +x config-hadoop.sh
ENTRYPOINT ["/config-hadoop.sh"]
#Comenta esto para herencia de los hijos
CMD ["tail", "-f", "/dev/null"] 
