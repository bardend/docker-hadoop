FROM openjdk:8-jdk-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    net-tools \
    gettext-base \
    && rm -rf /var/lib/apt/lists/* 

ENV HADOOP_VERSION=3.3.6

RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} /opt/hadoop && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

RUN ln -sf /opt/hadoop/etc/hadoop /etc/hadoop && \
    mkdir -p /opt/hadoop/logs /hadoop-data


ENV HADOOP_HOME=/opt/hadoop/
ENV HADOOP_CONF_DIR=/etc/hadoop 
ENV HADOOP_MAPRED_HOME=$HADOOP_HOME
ENV HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar
ENV PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
ENV MULTIHOMED_NETWORK=1
ENV USER=root

COPY templates/ /opt/hadoop-templates
COPY config-hadoop.sh /config-hadoop.sh
RUN chmod +x config-hadoop.sh
ENTRYPOINT ["/config-hadoop.sh"]
#CMD ["tail", "-f", "/dev/null"]
