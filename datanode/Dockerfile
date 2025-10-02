FROM bardend123/hadoop-base:v1

RUN mkdir -p /hadoop/dfs/data
ENV HDFS_CONF_dfs_datanode_data_dir=file:///hadoop/dfs/data
#This is volumen for data of datanode
VOLUME /hadoop/dfs/data

COPY run.sh /run.sh
RUN chmod +x /run.sh
EXPOSE 9864

CMD ["/run.sh"]