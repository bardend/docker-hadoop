FROM bardend123/hadoop-base:v1

RUN mkdir -p /hadoop/dfs/name
ENV HDFS_CONF_dfs_namenode_name_dir=file:///hadoop/dfs/name
#This is volumen for metadata of namenode
VOLUME /hadoop/dfs/name

COPY run.sh /run.sh
RUN chmod +x /run.sh
EXPOSE 9870

CMD ["/run.sh"]