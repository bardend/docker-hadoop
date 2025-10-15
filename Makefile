DOCKER_NETWORK = hadoop-network
HADOOP_HOME = /opt/hadoop
WORK_PATH = $(HADOOP_HOME)/java-files/wordcount
CLASS_JAVA = ContarPalabras

wordcount:
	docker compose -f docker-compose-hadoop.yml up -d
	sleep 20
	docker exec master bash -c "hdfs dfsadmin -safemode leave"
	docker exec master mkdir -p $(WORK_PATH)/src
	docker exec master mkdir -p $(WORK_PATH)/input
	docker cp ./submit/src/. master:$(WORK_PATH)/src/
	docker cp ./submit/input/. master:$(WORK_PATH)/input/
	docker cp ./submit/start.sh master:$(WORK_PATH)/
	docker exec master bash -c "hdfs dfs -mkdir -p /input"
	docker exec master bash -c "hdfs dfs -put -f $(WORK_PATH)/input/in.txt /input/contar-palabras"
	docker exec master bash -c "cd $(WORK_PATH) && chmod +x start.sh &&
							    ./start.sh ./src/$(CLASS_JAVA) /input/contar-palabras /output/contar-palabras"
down:
	docker compose -f docker-compose-hadoop.yml down
stop:
	docker compose -f docker-compose-hadoop.yml stop
restart:
	docker compose -f docker-compose-hadoop.yml restart
run:
	docker exec master bash -c "hdfs dfsadmin -safemode leave"
	docker exec master mkdir -p $(WORK_PATH)/src
	docker exec master mkdir -p $(WORK_PATH)/input
	docker cp ./submit/src/. master:$(WORK_PATH)/src/
	docker cp ./submit/input/. master:$(WORK_PATH)/input/
	docker cp ./submit/start.sh master:$(WORK_PATH)/
	docker exec master bash -c "hdfs dfs -mkdir -p /input"
	docker exec master bash -c "hdfs dfs -put -f $(WORK_PATH)/input/in.txt /input/contar-palabras"
	docker exec master bash -c "cd $(WORK_PATH) && chmod +x start.sh && \
							    ./start.sh ./src/$(CLASS_JAVA) /input/contar-palabras /output/contar-palabras"