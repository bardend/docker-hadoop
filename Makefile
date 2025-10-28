# Cargar variables del archivo .env
include .env
export $(shell sed 's/=.*//' .env)

.DEFAULT_GOAL := help


help:
	@echo "  make wordcount  - Ejecutar el job en los contenedores de pyspark"
	@echo "  make down       - Detener los contenedores"



wordcount:
	docker compose -f docker-compose-hadoop-spark.yml up -d
	sleep 20
	docker cp ./submit-pyspark/wordcount.py ${MASTER_HOST}:${SPARK_HOME}/wordcount.py
	docker cp ./submit-pyspark/data/string.txt ${MASTER_HOST}:${HADOOP_HOME}/string.txt
	docker exec ${MASTER_HOST} hdfs dfs -put -f ${HADOOP_HOME}/string.txt /string.txt
	docker exec ${MASTER_HOST} ${SPARK_HOME}/bin/spark-submit --master spark://${MASTER_HOST}:${SPARK_MASTER_PORT} ${SPARK_HOME}/wordcount.py

down:
	docker compose -f docker-compose-hadoop-spark.yml down