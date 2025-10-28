# Cargar variables del archivo .env
include .env
export $(shell sed 's/=.*//' .env)

wordcount:
	docker compose -f docker-compose-hadoop-spark.yml up -d
	sleep 20
	docker cp ./submit-pyspark/wordcount.py master:$(SPARK_HOME)/wordcount.py
	docker cp ./submit-pyspark/data/string.txt master:$(HADOOP_HOME)/string.txt
	docker exec master hdfs dfs -put -f $(HADOOP_HOME)/string.txt /string.txt
	docker exec master $(SPARK_HOME)/bin/spark-submit --master spark://master:7077 $(SPARK_HOME)/wordcount.py

down:
	docker compose -f docker-compose-hadoop-spark.yml down