include ../.env
export

# Nombre de la imagen
IMAGE_NAME=bardend123/hdfs-hadoop-spark-base
IMAGE_TAG=v2

.PHONY: build clean help


build:
	docker build \
	--build-arg HADOOP_VERSION=$(HADOOP_VERSION) \
	--build-arg HADOOP_HOME=$(HADOOP_HOME) \
	--build-arg HADOOP_CONFIG_DIR=$(HADOOP_CONFIG_DIR) \
	--build-arg HADOOP_TEMPLATE_TMP=$(HADOOP_TEMPLATE_TMP) \
	--build-arg SPARK_HOME=$(SPARK_HOME) \
	--build-arg SPARK_CONFIG_DIR=$(SPARK_CONFIG_DIR) \
	--build-arg SPARK_TEMPLATE_TMP=$(SPARK_TEMPLATE_TMP) \
	--build-arg SPARK_MASTER_HOST=$(SPARK_MASTER_HOST) \
	-t $(IMAGE_NAME):$(IMAGE_TAG) .
	@echo "✅ Imagen construida: $(IMAGE_NAME):$(IMAGE_TAG)"


run:
	docker run -d --name spark $(IMAGE_NAME):$(IMAGE_TAG)
clear:
	docker rm -f spark

