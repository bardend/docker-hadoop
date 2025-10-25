# Cargar variables de entorno
include ../.env
export

# Nombre de la imagen
IMAGE_NAME := bardend123/hadoop-spark-master
IMAGE_TAG := v2

build:
	@echo "Building Spark Master + HDFS NameNode image..."
	docker build \
		--build-arg SPARK_MASTER_PORT=$(SPARK_MASTER_PORT) \
		--build-arg SPARK_MASTER_WEBUI_PORT=$(SPARK_MASTER_WEBUI_PORT) \
		--build-arg SPARK_MASTER_HOST=$(SPARK_MASTER_HOST) \
		--build-arg SPARK_HOME=$(SPARK_HOME) \
		--build-arg DIR_VOL_NAMENODE=$(DIR_VOL_NAMENODE) \
		--build-arg MASTER_HOST=$(MASTER_HOST) \
		-t $(IMAGE_NAME):$(IMAGE_TAG) .
	@echo "✓ Image built: $(IMAGE_NAME):$(IMAGE_TAG)"
