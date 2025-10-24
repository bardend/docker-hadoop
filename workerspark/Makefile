include ../.env
export

# Nombre de la imagen
IMAGE_NAME=bardend123/spark-worker
IMAGE_TAG=v2

.PHONY: build clean help


build:
	docker build \
	--build-arg SPARK_MASTER_HOST=$(SPARK_MASTER_HOST) \
	--build-arg SPARK_MASTER_PORT=$(SPARK_MASTER_PORT) \
	--build-arg SPARK_WORKER_PORT=$(SPARK_WORKER_PORT) \
	--build-arg SPARK_HOME=$(SPARK_HOME) \
	-t $(IMAGE_NAME):$(IMAGE_TAG) .
