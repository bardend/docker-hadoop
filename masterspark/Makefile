include ../.env
export

# Nombre de la imagen
IMAGE_NAME=bardend123/spark-master
IMAGE_TAG=v2

.PHONY: build clean help


build:
	docker build \
	--build-arg SPARK_MASTER_HOST=$(SPARK_MASTER_HOST) \
	--build-arg SPARK_MASTER_PORT=$(SPARK_MASTER_PORT) \
	--build-arg SPARK_MASTER_WEBUI_PORT=$(SPARK_MASTER_WEBUI_PORT) \
	--build-arg SPARK_HOME=$(SPARK_HOME) \
	-t $(IMAGE_NAME):$(IMAGE_TAG) .


run:
	docker run -d --name spark $(IMAGE_NAME):$(IMAGE_TAG)
clear:
	docker rm -f spark

