.PHONY: build

include ../.env
export

IMAGE_NAME ?= bardend123/hadoop-spark-worker
IMAGE_TAG ?= v2

build:
	docker build \
		--build-arg SPARK_WORKER_PORT=${SPARK_WORKER_PORT} \
		--build-arg SPARK_MASTER_PORT=${SPARK_MASTER_PORT} \
		--build-arg SPARK_MASTER_WEBUI_PORT=${SPARK_MASTER_WEBUI_PORT} \
		--build-arg SPARK_MASTER_HOST=${SPARK_MASTER_HOST} \
		--build-arg SPARK_HOME=${SPARK_HOME} \
		--build-arg MASTER_HOST=${MASTER_HOST} \
		--build-arg DIR_VOL_DATANODE=${DIR_VOL_DATANODE} \
		-t ${IMAGE_NAME}:${IMAGE_TAG} .
	@echo "✓ Image built: ${IMAGE_NAME}:${IMAGE_TAG}"