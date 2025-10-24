#!/bin/bash

set -e

echo "=== Rebuilding Hadoop Docker Images ==="

IMAGES=(
    #"bardend123/hdfs-hadoop-spark-base:v2"
    "bardend123/hadoop-datanode:v2"
    "bardend123/hadoop-namenode:v2"
    #"bardend123/hadoop-nodemanager:v1"
    #"bardend123/hadoop-resourcemanager:v1"
    #"bardend123/hadoop-master:v1"
    #"bardend123/hadoop-worker:v1"
)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

for image in "${IMAGES[@]}"; do
    dir_name=$(echo "$image" | sed 's/.*-\([^:]*\).*/\1/')
    dir_path="$SCRIPT_DIR/$dir_name"
    
    echo "Building $image from: $dir_path"
    
    if [ -d "$dir_path" ]; then
        docker build -t "$image" "$dir_path"
        docker push "$image"
        echo "✓ $image completed"
    else
        echo "❌ Directory not found: $dir_path"
    fi
done

