#!/bin/bash
# Configurations of .xml on /etc/hadoop for dfs,yarn,map,...
set -e
TEMPLATE_DIR="/opt/hadoop-templates"
HADOOP_XML_DIR="/etc/hadoop"

process_template() {
    local template_file=$1
    local output_file=$2

    echo "Procesando→ $output_file" 

    if [[ -f "$template_file" ]]; then
        envsubst < "$template_file" > "$output_file"
    fi
}

for template in "$TEMPLATE_DIR"/*.template; do
    file=$(basename "$template")
    file_name="${file%.*}"
    process_template "$template" "$HADOOP_XML_DIR/$file_name"
done

exec "$@"