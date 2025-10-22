#!/bin/bash
# Configurations of .xml on /etc/hadoop for dfs,yarn,map,...
# Configurations of spark /spark/config/ for master, worker
set -e
TEMPLATE_DIRS=(
    "${SPARK_TEMPLATE_TMP}"
    "${HADOOP_TEMPLATE_TMP}"
)

OUTPUT_DIRS=(
    "${SPARK_CONFIG_DIR}"
    "${HADOOP_CONFIG_DIR}"
)

process_template() {
    local template_file=$1
    local output_file=$2
    perl -pe '
        s/\$\{(\w+):-([^}]+)\}/ 
            my $var = $1;
            my $default = $2;
            exists $ENV{$var} ? $ENV{$var} : $default;
        /gex;
    ' "$template_file" > "$output_file"
}
# Iterar por cada par de directorios
for i in "${!TEMPLATE_DIRS[@]}"; do
    TEMPLATE_DIR="${TEMPLATE_DIRS[$i]}"
    OUTPUT_DIR="${OUTPUT_DIRS[$i]}"
    
    echo "Procesing : ${TEMPLATE_DIR} -> ${OUTPUT_DIR}"
    
    # Procesar cada template en el directorio actual
    for template in "$TEMPLATE_DIR"/*.template; do
        file=$(basename "$template")
        file_name="${file%.*}"
        process_template "$template" "$OUTPUT_DIR/$file_name"
        echo "Procesing : ${file} -> {$OUTPUT_DIR/$file_name}"
    done
    echo "Final array"
done

echo "Final Deberia /bin/bash"
exec "$@"