#!/bin/bash
# Script para compilar y ejecutar RegresionLinealCluster en Hadoop

JAVA_FILE="RegresionLinealCluster.java"
JAR_FILE="RegresionLinealCluster.jar"
MAIN_CLASS="RegresionLinealCluster"

# Rutas en HDFS
INPUT_PATH="/input/dataset_ganado_cleaned.csv"
OUTPUT_PATH="/output/regresion_lineal"

echo "==================== COMPILANDO ===================="

# 1. Limpiar compilaciones anteriores
rm -f *.class *.jar

# 2. Compilar el archivo Java con las librerías de Hadoop
hadoop com.sun.tools.javac.Main $JAVA_FILE

if [ $? -ne 0 ]; then
    echo "Error en la compilación"
    exit 1
fi
echo "Compilación exitosa"

# 3. Crear el archivo JAR
jar cf "$JAR_FILE" *.class

if [ $? -ne 0 ]; then
    echo "Error creando el JAR"
    exit 1
fi
echo "JAR creado exitosamente: $JAR_FILE"

# 4. Limpiar directorios antiguos en HDFS
echo "Limpiando rutas HDFS anteriores..."
hdfs dfs -rm -r -f $OUTPUT_PATH 2>/dev/null
hdfs dfs -rm -r -f /temp 2>/dev/null

# 5. Verificar existencia del dataset
echo "Verificando dataset en HDFS..."
hdfs dfs -test -e $INPUT_PATH
if [ $? -ne 0 ]; then
    echo "No se encontró $INPUT_PATH"
    echo "Sube el archivo con: hdfs dfs -put dataset_ganado_0725_03.csv /input/"
    exit 1
fi
echo "Dataset encontrado en $INPUT_PATH"

# 6. Ejecutar el job y medir tiempo de ejecución
echo "Ejecutando RegresionLinealCluster con 3 MapReduce anidados..."
start_time=$(date +%s)

hadoop jar "$JAR_FILE" "$MAIN_CLASS" "$INPUT_PATH" "$OUTPUT_PATH"

exit_code=$?
if [ $exit_code -eq 0 ]; then
    end_time=$(date +%s)
    elapsed=$((end_time - start_time))
    echo "Job ejecutado exitosamente"
    echo "Tiempo total de ejecución: ${elapsed}s"
    echo "==================== RESULTADOS ===================="
    hdfs dfs -cat $OUTPUT_PATH/part-r-00000 | head
    echo "====================================================="
else
    echo "Error ejecutando el job (código: $exit_code)"
    echo "==================== LOGS ===================="
    # Intentar obtener logs de la aplicación
    APP_ID=$(yarn application -list -appStates FINISHED 2>/dev/null | grep "regresion-" | tail -1 | awk '{print $1}')
    if [ ! -z "$APP_ID" ]; then
        yarn logs -applicationId $APP_ID | tail -100
    fi
    exit 1
fi
