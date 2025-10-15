#!/bin/bash

# Script para compilar y ejecutar DesviacionAnimalesHembras

JAVA_FILE="DesviacionAnimalesHembras.java"
JAR_FILE="DesviacionAnimalesHembras.jar"

echo "Compilando DesviacionAnimalesHembras..."

# 1. Compilar el archivo .java usando hadoop com.sun.tools.javac.Main
hadoop com.sun.tools.javac.Main $JAVA_FILE

if [ $? -ne 0 ]; then
    echo "Error en la compilación"
    exit 1
fi

echo "Compilación exitosa"

# 2. Crear el JAR
jar cf "$JAR_FILE" "${JAVA_FILE%.java}"*.class

if [ $? -ne 0 ]; then
    echo "Error creando el JAR"
    exit 1
fi

echo "JAR creado exitosamente"

# 3. Borrar el directorio de salida si existe
hdfs dfs -rm -r -f /output/DesviacionAnimalesHembras

echo "Directorio de salida limpiado"

# 4. Ejecutar el job de Hadoop
echo "Ejecutando job de Hadoop..."
hadoop jar "$JAR_FILE" DesviacionAnimalesHembras /input/dataset_ganado_cleaned.csv /output/DesviacionAnimalesHembras

if [ $? -eq 0 ]; then
    echo "Job ejecutado exitosamente"
    echo "Resultados:"
    hdfs dfs -cat /output/DesviacionAnimalesHembras/part-r-00000
else
    echo "Error ejecutando el job"
    exit 1
fi
