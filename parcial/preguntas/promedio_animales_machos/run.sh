#!/bin/bash

# Script para compilar y ejecutar PromedioAnimalesMachos

JAVA_FILE="PromedioAnimalesMachos.java"
JAR_FILE="PromedioAnimalesMachos.jar"

echo "Compilando PromedioAnimalesMachos..."

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
hdfs dfs -rm -r -f /output/PromedioAnimalesMachos

echo "Directorio de salida limpiado"

# 4. Ejecutar el job de Hadoop
echo "Ejecutando job de Hadoop..."
hadoop jar "$JAR_FILE" PromedioAnimalesMachos /input/dataset_ganado_cleaned.csv /output/PromedioAnimalesMachos

if [ $? -eq 0 ]; then
    echo "Job ejecutado exitosamente"
    echo "Resultados:"
    hdfs dfs -cat /output/PromedioAnimalesMachos/part-r-00000
else
    echo "Error ejecutando el job"
    exit 1
fi
