#!/bin/bash

# Script para compilar y ejecutar ConsultaPorcinoChulucanas

JAVA_FILE="ConsultaPorcinoChulucanas.java"
JAR_FILE="ConsultaPorcinoChulucanas.jar"

echo "Compilando ConsultaPorcinoChulucanas..."

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

# 3. Borrar directorios de salida y temporales
hdfs dfs -rm -r -f /output/ConsultaPorcinoChulucanas
hdfs dfs -rm -r -f /temp/porcino
hdfs dfs -rm -r -f /temp/chulucanas

echo "Directorios limpiados"

# 4. Ejecutar el job de Hadoop
echo "Ejecutando job de Hadoop con 3 MapReduce anidados..."
hadoop jar "$JAR_FILE" ConsultaPorcinoChulucanas /input/dataset_ganado_cleaned.csv /output/ConsultaPorcinoChulucanas

if [ $? -eq 0 ]; then
    echo "Job ejecutado exitosamente"
    echo "Resultados:"
    hdfs dfs -cat /output/ConsultaPorcinoChulucanas/part-r-00000
    echo ""
    echo "Limpiando archivos temporales..."
    hdfs dfs -rm -r -f /temp/porcino
    hdfs dfs -rm -r -f /temp/chulucanas
else
    echo "Error ejecutando el job"
    exit 1
fi
