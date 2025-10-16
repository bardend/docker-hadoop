#!/bin/bash
# -----------------------------------------------------------
# Script de ejecución para la consulta de regresión lineal
# Consulta: Relación entre número de animales machos y cantidad de carne producida
# -----------------------------------------------------------

INPUT=/input/dataset_ganado_cleaned.csv
OUTPUT1=/output/regresion_sumatorias
OUTPUT2=/output/regresion_coeficientes
OUTPUT3=/output/regresion_predicciones
JAR_NAME=RegresionLinealMapReduce.jar
CLASS_NAME=RegresionLinealMapReduce

# Limpiar salidas anteriores
hdfs dfs -rm -r -f $OUTPUT1
hdfs dfs -rm -r -f $OUTPUT2
hdfs dfs -rm -r -f $OUTPUT3

echo "==================== COMPILANDO ===================="
hadoop com.sun.tools.javac.Main $CLASS_NAME.java

echo "==================== CREANDO JAR ===================="
jar cf $JAR_NAME $CLASS_NAME*.class

# Inicia el cronometro
start=$(date +%s)

echo "==================== EJECUTANDO JOB ===================="
hadoop jar $JAR_NAME $CLASS_NAME $INPUT $OUTPUT1 $OUTPUT2 $OUTPUT3

# Finalizar el cronometro
end=$(date +%s)
runtime=$((end-start))

echo "==================== RESULTADOS ===================="
echo "Coeficientes de regresión:"
hdfs dfs -cat $OUTPUT2/part-r-00000

echo ""
echo "Predicciones generadas (x, y_real, y_predicha):"
hdfs dfs -cat $OUTPUT3/part-r-00000 | head -n 10

echo ""
echo "Tiempo total de ejecución: ${runtime} segundos"
echo ""
echo "Consulta de regresión ejecutada correctamente."
echo "Archivos de salida:"
echo "  - Sumatorias:      $OUTPUT1"
echo "  - Coeficientes:    $OUTPUT2"
echo "  - Predicciones:    $OUTPUT3"
