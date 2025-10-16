#!/bin/bash
# Script para compilar y ejecutar NaiveBayesGanado
# Autor: MapReduce Hadoop Naive Bayes Implementation
# Descripción: Implementa clasificación Naive Bayes usando MapReduce para dataset de ganado

set -e  # Salir si hay error

JAVA_FILE="NaiveBayesGanado.java"
JAR_FILE="NaiveBayesGanado.jar"
INPUT_PATH="/input/dataset_ganado_cleaned.csv"
OUTPUT_PATH="/output/NaiveBayesGanado"

echo "=========================================="
echo "Naive Bayes MapReduce - Ganado Dataset"
echo "=========================================="
echo ""

# 1. Compilar el archivo .java
echo "[1/6] Compilando $JAVA_FILE..."
hadoop com.sun.tools.javac.Main $JAVA_FILE
if [ $? -ne 0 ]; then
    echo "❌ Error en la compilación"
    exit 1
fi
echo "✓ Compilación exitosa"
echo ""

# 2. Crear el JAR con todas las clases
echo "[2/6] Creando el archivo JAR..."
jar cf "$JAR_FILE" "${JAVA_FILE%.java}"*.class
if [ $? -ne 0 ]; then
    echo "❌ Error creando el JAR"
    exit 1
fi
echo "✓ JAR creado: $JAR_FILE"
echo ""

# 3. Limpiar directorios de salida previos
echo "[3/6] Limpiando directorios de salida..."
hdfs dfs -rm -r -f "$OUTPUT_PATH"
hdfs dfs -rm -r -f /temp/nb_statistics
hdfs dfs -rm -r -f /temp/nb_predictions
echo "✓ Directorios limpiados"
echo ""

# 4. Verificar que el archivo de entrada existe
echo "[4/6] Verificando archivo de entrada..."
if ! hdfs dfs -test -f "$INPUT_PATH"; then
    echo "❌ Error: No se encuentra $INPUT_PATH"
    exit 1
fi
echo "✓ Archivo de entrada encontrado"
echo ""

# 5. Ejecutar el job de Hadoop
echo "[5/6] Ejecutando MapReduce job..."
echo "Input:  $INPUT_PATH"
echo "Output: $OUTPUT_PATH"
echo ""
hadoop jar "$JAR_FILE" NaiveBayesGanado "$INPUT_PATH" "$OUTPUT_PATH"

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ Job ejecutado exitosamente"
    echo ""
    
    # 6. Mostrar resultados
    echo "[6/6] Resultados - Estadísticas por Especie:"
    echo "=========================================="
    hdfs dfs -cat "$OUTPUT_PATH/statistics/part-r-00000"
    echo ""
    echo "=========================================="
    echo "Resultados - Predicciones por Departamento:"
    echo "=========================================="
    hdfs dfs -cat "$OUTPUT_PATH/predictions/part-r-00000"
    echo ""
    
    # Limpiar temporales
    echo "Limpiando archivos temporales..."
    hdfs dfs -rm -r -f /temp/nb_statistics
    hdfs dfs -rm -r -f /temp/nb_predictions
    echo ""
    echo "=========================================="
    echo "✓ Proceso completado exitosamente"
    echo "=========================================="
else
    echo ""
    echo "❌ Error ejecutando el job"
    exit 1
fi
