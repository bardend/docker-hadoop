# Comprobamos que se pasen 3 argumentos
if [ "$#" -ne 3 ]; then
    echo "Uso: $0 <ArchivoJavaSinExtension> <InputHDFS> <OutputHDFS>"
    exit 1
fi

JAVA_FILE="$1"
CLASS_NAME=$(basename "$JAVA_FILE")
INPUT="$2"
OUTPUT="$3"
JAR_FILE="${CLASS_NAME}.jar"

# Guardar el directorio actual
CUR_DIR=$(pwd)

# Compilar el Java usando la ruta completa desde donde estás
echo hadoop com.sun.tools.javac.Main "$JAVA_FILE.java"
hadoop com.sun.tools.javac.Main "$JAVA_FILE.java"
if [ $? -ne 0 ]; then
    echo "Error en compilación"
    exit 2
fi

# Crear el jar desde el directorio raíz para mantener estructura de paquetes
echo jar cf "$JAR_FILE" -C . src/"${CLASS_NAME}"*.class
jar cf "$JAR_FILE" -C . src/"${CLASS_NAME}"*.class
if [ $? -ne 0 ]; then
    echo "Error al crear el jar"
    exit 3
fi

# Borrar output si existe
hdfs dfs -rm -r -f "$OUTPUT"

# Ejecutar el job en Hadoop con el nombre completo del paquete
echo hadoop jar "$JAR_FILE" src."$CLASS_NAME" "$INPUT" "$OUTPUT"
hadoop jar "$JAR_FILE" src."$CLASS_NAME" "$INPUT" "$OUTPUT"
if [ $? -ne 0 ]; then
    echo "Error al ejecutar el job en Hadoop"
    exit 4
fi

# Mostrar resultados
hdfs dfs -cat "$OUTPUT"/part-r-00000 | tail -20