from pyspark.sql import SparkSession
import os

# Leer variables de entorno con valores por defecto optimizados
MASTER_HOST = os.getenv('MASTER_HOST', 'master')
SPARK_EXECUTOR_CORES = os.getenv('SPARK_EXECUTOR_CORES', '1')
SPARK_EXECUTOR_MEMORY = os.getenv('SPARK_EXECUTOR_MEMORY', '800m')  # Aumentado a 1GB
SPARK_DRIVER_MEMORY = os.getenv('SPARK_DRIVER_MEMORY', '800m')      # Aumentado a 1GB
SPARK_EXECUTOR_MEMORY_OVERHEAD = os.getenv('SPARK_EXECUTOR_MEMORY_OVERHEAD', '200m')  # Aumentado

# DEBUG: Mostrar valores de las variables (opcional)
print("=" * 50)
print("🔍 CONFIGURACIÓN SPARK")
print("=" * 50)
print(f"MASTER_HOST: {MASTER_HOST}")
print(f"SPARK_EXECUTOR_CORES: {SPARK_EXECUTOR_CORES}")
print(f"SPARK_EXECUTOR_MEMORY: {SPARK_EXECUTOR_MEMORY}")
print(f"SPARK_DRIVER_MEMORY: {SPARK_DRIVER_MEMORY}")
print(f"SPARK_EXECUTOR_MEMORY_OVERHEAD: {SPARK_EXECUTOR_MEMORY_OVERHEAD}")
print("=" * 50)

# Configuración optimizada para cluster
spark = SparkSession.builder \
    .appName("PythonWordCount") \
    .master(f"spark://{MASTER_HOST}:7077") \
    .config("spark.executor.cores", SPARK_EXECUTOR_CORES) \
    .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY) \
    .config("spark.driver.memory", SPARK_DRIVER_MEMORY) \
    .config("spark.executor.memoryOverhead", SPARK_EXECUTOR_MEMORY_OVERHEAD) \
    .getOrCreate()

# Configurar nivel de log
spark.sparkContext.setLogLevel("WARN")

hdfs_path = f"hdfs://{MASTER_HOST}:9000/string.txt"

print("Leyendo archivo desde HDFS...")
lines = spark.read.text(hdfs_path).rdd.map(lambda r: r[0])

print("Procesando word count distribuido...")
word_counts = lines.flatMap(lambda x: x.split(' ')) \
                   .filter(lambda x: x.strip() != '') \
                   .map(lambda x: (x.lower(), 1)) \
                   .reduceByKey(lambda x, y: x + y) \
                   .sortBy(lambda x: x[1], ascending=False)

print("\nResultados del Word Count:")
print("=" * 50)
for (word, count) in word_counts.collect():
    print(f"{word:.<30} {count}")
print("=" * 50)

# Mostrar información del cluster usado
executor_count = spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size()
print(f"\nExecutors usados: {executor_count}")

print("\n✅ Job completado exitosamente!")
spark.stop()