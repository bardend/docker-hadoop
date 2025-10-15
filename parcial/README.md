# Consultas MapReduce - Dataset Ganado

Este proyecto contiene 5 consultas MapReduce desarrolladas para analizar el dataset de ganado de Piura.

## Estructura del Proyecto

```
preguntas/
├── promedio_animales_machos/
│   ├── PromedioAnimalesMachos.java
│   └── run.sh
├── media_kg_carne_hembras/
│   ├── MediaKgCarneHembras.java
│   └── run.sh
├── desviacion_animales_hembras/
│   ├── DesviacionAnimalesHembras.java
│   └── run.sh
├── consulta_porcino_chulucanas/
│   ├── ConsultaPorcinoChulucanas.java
│   └── run.sh
├── consulta_ovinos_sechura/
│   ├── ConsultaOvinosSechura.java
│   └── run.sh
└── README.md
```

## Dataset

El dataset contiene información sobre ganado en Piura con las siguientes columnas:
- FECHA_DE_MUESTRA
- FECHA_DE_CORTE
- UBIGEO
- DEPARTAMENTO
- PROVINCIA
- DISTRITO
- ESPECIE_GANADERA (VACUNO, PORCINO, OVINOS, CAPRINOS)
- NUMERO_ANIMALES MACHOS
- CANTIDAD_KILOGRAMOS_CARNE_MACHOS
- NUMERO_ANIMALES HEMBRAS
- CANTIDAD_KILOGRAMOS_CARNE_HEMBRAS

## Consultas de Estadísticas

### 1. Promedio de Animales Machos
**Archivo:** `promedio_animales_machos/PromedioAnimalesMachos.java`
- **Descripción:** Calcula el promedio de animales machos en todo el dataset
- **Campo analizado:** NUMERO_ANIMALES MACHOS (columna 7)

### 2. Media de Kilogramos de Carne Hembras
**Archivo:** `media_kg_carne_hembras/MediaKgCarneHembras.java`
- **Descripción:** Calcula la media de kilogramos de carne de hembras
- **Campo analizado:** CANTIDAD_KILOGRAMOS_CARNE_HEMBRAS (columna 10)

### 3. Desviación Estándar de Animales Hembras
**Archivo:** `desviacion_animales_hembras/DesviacionAnimalesHembras.java`
- **Descripción:** Calcula la desviación estándar del número de animales hembras
- **Campo analizado:** NUMERO_ANIMALES HEMBRAS (columna 9)

## Consultas Complejas (3 MapReduce Anidados)

### 4. Consulta Porcino → Chulucanas → Total Animales Machos
**Archivo:** `consulta_porcino_chulucanas/ConsultaPorcinoChulucanas.java`
- **MapReduce 1:** Filtrar por ESPECIE_GANADERA = "PORCINO"
- **MapReduce 2:** Filtrar por DISTRITO = "CHULUCANAS"
- **MapReduce 3:** Sumar NUMERO_ANIMALES MACHOS
- **Resultado:** Total de animales machos de porcinos en Chulucanas

### 5. Consulta Ovinos → Sechura → Total Kg Carne Hembras
**Archivo:** `consulta_ovinos_sechura/ConsultaOvinosSechura.java`
- **MapReduce 1:** Filtrar por ESPECIE_GANADERA = "OVINOS"
- **MapReduce 2:** Filtrar por DISTRITO = "SECHURA"
- **MapReduce 3:** Sumar CANTIDAD_KILOGRAMOS_CARNE_HEMBRAS
- **Resultado:** Total de kilogramos de carne de hembras de ovinos en Sechura

## Cómo Ejecutar

Para cada consulta, navega a su carpeta correspondiente y ejecuta:

```bash
cd [nombre_carpeta]
./run.sh
```

### Requisitos
- Hadoop configurado y funcionando
- Dataset en HDFS en la ruta `/input/dataset_ganado_cleaned.csv`
- Variables de entorno de Hadoop configuradas (`$HADOOP_CLASSPATH`)

### Proceso de Ejecución
Cada script `run.sh` realiza:
1. Compilación del archivo .java
2. Creación del archivo JAR
3. Limpieza del directorio de salida anterior
4. Ejecución del job de Hadoop
5. Mostrado de resultados

### Rutas de Salida
- Consultas simples: `/output/[NombreClase]/`
- Consultas complejas: `/output/[NombreClase]/` (con archivos temporales en `/temp/`)

## Notas Técnicas

- Los archivos temporales de las consultas complejas se limpian automáticamente
- Todas las consultas manejan errores de formato de datos
- Los resultados se muestran en consola al finalizar la ejecución
- Cada consulta es independiente y puede ejecutarse por separado
