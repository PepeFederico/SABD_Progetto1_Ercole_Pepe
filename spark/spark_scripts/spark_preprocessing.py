from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, coalesce, lit
from pyspark.sql.functions import input_file_name, regexp_extract, col, when

# 1. Inizializza la SparkSession
spark = SparkSession.builder \
    .appName("Pulizia_E_Conversione_Voli") \
    .getOrCreate()

# Percorsi HDFS
input_path = "hdfs://namenode:9000/data/nifi_output/*.avro"
output_path = "hdfs://namenode:9000/data/processed_data"


# 2. Lettura dei file Avro
print("Lettura dei file Avro in corso...")
df_globale = spark.read.format("avro").load(input_path)

# =============================================================
# 3. FASE DI PROCESSAMENTO E PULIZIA DATI
# =============================================================

# A) Estrazione del mese dal nome del file
df_elaborato = df_globale.withColumn(
    "mese",
    regexp_extract(input_file_name(), r"\d{4}(\d{2})_T_ONTIME_REPORTING", 1)
)

# 2. Lista delle colonne delle cause del ritardo
colonne_cause = [
    "CARRIER_DELAY",
    "WEATHER_DELAY",
    "NAS_DELAY",
    "SECURITY_DELAY",
    "LATE_AIRCRAFT_DELAY"
]

for c in colonne_cause:
    df_elaborato = df_elaborato.withColumn(
        c,
        when(col("ARR_DELAY") < 15, 0).otherwise(coalesce(col(c), lit(0)))
    )

# 4. Scrittura in formato Parquet pulito e partizionato
print("Scrittura in formato Parquet su HDFS...")
df_elaborato.write \
    .mode("overwrite") \
    .partitionBy("mese") \
    .parquet(output_path)

print("Processo completato! Dati pronti per le query.")