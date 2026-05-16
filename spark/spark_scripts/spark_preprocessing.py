from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, coalesce, lit
from pyspark.sql.types import IntegerType

spark = SparkSession.builder \
    .appName("Pulizia_E_Conversione_Voli") \
    .getOrCreate()

input_path  = "hdfs://namenode:9000/data/nifi_output/*.avro"
output_path = "hdfs://namenode:9000/data/processed_data"

df = spark.read.format("avro").load(input_path)

# Cause del ritardo: azzera se ARR_DELAY < 15 (soglia FAA), null → 0
colonne_cause = [
    "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY",
    "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY"
]
for c in colonne_cause:
    df = df.withColumn(
        c,
        when(col("ARR_DELAY") < 15, 0).otherwise(coalesce(col(c), lit(0)))
    )

# Estrazione fascia oraria da CRS_DEP_TIME (formato HHMM → ora intera)
df = df.withColumn("hour", (col("CRS_DEP_TIME") / 100).cast(IntegerType()))

# Scrittura Parquet partizionato per mese
df.write \
    .mode("overwrite") \
    .partitionBy("MONTH") \
    .parquet(output_path)

print("Preprocessing completato.")