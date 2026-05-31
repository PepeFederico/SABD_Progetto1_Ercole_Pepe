from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, coalesce, lit
from pyspark.sql.types import IntegerType

spark = SparkSession.builder \
    .appName("Pulizia_E_Conversione_Voli") \
    .getOrCreate()

input_path  = "hdfs://namenode:9000/data/nifi_output/*.avro"
output_path = "hdfs://namenode:9000/data/processed_data"

df = spark.read.format("avro").load(input_path)

# ==========================================
# 1. DROP RIGOROSO SUI CAMPI FONDAMENTALI
# ==========================================
# Se manca la compagnia, il mese, l'orario previsto o lo stato del volo,
# la riga è inutilizzabile per i nostri KPI. Drop diretto.
colonne_fondamentali = [
    "OP_UNIQUE_CARRIER", "MONTH", "CRS_DEP_TIME", "CANCELLED", "DIVERTED"
]
df_clean = df.dropna(subset=colonne_fondamentali)

# ==========================================
# 2. CONTROLLI DI COERENZA SUI RITARDI
# ==========================================

# Coerenza ARR_DELAY:
# Teniamo la riga SOLO SE l'arrivo c'è stato (non nullo)
# OPPURE è nullo ma è giustificato da una cancellazione o un dirottamento.
df_clean = df_clean.filter(
    col("ARR_DELAY").isNotNull() |
    (col("ARR_DELAY").isNull() & ((col("CANCELLED") == 1) | (col("DIVERTED") == 1)))
)

# Coerenza DEP_DELAY:
# Teniamo la riga SOLO SE la partenza c'è stata (non nulla)
# OPPURE è nulla ma è giustificata da una cancellazione (es. cancellato al gate prima del pushback).
# (Un volo dirottato ha per forza lasciato terra, quindi il suo DEP_DELAY non può essere nullo).
df_clean = df_clean.filter(
    col("DEP_DELAY").isNotNull() |
    (col("DEP_DELAY").isNull() & (col("CANCELLED") == 1))
)

# ==========================================
# 3. NORMALIZZAZIONE CAUSE
# ==========================================


# Gestione Cause di Ritardo FAA:
# Sotto i 15 minuti di ARR_DELAY, o se il volo non è arrivato (cancellato/dirottato),
# le cause non scattano. Forziamo a 0 per avere metriche pulite.
colonne_cause = [
    "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY",
    "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY"
]
for c in colonne_cause:
    df_clean = df_clean.withColumn(
        c,
        when(col("ARR_DELAY") < 15, lit(0.0)).otherwise(coalesce(col(c), lit(0.0)))
    )


# Scrittura Parquet partizionato per mese
df_clean.write \
    .mode("overwrite") \
    .partitionBy("MONTH") \
    .parquet(output_path)

print("Preprocessing completato.")