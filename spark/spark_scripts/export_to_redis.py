from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, to_json, struct, row_number
from pyspark.sql.window import Window

def process_q1(spark, hdfs_base_path):
    print("\n=== [START] Elaborazione QUERY 1 (Metriche Mensili) ===")
    path = f"{hdfs_base_path}/q1_output/df/*.csv"
    df = spark.read.csv(path, header=True, inferSchema=True)

    df_clean = df.withColumnRenamed("Compagnia Aerea", "compagnia") \
        .withColumnRenamed("Mese", "mese") \
        .withColumnRenamed("Ritardo Medio Partenza (min)", "ritardo_medio_part") \
        .withColumnRenamed("Ritardo Minimo Partenza (min)", "ritardo_min_part") \
        .withColumnRenamed("Ritardo Massimo Partenza (min)", "ritardo_max_part") \
        .withColumnRenamed("Percentuale voli cancellati", "perc_cancellati")

    # Chiave composita -> q1:compagnia:mese
    df_redis = df_clean.withColumn("key", concat_ws(":", col("compagnia"), col("mese")))

    df_redis.write \
        .format("org.apache.spark.sql.redis") \
        .option("key.column", "key") \
        .option("table", "q1") \
        .mode("append") \
        .save()
    print("=== [END] QUERY 1 completata con successo ===")


def process_q2(spark, hdfs_base_path):
    print("\n=== [START] Elaborazione QUERY 2 (Cause Ritardo Totali) ===")
    path = f"{hdfs_base_path}/q2_output/df/*.csv"
    df = spark.read.csv(path, header=True, inferSchema=True)

    df_clean = df.withColumnRenamed("Compagnia Aerea", "compagnia") \
        .withColumnRenamed("Numero di voli", "num_voli") \
        .withColumnRenamed("Ritardo Medio Arrivo (min)", "ritardo_medio_arr") \
        .withColumnRenamed("Ritardo Medio Veicolo (min)", "ritardo_veicolo") \
        .withColumnRenamed("Ritardo Medio Meteo (min)", "ritardo_meteo") \
        .withColumnRenamed("Ritardo Medio Nas (min)", "ritardo_nas") \
        .withColumnRenamed("Ritardo Medio Sicurezza (min)", "ritardo_sicurezza") \
        .withColumnRenamed("Ritardo Medio dovuto a voluto precedente (min)", "ritardo_volo_precedente")

    window_spec = Window.orderBy(col("ritardo_medio_arr").desc())
    df_ranked = df_clean.withColumn("posizione", row_number().over(window_spec))

    # Chiave q2:posizione_classifica
    df_redis = df_ranked.withColumn("key", col("posizione").cast("string"))
    df_redis.write \
        .format("org.apache.spark.sql.redis") \
        .option("key.column", "key") \
        .option("table", "q2") \
        .mode("overwrite") \
        .save()

    print("=== [END] QUERY 2 completata con successo ===")


def process_q3_P(spark, hdfs_base_path):
    print("\n=== [START] Elaborazione QUERY 3 (Percentili Orari) ===")
    path = f"{hdfs_base_path}/q3_output/df/percentiles/*.csv"
    df = spark.read.csv(path, header=True, inferSchema=True)

    df_clean = df.withColumnRenamed("Compagnia_Aerea", "compagnia") \
        .withColumnRenamed("Fascia oraria", "fascia_oraria") \
        .withColumnRenamed("25th percentile", "percentile_25") \
        .withColumnRenamed("50th percentile", "percentile_50") \
        .withColumnRenamed("75th_percentile", "percentile_75") \
        .withColumnRenamed("90th percentile", "percentile_90")

    # Chiave composita -> q3_p:compagnia:fascia_oraria
    df_redis = df_clean.withColumn("key", concat_ws(":", col("compagnia"), col("fascia_oraria")))

    df_redis.write \
        .format("org.apache.spark.sql.redis") \
        .option("key.column", "key") \
        .option("table", "q3_p") \
        .mode("append") \
        .save()
    print("=== [END] QUERY 3 Percentili completata ===")


def process_q3_MM(spark, hdfs_base_path):
    print("\n=== [START] Elaborazione QUERY 3 (Ritardi Partenza Minimi e Massimi) ===")
    path = f"{hdfs_base_path}/q3_output/df/MinMax/*.csv"
    df = spark.read.csv(path, header=True, inferSchema=True)

    df_clean = df.withColumnRenamed("Compagnia_Aerea", "compagnia") \
        .withColumnRenamed("Compagnia Aerea", "compagnia") \
        .withColumnRenamed("Ritardo Minimo Partenza (Min)", "ritardo_min_partenza") \
        .withColumnRenamed("Ritardo Massimo Partenza (Min)", "ritardo_max_partenza")

    # Chiave singola -> q3_mm:compagnia
    df_redis = df_clean.withColumn("key", col("compagnia"))

    df_redis.write \
        .format("org.apache.spark.sql.redis") \
        .option("key.column", "key") \
        .option("table", "q3_mm") \
        .mode("append") \
        .save()
    print("=== [END] QUERY 3 MinMax completata con successo ===")


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("HDFS-Multiple-Queries-To-Redis") \
        .config("spark.redis.host", "redis") \
        .config("spark.redis.port", "6379") \
        .config("spark.redis.auth", "qwertyuiopo1") \
        .getOrCreate()

    HDFS_BASE = "hdfs://namenode:9000/data/results"

    try:
        process_q1(spark, HDFS_BASE)
        process_q2(spark, HDFS_BASE)
        process_q3_P(spark, HDFS_BASE)
        process_q3_MM(spark, HDFS_BASE)
        print("\nPIPELINE COMPLETATA: Tutte le query sono state migrate su Redis!")
    except Exception as e:
        print(f"\nErrore durante l'esecuzione della pipeline: {e}")
    finally:
        spark.stop()