from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, to_json, struct


def process_q1(spark, hdfs_base_path):
    print("\n=== [START] Elaborazione QUERY 1 (Metriche Mensili) ===")
    # Lettura ricorsiva di tutti i file nella cartella Q1
    path = f"{hdfs_base_path}/q1_output/df/*.csv"
    df = spark.read.csv(path, header=True, inferSchema=True)

    # Rinominiamo le colonne complesse per evitare spazi nei campi JSON
    df_clean = df.withColumnRenamed("Compagnia Aerea", "compagnia") \
        .withColumnRenamed("Mese", "mese") \
        .withColumnRenamed("Ritardo Medio Partenza (min)", "ritardo_medio_part") \
        .withColumnRenamed("Ritardo Minimo Partenza (min)", "ritardo_min_part") \
        .withColumnRenamed("Ritardo Massimo Partenza (min)", "ritardo_max_part") \
        .withColumnRenamed("Percentuale voli cancellati", "perc_cancellati")

    # Chiave composita -> q1:mese:compagnia  (es. q1:1:AA)
    df_redis = df_clean.withColumn("key", concat_ws(":", col("compagnia"), col("mese")))

    # Scrittura su Redis come Stringa JSON (tabella logica: q1)
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

    # Pulizia nomi colonne per il JSON
    df_clean = df.withColumnRenamed("Compagnia Aerea", "compagnia") \
        .withColumnRenamed("Numero di voli", "num_voli") \
        .withColumnRenamed("Ritardo Medio Arrivo (min)", "ritardo_medio_arr") \
        .withColumnRenamed("Ritardo Medio Veicolo (min)", "ritardo_veicolo") \
        .withColumnRenamed("Ritardo Medio Meteo (min)", "ritardo_meteo") \
        .withColumnRenamed("Ritardo Medio Nas (min)", "ritardo_nas") \
        .withColumnRenamed("Ritardo Medio Sicurezza (min)", "ritardo_sicurezza") \
        .withColumnRenamed("Ritardo Medio dovuto a voluto precedente (min)", "ritardo_volo_precedente")

    # Chiave singola -> q2:compagnia (es. q2:OH)
    df_redis = df_clean.withColumn("key", col("compagnia"))

    # Scrittura su Redis (tabella logica: q2)
    df_redis.write \
        .format("org.apache.spark.sql.redis") \
        .option("key.column", "key") \
        .option("table", "q2") \
        .mode("append") \
        .save()
    print("=== [END] QUERY 2 completata con successo ===")


def process_q3_P(spark, hdfs_base_path):
    print("\n=== [START] Elaborazione QUERY 3 (Percentili Orari) ===")
    path = f"{hdfs_base_path}/q3_output/df/percentiles/*.csv"
    df = spark.read.csv(path, header=True, inferSchema=True)

    # Chiave composita -> q3:compagnia:fascia_oraria (es. q3:UA:12)
    df_redis = df.withColumn("key", concat_ws(":", col("Compagnia_Aerea"), col("Fascia_oraria")))

    # Scrittura su Redis (tabella logica: q3)
    df_redis.write \
        .format("org.apache.spark.sql.redis") \
        .option("key.column", "key") \
        .option("table", "q3_p") \
        .mode("append") \
        .save()
    print("=== [END] QUERY 3 completata con successo ===")



def process_q3_MM(spark, hdfs_base_path):
    print("\n=== [START] Elaborazione QUERY 3 (Ritardi Partenza Minimi e Massimi) ===")
    path = f"{hdfs_base_path}/q3_output/df/MinMax/*.csv"
    df = spark.read.csv(path, header=True, inferSchema=True)

    df_clean = df.withColumnRenamed("Ritardo Minimo Partenza (Min)", "Ritardo_Partenza_Minimo") \
        .withColumnRenamed("Ritardo Massimo Partenza (Min)", "Ritardo_Partenza_Massimo")

    df_redis = df_clean.withColumn("key", col("Compagnia_Aerea"))

    # Scrittura su Redis (tabella logica: q3)
    df_redis.write \
        .format("org.apache.spark.sql.redis") \
        .option("key.column", "key") \
        .option("table", "q3_mm") \
        .mode("append") \
        .save()
    print("=== [END] QUERY 3 completata con successo ===")


if __name__ == "__main__":
    # Inizializzazione dell'unica SparkSession per l'intero ciclo di vita dell'applicazione
    spark = SparkSession.builder \
        .appName("HDFS-Multiple-Queries-To-Redis") \
        .config("spark.redis.host", "redis") \
        .config("spark.redis.port", "6379") \
        .config("spark.redis.auth", "qwertyuiopo1") \
        .getOrCreate()

    # Base path di HDFS sfruttando l'host e la porta RPC definiti nel tuo docker-compose
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