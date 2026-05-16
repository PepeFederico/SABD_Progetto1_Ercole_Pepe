import os
import statistics
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, sum, count, when, round

# Configurazione del logging per garantire la visibilità in Airflow/Docker
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

input_path = "hdfs://namenode:9000/data/processed_data"
output_path = "hdfs://namenode:9000/data/results/q1_output"

paths_to_read = [
        os.path.join(input_path, "MONTH=1"),
        os.path.join(input_path, "MONTH=2"),
        os.path.join(input_path, "MONTH=3"),
        os.path.join(input_path, "MONTH=4")
    ]

if __name__ == "__main__":
    # 1. Avvio del cronometro globale (include il tempo di avvio del cluster)
    start_time_script = time.time()

    # Inizializzazione della Sessione
    spark = SparkSession.builder \
        .appName("Query_1_AA_DL_Performance") \
        .getOrCreate()

    # Silenziamo i log di INFO di Spark per vedere chiaramente i nostri log applicativi
    spark.sparkContext.setLogLevel("WARN")

    logging.info("Inizio lettura e processamento Query 1...")
    all_times = []

    # 2. Avvio del cronometro specifico per la query (solo I/O e calcolo)
    for i in range(0, 10):
        start_time_query = time.time()

        # Lettura dei dati
        df = spark.read.option("basePath", input_path).parquet(*paths_to_read)

        # Filtraggio
        df_filtered = df.filter(col("OP_UNIQUE_CARRIER").isin("AA", "DL"))

        # Aggregazione
        q1_result = df_filtered.groupBy("MONTH", "OP_UNIQUE_CARRIER").agg(
            round(avg(when(col("CANCELLED") == 0, col("DEP_DELAY"))), 2).alias("dep_delay_mean"),
            round(min(when(col("CANCELLED") == 0, col("DEP_DELAY"))), 2).alias("dep_delay_min"),
            round(max(when(col("CANCELLED") == 0, col("DEP_DELAY"))), 2).alias("dep_delay_max"),
            round((sum("CANCELLED") / count("*")) * 100, 2).alias("cancellation_rate")
        )

        # Ordinamento
        q1_result = q1_result.orderBy("MONTH", "OP_UNIQUE_CARRIER")
        end_time_before = time.time()
        logging.info(f"✔ Query 1 calcolata e salvata in HDFS in: {end_time_before - start_time_query:.2f} secondi. (Prima di scrittura CSV)")

        # Azione: Scrittura in CSV (Questo scatena l'esecuzione reale!)
        if i == 9:
            q1_result.coalesce(1) \
                .write \
                .mode("overwrite") \
                .csv(output_path, header=True)
        else:
            q1_result.coalesce(1) \
                .write \
                .format("noop") \
                .mode("overwrite") \
                .save()

        # 3. Stop del cronometro specifico
        end_time = time.time()
        all_times.append(end_time - start_time_query)


    avg_time = statistics.mean(all_times)
    logging.info("-" * 50)
    logging.info("-" * 50)


    # Stop del cronometro globale
    end_time_script = time.time()
    logging.info(f"Tempo medio di esecuzione: {avg_time:.4f} secondi")


    spark.stop()










