import os
import statistics
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, sum, count, when, round
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F


# Configurazione del logging per garantire la visibilità in Airflow/Docker
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

input_path = "hdfs://namenode:9000/data/processed_data"
output_path_P = "hdfs://namenode:9000/data/results/q3_output/df/percentiles"
output_path_MM = "hdfs://namenode:9000/data/results/q3_output/df/MinMax"

carriers = ["AA", "DL", "UA", "WN"]
quantiles = [0.25, 0.5, 0.75, 0.90]
score = 100 # 1/errore_desiderato = score, quindi nel nostro caso errore_desiderato è 0.01


paths_to_read = [
        os.path.join(input_path, "MONTH=1"),
        os.path.join(input_path, "MONTH=2"),
        os.path.join(input_path, "MONTH=3"),
        os.path.join(input_path, "MONTH=4")
    ]

if __name__ == "__main__":

    # Inizializzazione della Sessione
    spark = SparkSession.builder \
        .appName("Query_3_DF_Performance") \
        .getOrCreate()

    # Silenziamo i log di INFO di Spark per vedere chiaramente i nostri log applicativi
    spark.sparkContext.setLogLevel("WARN")

    logging.info("Inizio lettura e processamento Query 3...")
    all_times = []

    # 2. Avvio del cronometro specifico per la query (solo I/O e calcolo)
    for i in range(0, 10):
        start_time_query = time.time()

        # Lettura dei dati
        df = spark.read.option("basePath", input_path).parquet(*paths_to_read)

        # Filtraggio
        df_filtered = df.filter((col("CANCELLED") == 0) & (col("OP_UNIQUE_CARRIER").isin(carriers)))
        df_clean = df_filtered.withColumn("Fascia_oraria", (col("CRS_DEP_TIME") / 100).cast(IntegerType()))
        df_clean.cache()


        # Aggregazione
        q1_result_1 = df_clean.groupBy("OP_UNIQUE_CARRIER", "Fascia_oraria").agg(
            F.percentile_approx("DEP_DELAY", quantiles, score).alias("percentili")
        ).select(
            col("OP_UNIQUE_CARRIER").alias("Compagnia_Aerea"),
            "Fascia_oraria",
            round(col("percentili")[0].cast("double"), 2).alias("P25"),
            round(col("percentili")[1].cast("double"), 2).alias("P50"),
            round(col("percentili")[2].cast("double"), 2).alias("P75"),
            round(col("percentili")[3].cast("double"), 2).alias("P90")
        ).orderBy("Compagnia_Aerea", "Fascia_oraria", ascending=True)

        q1_result_2 = (df_clean.groupBy("OP_UNIQUE_CARRIER").agg(
            round(min(col("DEP_DELAY")), 2).alias("Ritardo Minimo Partenza (Min)"),
            round(max(col("DEP_DELAY")), 2).alias("Ritardo Massimo Partenza (Min)"),
        ).select(
            col("OP_UNIQUE_CARRIER").alias("Compagnia_Aerea"),
            "Ritardo Minimo Partenza (Min)",
            "Ritardo Massimo Partenza (Min)",
        ).orderBy(col("Compagnia_Aerea")))

        end_time_before = time.time()

        # Azione: Scrittura in CSV
        if i == 9:
            q1_result_1.coalesce(1) \
                .write \
                .mode("overwrite") \
                .csv(output_path_P, header=True)
            q1_result_2.coalesce(1) \
                .write \
                .mode("overwrite") \
                .csv(output_path_MM, header=True)
        else:
            q1_result_1.coalesce(1) \
                .write \
                .format("noop") \
                .mode("overwrite") \
                .save()
            q1_result_2.coalesce(1) \
                .write \
                .format("noop") \
                .mode("overwrite") \
                .save()

        # 3. Stop del cronometro specifico
        end_time = time.time()
        exec_time = end_time - start_time_query
        all_times.append(exec_time)
        logging.info(f"Iterazione {i + 1}/10 conclusa in {exec_time:.2f} sec")

    avg_time = statistics.mean(all_times)
    logging.info("-" * 50)
    logging.info("-" * 50)

    logging.info(f"Tempo medio di esecuzione: {avg_time:.4f} secondi")


    spark.stop()
