import os
import statistics
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, sum, count, when, round

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

input_path = "hdfs://namenode:9000/data/processed_data"
output_path = "hdfs://namenode:9000/data/results/q1_output/df"

paths_to_read = [
        os.path.join(input_path, "MONTH=1"),
        os.path.join(input_path, "MONTH=2"),
        os.path.join(input_path, "MONTH=3"),
        os.path.join(input_path, "MONTH=4")
    ]

if __name__ == "__main__":
    # Inizializzazione della Sessione
    spark = SparkSession.builder \
        .appName("Query_1_DF_Performance") \
        .getOrCreate()

    # Silenziamo i log di INFO di Spark per vedere chiaramente i nostri log applicativi
    spark.sparkContext.setLogLevel("WARN")

    logging.info("Inizio lettura e processamento Query 1...")
    all_times = []

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
        q1_result = q1_result.orderBy("MONTH", "OP_UNIQUE_CARRIER")

        final_df = q1_result.select(
            col("MONTH").alias("Mese"),
            col("OP_UNIQUE_CARRIER").alias("Compagnia Aerea"),
            col("dep_delay_mean").alias("Ritardo Medio Partenza (min)"),
            col("dep_delay_min").alias("Ritardo Minimo Partenza (min)"),
            col("dep_delay_max").alias("Ritardo Massimo Partenza (min)"),
            col("cancellation_rate").alias("Percentuale voli cancellati"),
        )

        end_time_before = time.time()

        if i == 9:
            final_df.coalesce(1) \
                .write \
                .mode("overwrite") \
                .csv(output_path, header=True)
        else:
            final_df.coalesce(1) \
                .write \
                .format("noop") \
                .mode("overwrite") \
                .save()

        end_time = time.time()
        exec_time = end_time - start_time_query
        all_times.append(exec_time)
        logging.info(f"Iterazione {i + 1}/10 conclusa in {exec_time:.2f} sec")


    avg_time = statistics.mean(all_times)
    logging.info("-" * 50)
    logging.info("-" * 50)

    logging.info(f"Tempo medio di esecuzione: {avg_time:.4f} secondi")


    spark.stop()