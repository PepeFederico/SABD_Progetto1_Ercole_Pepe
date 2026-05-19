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
output_path = "hdfs://namenode:9000/data/results/q2_output/df"


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
        .appName("Query_2_AA_DL_Performance") \
        .getOrCreate()

    # Silenziamo i log di INFO di Spark per vedere chiaramente i nostri log applicativi
    spark.sparkContext.setLogLevel("WARN")

    logging.info("Inizio lettura e processamento Query 2...")
    all_times = []

    # 2. Avvio del cronometro specifico per la query (solo I/O e calcolo)
    for i in range(0, 10):
        start_time_query = time.time()

        # Lettura dei dati
        df = spark.read.option("basePath", input_path).parquet(*paths_to_read)

        # Filtraggio
        df_filtered = df.filter(col("DIVERTED") == 0).filter(col("CANCELLED") == 0)

        # Aggregazione
        q1_result = df_filtered.groupBy("OP_UNIQUE_CARRIER").agg(
            count("*").alias("records"),
            round(avg(col("ARR_DELAY")), 2).alias("arr_delay_mean"),
            round(avg(col("CARRIER_DELAY")), 2).alias("carrier_delay_mean"),
            round(avg(col("WEATHER_DELAY")), 2).alias("weather_delay_mean"),
            round(avg(col("NAS_DELAY")), 2).alias("nas_delay_mean"),
            round(avg(col("SECURITY_DELAY")), 2).alias("security_delay_mean"),
            round(avg(col("LATE_AIRCRAFT_DELAY")), 2).alias("late_air_delay_mean"),
        )

        q1_500_filtered = q1_result.filter(col("records") >= 500)
        q1_100_filtered = q1_500_filtered.orderBy(col("arr_delay_mean").desc()).limit(10)

        final_df = q1_100_filtered.select(
            col("OP_UNIQUE_CARRIER").alias("Compagnia Aerea"),
            col("records").alias("Numero di voli"),
            col("arr_delay_mean").alias("Ritardo Medio Arrivo (min)"),
            col("carrier_delay_mean").alias("Ritardo Medio Veicolo (min)"),
            col("weather_delay_mean").alias("Ritardo Medio Meteo (min)"),
            col("nas_delay_mean").alias("Ritardo Medio Nas (min)"),
            col("security_delay_mean").alias("Ritardo Medio Sicurezza (min)"),
            col("late_air_delay_mean").alias("Ritardo Medio dovuto a voluto precedente (min)"),
        )



        # Ordinamento
        end_time_before = time.time()

        # Azione: Scrittura in CSV (Questo scatena l'esecuzione reale!)
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

        # 3. Stop del cronometro specifico
        end_time = time.time()
        exec_time = end_time - start_time_query
        all_times.append(exec_time)
        logging.info(f"Iterazione {i + 1}/10 conclusa in {exec_time:.2f} sec")

    avg_time = statistics.mean(all_times)
    logging.info("-" * 50)
    logging.info("-" * 50)


    # Stop del cronometro globale
    end_time_script = time.time()
    logging.info(f"Tempo medio di esecuzione: {avg_time:.4f} secondi")


    spark.stop()
