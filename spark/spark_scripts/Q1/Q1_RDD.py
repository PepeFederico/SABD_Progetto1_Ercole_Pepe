import os
import statistics
import time
import logging
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col

# Configurazione del logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

input_path = "hdfs://namenode:9000/data/processed_data"
output_path = "hdfs://namenode:9000/data/results/q1_output/rdd"

paths_to_read = [
    os.path.join(input_path, f"MONTH={i}") for i in range(1, 5)
]


# === FUNZIONI PER L'ELABORAZIONE RDD ===

def map_to_key_value(row):
    """
    Trasforma una riga in una coppia (Chiave, Valore).
    Chiave = (MONTH, OP_UNIQUE_CARRIER)
    Valore = (tot_flights, canc_flights, sum_delay, min_delay, max_delay)
    """
    month = row['MONTH']
    carrier = row['OP_UNIQUE_CARRIER']

    cancelled = row["CANCELLED"]
    delay = float(row['DEP_DELAY']) if row['DEP_DELAY'] is not None else 0.0

    if cancelled == 1.0:
        return (
            (month, carrier),
            (1, 1, 0.0, 999999, -999999)
        )
    else:
        return (
            (month, carrier),
            (1, 0, delay, delay, delay)
        )


def reduce_combiner(val1, val2):
    """
    Combina due "Valori" che appartengono alla stessa chiave (stesso mese/compagnia).
    """
    return (
        val1[0] + val2[0],  # Somma voli totali
        val1[1] + val2[1],  # Somma voli cancellati
        val1[2] + val2[2],  # Somma dei ritardi
        min(val1[3], val2[3]),  # Calcolo minimo ritardi
        max(val1[4], val2[4])  # Calcolo massimo ritardi
    )


def calculate_final_metrics(pair):
    """
    A partire dai totali aggregati, calcola le metriche finali (medie, percentuali) e arrotonda.
    Restituisce un oggetto Row, utile per riconvertire facilmente in DataFrame per l'output.
    """
    key, val = pair

    month = key[0]
    carrier = key[1]

    tot_flights = val[0]
    canc_flights = val[1]
    sum_delay = val[2]
    min_delay = val[3]
    max_delay = val[4]

    mean_delay = (sum_delay / (tot_flights - canc_flights))
    canc_rate = (canc_flights / tot_flights) * 100


    return Row(
        MONTH=month,
        OP_UNIQUE_CARRIER=carrier,
        dep_delay_mean=round(mean_delay, 2) if mean_delay is not None else None,
        dep_delay_min=round(min_delay, 2),
        dep_delay_max=round(max_delay, 2),
        cancellation_rate=round(canc_rate, 2)
    )


# === SCRIPT PRINCIPALE ===

if __name__ == "__main__":
    start_time_script = time.time()

    spark = SparkSession.builder \
        .appName("Query_1_AA_DL_Performance_RDD") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    logging.info("Inizio lettura e processamento Query 1 (RDD approach)...")
    all_times = []

    for i in range(0, 10):
        start_time_query = time.time()

        # 1. Lettura tramite DF (ottimizzato per Parquet) per ottenere lo schema, poi convertito in RDD
        df_raw = spark.read.option("basePath", input_path).parquet(*paths_to_read)
        rdd = df_raw.rdd

        # 2. Pipeline RDD (Trasformazioni)
        final_rdd = (
            rdd
            .filter(lambda row: row['OP_UNIQUE_CARRIER'] in ("AA", "DL"))  # Filtro per compagnia
            .map(map_to_key_value)  # Setup MapReduce
            .reduceByKey(reduce_combiner)  # Aggregazione Distribuita
            .map(calculate_final_metrics)  # Calcolo Statistiche (Medie/%)
            .sortBy(lambda row: (row['MONTH'], row['OP_UNIQUE_CARRIER']))  # Ordinamento
        )

        end_time_before = time.time()

        # 3. Azione (Trigger)
        if i == 9:
            df_from_rdd = spark.createDataFrame(final_rdd)

            # APPLICHIAMO LA SELECT PER RINOMINARE LE COLONNE
            final_df = df_from_rdd.select(
                col("MONTH").alias("Mese"),
                col("OP_UNIQUE_CARRIER").alias("Compagnia Aerea"),
                col("dep_delay_mean").alias("Ritardo Medio Partenza (min)"),
                col("dep_delay_min").alias("Ritardo Minimo Partenza (min)"),
                col("dep_delay_max").alias("Ritardo Massimo Partenza (min)"),
                col("cancellation_rate").alias("Percentuale voli cancellati")
            )

            # Scriviamo il risultato finale
            final_df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
        else:
            df_from_rdd = spark.createDataFrame(final_rdd)
            df_from_rdd.coalesce(1).write.format("noop").mode("overwrite").csv(output_path, header=True)

        end_time = time.time()
        exec_time = end_time - start_time_query
        all_times.append(exec_time)
        logging.info(f"Iterazione {i + 1}/10 conclusa in {exec_time:.2f} sec")

    avg_time = statistics.mean(all_times)
    logging.info("-" * 50)
    logging.info(f"Tempo medio di esecuzione (RDD): {avg_time:.4f} secondi")
    logging.info("-" * 50)

    spark.stop()