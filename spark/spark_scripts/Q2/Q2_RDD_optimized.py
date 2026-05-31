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
output_path = "hdfs://namenode:9000/data/results/q2_output/rdd_tuple"

paths_to_read = [os.path.join(input_path, f"mese={i:02d}") for i in range(1, 5)]


def rdd_filter(tupla):
    # Usiamo gli indici scoperti dinamicamente dallo schema
    carrier         = tupla[idx_carrier]
    cancelled_val   = tupla[idx_cancelled]
    diverted_val    = tupla[idx_diverted]

    try:
        cancelled   = float(cancelled_val) if cancelled_val is not None else 0.0
        diverted    = float(diverted_val) if diverted_val is not None else 1.0
    except (ValueError, TypeError):
        cancelled = 1.0
        diverted = 0.0

    return carrier and cancelled == 0.0 and diverted == 0

# === FUNZIONI PER L'ELABORAZIONE RDD ===
def map_to_key_value(tupla):
    """
    Trasforma una riga in una coppia (Chiave, Valore).
    Chiave = (OP_UNIQUE_CARRIER)
    Valore = (tot_flights, canc_flights, sum_delay, min_delay, max_delay)
    """

    carrier = tupla[idx_carrier]

    arr_delay           = float(tupla[idx_delay])
    carrier_delay       = float(tupla[idx_carrier_delay])
    weather_delay       = float(tupla[idx_weather_delay])
    nas_delay           = float(tupla[idx_nas_delay])
    security_delay      = float(tupla[idx_security])
    late_aircraft_delay = float(tupla[idx_late_aircraft_delay])

    return (
            carrier,
            (1, arr_delay, carrier_delay, weather_delay, nas_delay, security_delay, late_aircraft_delay ),
        )


def reduce_combiner(val1, val2):
    """
    Combina due "Valori" che appartengono alla stessa chiave (stesso mese/compagnia).
    """
    return (
        val1[0] + val2[0],  # Somma voli
        val1[1] + val2[1],  # Somma ritardi
        val1[2] + val2[2],  # Somma ritardi veicolo
        val1[3] + val2[3],  # Somma ritardi tempo
        val1[4] + val2[4],  # Somma ritardi nas
        val1[5] + val2[5],  # Somma ritardi sicurezza
        val1[6] + val2[6],  # Somma ritardi veicolo precedente
    )


def calculate_final_metrics(pair):
    """
    A partire dai totali aggregati, calcola le metriche finali (medie, percentuali) e arrotonda.
    Restituisce un oggetto Row, utile per riconvertire facilmente in DataFrame per l'output.
    """
    key, val = pair

    carrier = key


    tot_flights = val[0]
    total_delay = val[1]
    total_carrier_delay = val[2]
    total_weather_delay = val[3]
    total_nas_delay = val[4]
    total_security_delay = val[5]
    total_late_aircraft_delay = val[6]

    mean_total_delay = (total_delay / tot_flights)
    mean_carrier_delay = (total_carrier_delay / tot_flights)
    mean_weather_delay = (total_weather_delay / tot_flights)
    mean_nas_delay = (total_nas_delay / tot_flights)
    mean_security_delay = (total_security_delay / tot_flights)
    mean_late_aircraft_delay = (total_late_aircraft_delay / tot_flights)


    return Row(
        OP_UNIQUE_CARRIER=carrier,
        tot_flights=tot_flights,
        mean_total_delay= round(mean_total_delay, 2),
        mean_carrier_delay = round(mean_carrier_delay, 2),
        mean_weather_delay = round(mean_weather_delay, 2),
        mean_nas_delay = round(mean_nas_delay, 2),
        mean_security_delay = round(mean_security_delay, 2),
        mean_late_aircraft_delay = round(mean_late_aircraft_delay, 2),
    )


# === SCRIPT PRINCIPALE ===

if __name__ == "__main__":
    start_time_script = time.time()

    spark = SparkSession.builder \
        .appName("Query_2_RDD_Performance_RDD") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    init_df = spark.read.option("basePath", input_path).parquet(*paths_to_read)
    columns = init_df.columns

    # Indici globali che useranno le funzioni RDD
    idx_carrier                 = columns.index("OP_UNIQUE_CARRIER")
    idx_delay                   = columns.index("ARR_DELAY")
    idx_carrier_delay           = columns.index("CARRIER_DELAY")
    idx_weather_delay           = columns.index("WEATHER_DELAY")
    idx_nas_delay               = columns.index("NAS_DELAY")
    idx_security                = columns.index("SECURITY_DELAY")
    idx_late_aircraft_delay     = columns.index("LATE_AIRCRAFT_DELAY")
    idx_cancelled               = columns.index("CANCELLED")
    idx_diverted                = columns.index("DIVERTED")

    logging.info("Inizio lettura e processamento Query 2 (RDD approach)...")
    all_times = []

    for i in range(0, 10):
        start_time_query = time.time()

        # 1. Lettura tramite DF (ottimizzato per Parquet) per ottenere lo schema, poi convertito in RDD
        rdd_df = spark.read.option("basePath", input_path).parquet(*paths_to_read).rdd.map(tuple)
        rdd_filtered = rdd_df.filter(rdd_filter)

        # 2. Pipeline RDD (Trasformazioni)
        final_rdd = (
            rdd_filtered
            .map(map_to_key_value)
            .reduceByKey(reduce_combiner)
            .map(calculate_final_metrics)
            .filter(lambda row: row[1] >= 500)
            .sortBy(lambda row: row[2], ascending = False)
            .take(10)
        )

        end_time_before = time.time()

        # 3. Azione (Trigger)
        if i == 9:
            rdd_to_df = spark.createDataFrame(final_rdd)

            # APPLICHIAMO LA SELECT PER RINOMINARE LE COLONNE
            final_df = rdd_to_df.select(
            col("OP_UNIQUE_CARRIER").alias("Compagnia Aerea"),
            col("tot_flights").alias("Numero di voli"),
            col("mean_total_delay").alias("Ritardo Medio Arrivo (min)"),
            col("mean_carrier_delay").alias("Ritardo Medio Veicolo (min)"),
            col("mean_weather_delay").alias("Ritardo Medio Meteo (min)"),
            col("mean_nas_delay").alias("Ritardo Medio Nas (min)"),
            col("mean_security_delay").alias("Ritardo Medio Sicurezza (min)"),
            col("mean_late_aircraft_delay").alias("Ritardo Medio dovuto a voluto precedente (min)"),
        )
            final_df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
        else:
            df_from_rdd = spark.createDataFrame(final_rdd)
            df_from_rdd.coalesce(1).write.format("noop").mode("overwrite").save()

        end_time = time.time()
        exec_time = end_time - start_time_query
        all_times.append(exec_time)
        logging.info(f"Iterazione {i + 1}/10 conclusa in {exec_time:.2f} sec")

    avg_time = statistics.mean(all_times)
    logging.info("-" * 50)
    logging.info(f"Tempo medio di esecuzione (RDD): {avg_time:.4f} secondi")
    logging.info("-" * 50)

    spark.stop()