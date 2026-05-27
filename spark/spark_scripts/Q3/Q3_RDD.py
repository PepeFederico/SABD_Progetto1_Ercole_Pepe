import os
import statistics
import time
import logging
from pyspark.sql import SparkSession, Row
from tdigest import TDigest

# Configurazione del logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

input_path = "hdfs://namenode:9000/data/processed_data"
output_path_P = "hdfs://namenode:9000/data/results/q3_output/rdd/percentile"
output_path_MM = "hdfs://namenode:9000/data/results/q3_output/rdd/MinMax"
paths_to_read = [
    os.path.join(input_path, f"MONTH={i}") for i in range(1, 5)
]

carriers = ["AA", "DL", "UA", "WN"]


# === FUNZIONI PER T-DIGEST ===
def create_combiner(value):
    digest = TDigest()
    if value is not None:
        digest.update(value)
    return digest


def merge_value(digest, value):
    if value is not None:
        digest.update(value)
    return digest


def merge_combiners(digest1, digest2):
    return digest1 + digest2


# === FUNZIONI PER L'ELABORAZIONE RDD ===
def convert_to_tuple(row):
    """
    Trasforma una riga in una coppia (Chiave, Valore).
    Chiave = (OP_UNIQUE_CARRIER, CRS_DEP_TIME_HOUR)
    Valore = (DEP_DELAY)
    """
    carrier = row['OP_UNIQUE_CARRIER']

    # Facciamo il calcolo dell'ora qui dentro!
    hour = int(row["CRS_DEP_TIME"]) // 100

    delay = float(row['DEP_DELAY']) if row['DEP_DELAY'] is not None else 0.0

    return ((carrier, hour), delay)


def map_to_key_value(row):
    """
    Trasforma una riga in una coppia (Chiave, Valore).
    Chiave = OP_UNIQUE_CARRIER)
    Valore = (max_delay, min_delay)
    """
    carrier = row['OP_UNIQUE_CARRIER']
    delay = float(row['DEP_DELAY']) if row['DEP_DELAY'] is not None else 0.0

    return carrier, (delay, delay)


def reduce_combiner(val1, val2):
    """
    Combina due "Valori" che appartengono alla stessa chiave (stessa compagnia).
    """
    return (
        min(val1[0], val2[0]),  # Calcolo minimo ritardi
        max(val1[1], val2[1])  # Calcolo massimo ritardi
    )


def toRow(pair):
    key = pair[0]
    val = pair[1]
    return Row(
        Compagnia_Aerea=key,
        Ritardo_Partenza_Minimo=val[0],
        Ritardo_Partenza_Massimo=val[1],
    )



def extract_percentiles(pair):
    # ATTENZIONE: parentesi quadre per estrarre elementi dalle tuple
    key = pair[0]
    td = pair[1]

    p25 = td.percentile(25)
    p50 = td.percentile(50)
    p75 = td.percentile(75)
    p90 = td.percentile(90)

    return Row(
        Compagnia_Aerea=key[0],
        Fascia_Oraria=key[1],
        P25=round(p25, 2),
        P50=round(p50, 2),
        P75=round(p75, 2),
        P90=round(p90, 2),
    )


# === SCRIPT PRINCIPALE ===
if __name__ == "__main__":
    start_time_script = time.time()

    spark = SparkSession.builder \
        .appName("Query_3_RDD_Performance_RDD") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    logging.info("Inizio lettura e processamento Query 3 (RDD approach)...")
    all_times = []

    for i in range(0, 10):
        start_time_query = time.time()

        # 1. Lettura tramite DF (ottimizzato per Parquet) per ottenere lo schema, poi convertito in RDD
        df_raw = spark.read.option("basePath", input_path).parquet(*paths_to_read)

        # Filtriamo mantenendo però l'intera ROW (struttura intatta)
        # Convertiamo CANCELLED a float per evitare errori in caso sia memorizzato come stringa o decimale
        rdd_filtered = df_raw.rdd.filter(
            lambda row: (
                    row["OP_UNIQUE_CARRIER"] in carriers
                    and float(row["CANCELLED"]) == 0.0
            )
        )
        rdd_filtered.cache()

        # 2. Pipeline RDD (Trasformazioni)
        final_rdd_1 = (
            rdd_filtered
            .map(convert_to_tuple)  # Qui prende la riga sana e ne estrae la tupla
            .combineByKey(create_combiner, merge_value, merge_combiners)
            .sortByKey()
            .map(extract_percentiles)
        )

        final_rdd_2 = (
            rdd_filtered
            .map(map_to_key_value)
            .reduceByKey(reduce_combiner)
            .sortByKey()
            .map(toRow)
        )



        # 3. Azione (Trigger)
        if i == 9:
            final_df = spark.createDataFrame(final_rdd_1)
            final_df.coalesce(1).write.mode("overwrite").csv(output_path_P, header=True)
            final_df_2 = spark.createDataFrame(final_rdd_2)
            final_df_2.coalesce(1).write.mode("overwrite").csv(output_path_MM, header=True)
        else:
            df_from_rdd = spark.createDataFrame(final_rdd_1)
            df_from_rdd.coalesce(1).write.format("noop").mode("overwrite").save()
            final_df_2 = spark.createDataFrame(final_rdd_2)
            final_df_2.coalesce(1).write.format("noop").mode("overwrite").save()

        rdd_filtered.unpersist()
        end_time = time.time()
        exec_time = end_time - start_time_query
        all_times.append(exec_time)
        logging.info(f"Iterazione {i + 1}/10 conclusa in {exec_time:.2f} sec")

    avg_time = statistics.mean(all_times)
    logging.info("-" * 50)
    logging.info(f"Tempo medio di esecuzione (RDD): {avg_time:.4f} secondi")
    logging.info("-" * 50)

    spark.stop()