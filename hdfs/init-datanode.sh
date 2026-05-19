#!/bin/bash
set -e

DATANODE_DIR="/opt/hadoop/data/dataNode"

echo "=================================================="
echo "Controllo cartella DataNode: $DATANODE_DIR"
echo "=================================================="

# Verifica se la directory NON esiste e creala
if [ ! -d "$DATANODE_DIR" ]; then
    echo "Cartella non trovata. Creazione in corso..."
    mkdir -p "$DATANODE_DIR"
else
    echo "Cartella esistente. I dati precedenti verranno mantenuti."
fi

# Permessi (Usa root se nel compose hai 'user: root')
chmod 777 "$DATANODE_DIR"

# Avvio del servizio
echo "=================================================="
echo "Starting HDFS DataNode Service..."
echo "=================================================="
hdfs datanode