#!/bin/bash

# 1. Esci subito se ci sono errori
set -e

# 2. Definizione percorso (senza backslash!)
DATANODE_DIR="/opt/hadoop/data/dataNode"

echo "====================================================="
echo "Analisi cartella DataNode: $DATANODE_DIR"
echo "====================================================="

# 3. Pulizia REALE della cartella
if [ -d "$DATANODE_DIR" ]; then
    echo "Pulizia in corso..."
    # Cancelliamo tutto il contenuto, inclusi i file nascosti
    rm -rf "${DATANODE_DIR:?}"/*
    echo "Cartella pulita con successo."
else
    echo "Cartella non trovata. Creazione in corso..."
    mkdir -p "$DATANODE_DIR"
fi

# 4. Permessi (Usa root se nel compose hai 'user: root')
chmod 777 "$DATANODE_DIR"

# 5. Avvio del servizio
echo "======================================="
echo "Starting HDFS DataNode Service..."
echo "======================================="
hdfs datanode