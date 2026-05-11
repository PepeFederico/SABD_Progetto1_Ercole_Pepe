#!/bin/bash
set -e

NAMENODE_DIR="/opt/hadoop/data/nameNode"

# 1. Formattazione se necessario
if [ ! -d "$NAMENODE_DIR/current" ]; then
    echo "Formattazione NameNode..."
    hdfs namenode -format -force -nonInteractive
fi

# 2. Avviamo il NameNode in BACKGROUND (&)
echo "Avvio NameNode in background..."
hdfs namenode &

# 3. Aspettiamo che HDFS sia effettivamente pronto (uscita da Safemode)
echo "In attesa che HDFS sia pronto..."
until hdfs dfsadmin -safemode wait | grep -q "Safe mode is OFF"; do
  sleep 2
done

# 4. Eseguiamo i comandi di setup
echo "Creazione cartelle..."
hdfs dfs -mkdir -p /data/nifi_output
hdfs dfs -chmod 777 /data/nifi_output
echo "Setup completato!"

# 5. Riportiamo il processo in foreground per non far spegnere il container
wait