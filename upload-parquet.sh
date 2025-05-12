#!/bin/bash
# Script pour télécharger et distribuer un fichier Parquet vers le cluster MemoryDB

# Configuration
NODE1="localhost:8081"  # Adresse du nœud principal
TABLE_NAME="parquet_file"  # Nom de la table cible
PARQUET_FILE="/Users/yoanln/MemoryDB/data/test2.parquet"  # Chemin du fichier local à télécharger
ROW_LIMIT=1000  # Nombre de lignes à charger (-1 pour toutes)
BATCH_SIZE=10000  # Taille des lots pour le traitement

# Vérification que le fichier existe
if [ ! -f "$PARQUET_FILE" ]; then
  echo "Erreur: Le fichier $PARQUET_FILE n'existe pas!"
  exit 1
fi

echo "Téléchargement et distribution du fichier $PARQUET_FILE vers le cluster MemoryDB..."
echo "Table cible: $TABLE_NAME"
echo "Limite de lignes: $ROW_LIMIT"
echo "Taille des lots: $BATCH_SIZE"

# Exécution de la requête curl multipart
curl -X POST \
  -F "file=@$PARQUET_FILE" \
  -F "rowLimit=$ROW_LIMIT" \
  -F "batchSize=$BATCH_SIZE" \
  http://$NODE1/api/tables/$TABLE_NAME/load-distributed-upload

echo ""
echo "Commande terminée. Vérifiez les logs du serveur pour plus d'informations."
