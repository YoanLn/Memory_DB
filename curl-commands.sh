#!/bin/bash
# Fichier de commandes curl pour MemoryDB
# Exécuter avec: bash curl-commands.sh [commande]

# Configuration
NODE1="localhost:8081"
NODE2="localhost:8082"
NODE3="localhost:8083"
PARQUET_FILE="/Users/yoanln/MemoryDB/data/test2.parquet"

# Pour un déploiement sur des machines physiques, modifiez ces valeurs:
#NODE1="192.168.1.101:8080"
#NODE2="192.168.1.102:8080"
#NODE3="192.168.1.103:8080"

# Création d'une table
create_table() {
  echo "Création de la table parquet_file..."
  curl -X POST -H "Content-Type: application/json" \
    -d '{
  "name": "parquet_file",
  "columns": [
    {"name": "hvfhs_license_num", "type": "STRING", "nullable": true},
    {"name": "dispatching_base_num", "type": "STRING", "nullable": true},
    {"name": "originating_base_num", "type": "STRING", "nullable": true},
    {"name": "request_datetime", "type": "LONG", "nullable": true},
    {"name": "on_scene_datetime", "type": "LONG", "nullable": true},
    {"name": "pickup_datetime", "type": "LONG", "nullable": true},
    {"name": "dropoff_datetime", "type": "LONG", "nullable": true},
    {"name": "PULocationID", "type": "INTEGER", "nullable": true},
    {"name": "DOLocationID", "type": "INTEGER", "nullable": true},
    {"name": "trip_miles", "type": "DOUBLE", "nullable": true},
    {"name": "trip_time", "type": "LONG", "nullable": true},
    {"name": "base_passenger_fare", "type": "DOUBLE", "nullable": true},
    {"name": "tolls", "type": "DOUBLE", "nullable": true},
    {"name": "bcf", "type": "DOUBLE", "nullable": true},
    {"name": "sales_tax", "type": "DOUBLE", "nullable": true},
    {"name": "congestion_surcharge", "type": "DOUBLE", "nullable": true},
    {"name": "airport_fee", "type": "DOUBLE", "nullable": true},
    {"name": "tips", "type": "DOUBLE", "nullable": true},
    {"name": "driver_pay", "type": "DOUBLE", "nullable": true},
    {"name": "shared_request_flag", "type": "STRING", "nullable": true},
    {"name": "shared_match_flag", "type": "STRING", "nullable": true},
    {"name": "access_a_ride_flag", "type": "STRING", "nullable": true},
    {"name": "wav_request_flag", "type": "STRING", "nullable": true},
    {"name": "wav_match_flag", "type": "STRING", "nullable": true}
  ]
}' \
    http://$NODE1/api/tables
}

# Liste des tables
list_tables() {
  echo "Liste des tables sur le nœud 1:"
  curl -s http://$NODE1/api/tables | jq '.'
  
  echo "Liste des tables sur le nœud 2:"
  curl -s http://$NODE2/api/tables | jq '.'
  
  echo "Liste des tables sur le nœud 3:"
  curl -s http://$NODE3/api/tables | jq '.'
}

# Chargement distribué - Petit nombre de lignes (test)
load_distributed_small() {
  echo "Chargement distribué avec un petit nombre de lignes (3)..."
  curl -X POST -H "Content-Type: application/json" \
    -d "{
      \"filePath\": \"$PARQUET_FILE\",
      \"rowLimit\": 3,
      \"batchSize\": 1000
    }" \
    http://$NODE1/api/tables/parquet_file/load-distributed
}

# Chargement distribué - Grand nombre de lignes
load_distributed_large() {
  echo "Chargement distribué avec un grand nombre de lignes (1000)..."
  curl -X POST -H "Content-Type: application/json" \
    -d "{
      \"filePath\": \"$PARQUET_FILE\",
      \"rowLimit\": 1000,
      \"batchSize\": 100000
    }" \
    http://$NODE1/api/tables/parquet_file/load-distributed
}

# Chargement distribué - Fichier complet
load_distributed_full() {
  echo "Chargement du fichier Parquet complet en mode distribué..."
  curl -X POST -H "Content-Type: application/json" \
    -d "{
      \"filePath\": \"$PARQUET_FILE\",
      \"rowLimit\": 3,
      \"batchSize\": 100000
    }" \
    http://$NODE1/api/tables/parquet_file/load-distributed
}

# Vérifier les statistiques de la table
check_stats() {
  echo "Statistiques de la table sur le nœud 1:"
  curl -s http://$NODE1/api/tables/parquet_file/stats | jq '.'
  
  echo "Statistiques de la table sur le nœud 2:"
  curl -s http://$NODE2/api/tables/parquet_file/stats | jq '.'
  
  echo "Statistiques de la table sur le nœud 3:"
  curl -s http://$NODE3/api/tables/parquet_file/stats | jq '.'
}

# Requête de test sur la table
query_table() {
  echo "Exécution d'une requête SQL simple..."
  curl -X POST -H "Content-Type: application/json" \
    -d '{
      "query": "SELECT COUNT(*) AS total FROM parquet_file",
      "limit": 10
    }' \
    http://$NODE1/api/query
}

# Supprimer une table
delete_table() {
  local table_name="${1:-parquet_file}"
  echo "Suppression de la table $table_name..."
  curl -X DELETE http://$NODE1/api/tables/$table_name
}

# Exécution en fonction du paramètre
case "$1" in
  create)
    create_table
    ;;
  list)
    list_tables
    ;;
  load-small)
    load_distributed_small
    ;;
  load-large)
    load_distributed_large
    ;;
  load-full)
    load_distributed_full
    ;;
  stats)
    check_stats
    ;;
  query)
    query_table
    ;;
  delete)
    delete_table "$2"
    ;;
  *)
    echo "Usage: $0 [create|list|load-small|load-large|load-full|stats|query|delete]"
    echo ""
    echo "Exemples:"
    echo "  $0 create       - Crée la table parquet_file"
    echo "  $0 list         - Liste toutes les tables sur les 3 nœuds"
    echo "  $0 load-small   - Charge 3 lignes (test)"
    echo "  $0 load-large   - Charge 1000 lignes"
    echo "  $0 load-full    - Charge le fichier complet"
    echo "  $0 stats        - Affiche les statistiques de la table sur tous les nœuds"
    echo "  $0 query        - Exécute une requête SQL de test"
    echo "  $0 delete nom   - Supprime la table spécifiée (par défaut: parquet_file)"
    ;;
esac
