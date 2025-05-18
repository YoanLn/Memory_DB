#!/bin/bash
# Fichier de commandes curl pour MemoryDB
# Exécuter avec: bash curl-commands.sh [commande]

# Configuration
NODE1="localhost:8081"
NODE2="localhost:8082"
NODE3="localhost:8083"
PARQUET_FILE="data/test2.parquet"

# Pour un déploiement sur des machines physiques, modifiez ces valeurs:
#NODE1="192.168.1.101:8080"
#NODE2="192.168.1.102:8080"
#NODE3="192.168.1.103:8080"

# Création d'une table
create_table() {
  echo "Création de la table parquet_file..."
  curl --noproxy localhost -X POST -H "Content-Type: application/json" \
    -d '{
  "name": "parquet_file",
  "columns": [
    {"name": "VendorID", "type": "INTEGER", "nullable": true},
    {"name": "tpep_pickup_datetime", "type": "LONG", "nullable": true},
    {"name": "tpep_dropoff_datetime", "type": "LONG", "nullable": true},
    {"name": "passenger_count", "type": "LONG", "nullable": true},
    {"name": "trip_distance", "type": "DOUBLE", "nullable": true},
    {"name": "RatecodeID", "type": "LONG", "nullable": true},
    {"name": "store_and_fwd_flag", "type": "STRING", "nullable": true},
    {"name": "PULocationID", "type": "INTEGER", "nullable": true},
    {"name": "DOLocationID", "type": "INTEGER", "nullable": true},
    {"name": "payment_type", "type": "LONG", "nullable": true},
    {"name": "fare_amount", "type": "DOUBLE", "nullable": true},
    {"name": "extra", "type": "DOUBLE", "nullable": true},
    {"name": "mta_tax", "type": "DOUBLE", "nullable": true},
    {"name": "tip_amount", "type": "DOUBLE", "nullable": true},
    {"name": "tolls_amount", "type": "DOUBLE", "nullable": true},
    {"name": "improvement_surcharge", "type": "DOUBLE", "nullable": true},
    {"name": "total_amount", "type": "DOUBLE", "nullable": true},
    {"name": "congestion_surcharge", "type": "DOUBLE", "nullable": true},
    {"name": "Airport_fee", "type": "DOUBLE", "nullable": true},
    {"name": "cbd_congestion_fee", "type": "DOUBLE", "nullable": true}
  ]
}' \
    http://$NODE1/api/tables
}

# Liste des tables
list_tables() {
  echo "Liste des tables sur le nœud 1:"
  curl --noproxy localhost -s http://$NODE1/api/tables | jq '.'
  
  echo "Liste des tables sur le nœud 2:"
  curl --noproxy localhost -s http://$NODE2/api/tables | jq '.'
  
  echo "Liste des tables sur le nœud 3:"
  curl --noproxy localhost -s http://$NODE3/api/tables | jq '.'
}


# Chargement distribué - Petit nombre de lignes (test)
load_distributed_small() {
  echo "Chargement distribué avec un petit nombre de lignes (3)..."
  curl --noproxy localhost -X POST \
    -F "file=@$PARQUET_FILE" \
    -F "rowLimit=3" \
    -F "batchSize=1000" \
    http://$NODE1/api/tables/parquet_file/load-distributed-upload
}

# Chargement distribué - Grand nombre de lignes
load_distributed_large() {
  echo "Chargement distribué avec un grand nombre de lignes (sans limite)..."
  curl --noproxy localhost -X POST \
    -F "file=@$PARQUET_FILE" \
    -F "rowLimit=-1" \
    -F "batchSize=100000" \
    -F "skipRows=0" \
    http://$NODE1/api/tables/parquet_file/load-distributed-upload
}

# Comptage des lignes dans un fichier Parquet sans chargement
count_parquet_rows() {
  echo "Comptage des lignes dans le fichier Parquet: $PARQUET_FILE..."
  
  # Utilisation du chargeur distribué avec rowLimit=0 pour simplement compter les lignes
  curl --noproxy localhost -X POST \
    -H "Content-Type: application/json" \
    -d '{"filePath":"'"$PARQUET_FILE"'", "rowLimit":0, "skipRows":0, "batchSize":10000}' \
    http://$NODE1/api/tables/parquet_file/load-distributed
  
  echo ""
  echo "Note: Pour les chargements distribués, définir rowLimit à 20M au lieu de 30M est plus approprié pour éviter les problèmes de distribution."
}

# Chargement optimisé pour les environnements universitaires (proxy, quota disque limité)
load_binary() {
  local host=${1:-$NODE1}
  local row_limit=${2:-"3"}
  local batch_size=${3:-"100000"}
  local file=${4:-"$PARQUET_FILE"}
  
  echo "Chargement binaire optimisé pour environnements universitaires..."
  echo "Host: $host, Limite lignes: $row_limit, Batch: $batch_size"
  
  # Vérification de la présence du fichier
  if [ -z "$file" ]; then
    echo "ERREUR: Fichier Parquet non défini. Utilisez export PARQUET_FILE=/chemin/vers/fichier.parquet"
    return 1
  fi
  
  if [ ! -f "$file" ]; then
    echo "ERREUR: Fichier Parquet introuvable: $file"
    return 1
  fi
  
  echo "Envoi du fichier: $file ($(du -h "$file" | cut -f1))"
  
  # Utilisation du mode binaire direct sans proxy et sans écriture sur disque
  # --data-binary envoie le fichier directement sans conversion
  # Ne pas utiliser -F qui crée un multipart/form-data et des fichiers temporaires
  # -v pour obtenir des détails sur l'avancement et la réponse
  curl -v --noproxy '*' \
    -X POST \
    -H "Content-Type: application/octet-stream" \
    --data-binary @"$file" \
    "http://$host/api/tables/parquet_file/load-binary?rowLimit=99&batchSize=1&skipRows=0"
  
  echo ""
  echo "Chargement terminé. Vérifiez les statistiques avec: ./curl-commands.sh stats"
}

# Chargement en mode coordonnateur uniquement - pour configurations multi-PC
# Cette méthode charge les données uniquement sur le nœud coordonnateur,
# puis distribue les résultats de requête sans essayer de transférer le fichier
load_coordinator_only() {
  local row_limit="${1:-100}"
  local file_path="${PARQUET_FILE}"
  
  # Vérifier que le fichier existe
  if [ ! -f "$file_path" ]; then
    echo "ERREUR: Le fichier Parquet n'existe pas à l'emplacement: $file_path"
    echo "Veuillez vérifier le chemin du fichier dans la variable PARQUET_FILE au début du script."
    return 1
  fi

  echo "Chargement en mode coordonnateur uniquement avec $row_limit lignes..."
  echo "Utilisation du fichier: $(basename $file_path)"
  
  # 1. D'abord, supprimer la table sur tous les nœuds pour repartir proprement
  echo "Suppression de la table sur tous les nœuds..."
  curl --noproxy localhost -X DELETE http://$NODE1/api/tables/parquet_file >/dev/null 2>&1
  curl --noproxy localhost -X DELETE http://$NODE2/api/tables/parquet_file >/dev/null 2>&1
  if [ "$NODE3" != "" ]; then
    curl --noproxy localhost -X DELETE http://$NODE3/api/tables/parquet_file >/dev/null 2>&1
  fi
  
  # 2. Recréer la table sur tous les nœuds
  create_table
  sleep 1
  
  # 3. Charger les données uniquement sur le nœud coordonnateur (nœud 1)
  echo "Chargement des données uniquement sur le nœud coordonnateur..."
  
  # Utilisation de l'option -F avec le chemin absolu complet
  echo "Exécution de: curl -X POST -F \"file=@$file_path\" [...] http://$NODE1/api/tables/parquet_file/load"
  curl --noproxy localhost -X POST \
    -F "file=@$file_path" \
    -F "rowLimit=$row_limit" \
    -F "batchSize=1000" \
    http://$NODE1/api/tables/parquet_file/load
    
  echo -e "\nRemarque: Les données sont maintenant chargées uniquement sur le nœud coordonnateur."
  echo "Les requêtes seront exécutées sur le nœud coordonnateur et les résultats distribués."
  echo "Cette approche est utile quand les fichiers Parquet ne sont pas accessibles sur tous les nœuds."
}

# Chargement distribué - Fichier complet
load_distributed_full() {
  echo "Chargement du fichier Parquet complet en mode distribué..."
  curl --noproxy localhost -X POST \
    -F "file=@$PARQUET_FILE" \
    -F "rowLimit=-1" \
    -F "batchSize=200000" \
    -F "skipRows=0" \
    http://$NODE1/api/tables/parquet_file/load-distributed-upload
}



# Vérifier les statistiques de la table
check_stats() {
  echo "Statistiques de la table sur le nœud 1:"
  curl --noproxy localhost -s http://$NODE1/api/tables/parquet_file/stats
  
  echo "\nStatistiques de la table sur le nœud 2:"
  curl --noproxy localhost -s http://$NODE2/api/tables/parquet_file/stats
  
  echo "\nStatistiques de la table sur le nœud 3:"
  curl --noproxy localhost -s http://$NODE3/api/tables/parquet_file/stats | jq '.'
}

# Afficher les statistiques d'une table sur le nœud 1
stats() {
  echo "Statistiques de la table sur le nœud 1:"
  curl --noproxy localhost -X GET http://$NODE1/api/tables/parquet_file/stats
  
  echo "\nStatistiques de la table sur le nœud 2:"
  curl --noproxy localhost -X GET http://$NODE2/api/tables/parquet_file/stats
  
  echo "\nStatistiques de la table sur le nœud 3:"
  curl --noproxy localhost -X GET http://$NODE3/api/tables/parquet_file/stats
}

# Afficher les statistiques consolidées d'une table sur l'ensemble du cluster
consolidated_stats() {
  echo "Statistiques consolidées manuellement de la table sur l'ensemble du cluster:"
  
  # Récupérer les statistiques de chaque nœud
  echo "Récupération des statistiques depuis tous les nœuds..."
  NODE1_STATS=$(curl --noproxy localhost -s -X GET http://$NODE1/api/tables/parquet_file/stats)
  NODE2_STATS=$(curl --noproxy localhost -s -X GET http://$NODE2/api/tables/parquet_file/stats)
  NODE3_STATS=$(curl --noproxy localhost -s -X GET http://$NODE3/api/tables/parquet_file/stats)
  
  # Extraire les nombres de lignes
  NODE1_ROWS=$(echo $NODE1_STATS | grep -o '"rowCount":[0-9]*' | cut -d':' -f2)
  NODE2_ROWS=$(echo $NODE2_STATS | grep -o '"rowCount":[0-9]*' | cut -d':' -f2)
  NODE3_ROWS=$(echo $NODE3_STATS | grep -o '"rowCount":[0-9]*' | cut -d':' -f2)
  
  # Si les valeurs sont vides, utiliser 0
  NODE1_ROWS=${NODE1_ROWS:-0}
  NODE2_ROWS=${NODE2_ROWS:-0}
  NODE3_ROWS=${NODE3_ROWS:-0}
  
  # Calculer le total
  TOTAL_ROWS=$((NODE1_ROWS + NODE2_ROWS + NODE3_ROWS))
  
  # Afficher un résumé
  echo "----- Résumé -----"
  echo "Nœud 1: $NODE1_ROWS lignes"
  echo "Nœud 2: $NODE2_ROWS lignes"
  echo "Nœud 3: $NODE3_ROWS lignes"
  echo "Total: $TOTAL_ROWS lignes"
  
  # Calculer la distribution en pourcentage
  if [ $TOTAL_ROWS -gt 0 ]; then
    NODE1_PCT=$(echo "scale=2; $NODE1_ROWS * 100 / $TOTAL_ROWS" | bc)
    NODE2_PCT=$(echo "scale=2; $NODE2_ROWS * 100 / $TOTAL_ROWS" | bc)
    NODE3_PCT=$(echo "scale=2; $NODE3_ROWS * 100 / $TOTAL_ROWS" | bc)
    
    echo "
Distribution:"
    echo "Nœud 1: $NODE1_PCT%"
    echo "Nœud 2: $NODE2_PCT%"
    echo "Nœud 3: $NODE3_PCT%"
  fi
}

# Requête de test sur la table
query_table() {
  echo "Exécution d'une requête simple..."
  curl --noproxy localhost -s -X POST -H "Content-Type: application/json" \
    -d '{"tableName": "parquet_file", "columns": ["*"], "limit": 1000, "distributed": true}' \
    http://$NODE1/api/query | jq '.'
}

# Requête de test avec GROUP BY
query_group_by() {
  local col_arg="$1"
  local group_by_column="VendorID" # Default value
  if [ -n "$col_arg" ]; then # If an argument is actually provided and is not an empty string
    group_by_column="$col_arg"
  fi
  local distributed="${2:-true}"
  echo "Exécution d'une requête GROUP BY sur la colonne $group_by_column (distributed=$distributed)..."
  
  # Print the full curl command for diagnostics
  echo "Curl command:"
  echo "curl --noproxy localhost -X POST -H 'Content-Type: application/json' \
    -d '{
      \"tableName\": \"parquet_file\",
      \"columns\": [\"$group_by_column\"],
      \"groupBy\": [\"$group_by_column\"],
      \"aggregates\": {\"count\": \"COUNT\"},
      \"limit\": 10,
      \"distributed\": $distributed
    }' \
    http://$NODE1/api/query"
    
  # Execute the actual curl command
  curl --noproxy localhost -v -X POST -H "Content-Type: application/json" \
    -d "{
      \"tableName\": \"parquet_file\",
      \"columns\": [\"$group_by_column\"],
      \"groupBy\": [\"$group_by_column\"],
      \"aggregates\": {\"count\": \"COUNT\"},
      \"distributed\": $distributed
    }" \
    http://$NODE1/api/query | jq '.'
}

# Requête de test avec GROUP BY et agrégations multiples
query_group_by_aggregates() {
  local group_col="${1:-category}"
  local agg_col="${2:-value}"
  echo "Exécution d'une requête distribuée avec GROUP BY et agrégations multiples..."
  
  # Requête simplifiée pour le débogage
  local query_json="{
      \"tableName\": \"parquet_file\",
      \"columns\": [\"$group_col\"],
      \"groupBy\": [\"$group_col\"],
      \"aggregates\": {
        \"sum_$agg_col\": \"SUM\",
        \"count\": \"COUNT\",
        \"avg_$agg_col\": \"AVG\",
        \"min_$agg_col\": \"MIN\",
        \"max_$agg_col\": \"MAX\"
      },
      \"distributed\": true
    }"
  
  echo "Requête JSON:"
  echo "$query_json" | jq '.'
  
  # Exécute la requête
  echo "Résultat de la requête:"
  curl --noproxy localhost -s -X POST -H "Content-Type: application/json" \
    -d "$query_json" \
    http://$NODE1/api/query | jq '.'
}

# Supprimer une table
delete_table() {
  local table_name="${1:-parquet_file}"
  echo "Suppression de la table $table_name..."
  
  # Essaie d'abord avec l'endpoint /api/table
  echo "Essai avec endpoint /api/table/$table_name..."
  curl --noproxy localhost -X DELETE "http://$NODE1/api/table/$table_name"
  
  # Si l'autre endpoint ne fonctionne pas, essaie avec /api/tables
  echo "\nEssai avec endpoint /api/tables/$table_name..."
  curl --noproxy localhost -X DELETE "http://$NODE1/api/tables/$table_name"
}

# Vérifier l'état de santé du cluster
check_health() {
  echo "Vérification de l'état de santé du cluster..."
  curl --noproxy localhost -s -X GET -H "Accept: application/json" \
    http://$NODE1/api/cluster/health | jq '.'
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
  load-binary)
    load_binary "${2:-$NODE1}" "${3:--1}" "${4:-100000}"
    ;;
  stats)
    stats
    ;;
  stats-consolidated)
    consolidated_stats
    ;;
  count-rows)
    count_parquet_rows
    ;;
  query)
    query_table
    ;;
  group-by)
    query_group_by "$2"
    ;;
  aggregates)
    query_group_by_aggregates "$2" "$3"
    ;;
  delete)
    delete_table "$2"
    ;;
  health)
    check_health
    ;;
  logs)
    tail -f ./target/quarkus-logs/node1.log | grep --color=auto -E "ClusterManager|GroupBy|aggregated|results"
    ;;
  debug)
    echo "Mode debug activé" && set -x && query_group_by_aggregates "${2:-PULocationID}" "${3:-trip_miles}" && set +x
    ;;
  test-small)
    echo "Test avec petit jeu de données..."
    create_table
    load_distributed_parquet small
    query_group_by_aggregates "${2:-PULocationID}" "${3:-trip_miles}"
    ;;
  test-large)
    echo "Test avec grand jeu de données..."
    create_table
    load_distributed_parquet large
    query_group_by_aggregates "${2:-PULocationID}" "${3:-trip_miles}"
    ;;
  test-coordinator)
    echo "Test avec chargement sur le nœud coordonnateur uniquement..."
    load_coordinator_only "${2:-50}"
    query_group_by_aggregates "${3:-PULocationID}" "${4:-trip_miles}"
    ;;
  university-setup)
    echo "Configuration pour environnement universitaire avec machines distinctes..."
    university_setup "${2:-100}" "${3:-$NODE1}" "${4:-$PARQUET_FILE}"
    ;;
  *)
    echo "Usage: ./curl-commands.sh [create|list|load-small|load-large|load-full|stats|stats-consolidated|count-rows|query|group-by|aggregates|delete|health|logs|debug|test-small|test-large|test-coordinator|university-setup]"
    echo ""
    echo "Exemples:"
    echo "  $0 create             - Crée la table parquet_file"
    echo "  $0 list               - Liste toutes les tables sur les 3 nœuds"
    echo "  $0 load-small         - Charge 3 lignes (depuis fichier local)"
    echo "  $0 load-large         - Charge 1000 lignes (depuis fichier local)"
    echo "  $0 load-full          - Charge le fichier complet (depuis fichier local)"
    echo "  $0 stats              - Affiche les statistiques de la table sur tous les nœuds"
    echo "  $0 stats-consolidated - Affiche les statistiques consolidées sur l'ensemble du cluster"
    echo "  $0 count-rows         - Compte le nombre de lignes dans le fichier Parquet sans charger les données"
    echo "  $0 query              - Exécute une requête simple"
    echo "  $0 group-by column    - Exécute une requête avec GROUP BY sur la colonne spécifiée"
    echo "  $0 aggregates col val - Exécute une requête avec GROUP BY et fonctions d'agrégation"
    echo "  $0 delete nom         - Supprime la table spécifiée (par défaut: parquet_file)"
    echo "  $0 health             - Vérifie l'état de santé du cluster"
    ;;
esac
