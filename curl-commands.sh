#!/bin/bash
# Fichier de commandes curl pour MemoryDB
# Exécuter avec: bash curl-commands.sh [commande]

# Configuration
NODE1="localhost:8081"
NODE2="localhost:8082"
NODE3="localhost:8083"
PARQUET_FILE="data/test5.parquet"

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
    {"name": "VendorID", "type": "LONG", "nullable": true},
    {"name": "tpep_pickup_datetime", "type": "LONG", "nullable": true},
    {"name": "tpep_dropoff_datetime", "type": "LONG", "nullable": true},
    {"name": "passenger_count", "type": "LONG", "nullable": true},
    {"name": "trip_distance", "type": "DOUBLE", "nullable": true},
    {"name": "RatecodeID", "type": "LONG", "nullable": true},
    {"name": "store_and_fwd_flag", "type": "STRING", "nullable": true},
    {"name": "PULocationID", "type": "LONG", "nullable": true},
    {"name": "DOLocationID", "type": "LONG", "nullable": true},
    {"name": "payment_type", "type": "LONG", "nullable": true},
    {"name": "fare_amount", "type": "DOUBLE", "nullable": true},
    {"name": "extra", "type": "DOUBLE", "nullable": true},
    {"name": "mta_tax", "type": "DOUBLE", "nullable": true},
    {"name": "tip_amount", "type": "DOUBLE", "nullable": true},
    {"name": "tolls_amount", "type": "DOUBLE", "nullable": true},
    {"name": "improvement_surcharge", "type": "DOUBLE", "nullable": true},
    {"name": "total_amount", "type": "DOUBLE", "nullable": true},
    {"name": "congestion_surcharge", "type": "INTEGER", "nullable": true},
    {"name": "airport_fee", "type": "INTEGER", "nullable": true}
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

# Chargement binaire optimisé pour les environnements universitaires (proxy, quota disque limité)
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
  curl -v --noproxy '*' \
    -X POST \
    -H "Content-Type: application/octet-stream" \
    --data-binary @"$file" \
    "http://$host/api/tables/parquet_file/load-binary?batchSize=200000&rowLimit=20000000&skipRows=0"
  
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

# ========================================
# REQUÊTES ANALYTIQUES BUSINESS INTELLIGENCE
# ========================================

# 📊 Vue d'Ensemble Globale - Statistiques générales du cluster entier
analytics_overview() {
  echo "📊 VUE D'ENSEMBLE GLOBALE - Statistiques consolidées de tout le cluster"
  echo "Agrégation globale sans GROUP BY pour avoir les totaux du cluster..."
  
  curl --noproxy localhost -X POST -H "Content-Type: application/json" \
    -d '{
      "tableName": "parquet_file",
      "columns": [],
      "aggregates": {
        "COUNT_STAR": "COUNT",
        "AVG_fare_amount": "AVG",
        "SUM_total_amount": "SUM",
        "AVG_trip_distance": "AVG",
        "MAX_fare_amount": "MAX",
        "MIN_fare_amount": "MIN",
        "AVG_tip_amount": "AVG",
        "SUM_tip_amount": "SUM"
      },
      "distributed": true
    }' \
    http://$NODE1/api/query | jq '.'
}

# 📋 Échantillon de Données Brutes - Toutes les colonnes sans agrégation
analytics_sample_data() {
  echo "📋 ÉCHANTILLON DE DONNÉES BRUTES - Toutes les colonnes, 100 premières lignes"
  echo "Affichage des données brutes pour voir la structure complète..."
  
  curl --noproxy localhost -X POST -H "Content-Type: application/json" \
    -d '{
      "tableName": "parquet_file",
      "columns": ["*"],
      "conditions": [
        {"columnName": "fare_amount", "operator": "GREATER_THAN", "value": 5}
      ],
      "orderBy": [{"column": "total_amount", "ascending": false}],
      "limit": 100,
      "distributed": true
    }' \
    http://$NODE1/api/query | jq '.'
}

# 💰 Trajets les Plus Chers - WHERE total_amount > 100 ORDER BY total_amount DESC
analytics_expensive_trips() {
  echo "💰 TRAJETS LES PLUS CHERS - Trajets > 100$ triés par montant décroissant"
  echo "Exécution de la requête des trajets les plus chers..."
  
  curl --noproxy localhost -X POST -H "Content-Type: application/json" \
    -d '{
      "tableName": "parquet_file",
      "columns": ["VendorID", "passenger_count", "trip_distance", "fare_amount", "tip_amount", "total_amount", "PULocationID", "DOLocationID"],
      "conditions": [
        {"columnName": "total_amount", "operator": "GREATER_THAN", "value": 50}
      ],
      "orderBy": [{"column": "total_amount", "ascending": false}],
      "distributed": true
    }' \
    http://$NODE1/api/query | jq '.'
}

# 🚕 Performance des Vendeurs - GROUP BY VendorID avec HAVING pour filtrer les vendeurs performants
analytics_vendor_performance() {
  echo "🚕 PERFORMANCE DES VENDEURS - Vendeurs avec >1000 trajets et revenus moyens >15$"
  echo "Utilisation de HAVING pour filtrer les vendeurs performants..."
  
  curl --noproxy localhost -X POST -H "Content-Type: application/json" \
    -d '{
      "tableName": "parquet_file",
      "columns": ["VendorID"],
      "conditions": [
        {"columnName": "VendorID", "operator": "IN", "value": [1, 2, 3, 4, 5]}
      ],
      "groupBy": ["VendorID"],
      "aggregates": {
        "COUNT_STAR": "COUNT",
        "SUM_total_amount": "SUM",
        "AVG_fare_amount": "AVG",
        "AVG_tip_amount": "AVG",
        "AVG_trip_distance": "AVG"
      },
      "havingConditions": [
        {"columnName": "COUNT_STAR", "operator": "GREATER_THAN", "value": 1000},
        {"columnName": "AVG_fare_amount", "operator": "GREATER_THAN", "value": 15}
      ],
      "orderBy": [{"column": "SUM_total_amount", "ascending": false}],
      "distributed": true
    }' \
    http://$NODE1/api/query | jq '.'
}

# 📍 Top Zones de Pickup - Zones premium avec HAVING et BETWEEN pour filtrer les zones rentables
analytics_pickup_zones() {
  echo "📍 TOP ZONES DE PICKUP PREMIUM - Zones avec >500 trajets et revenus moyens entre 20-100$"
  echo "Utilisation de HAVING et BETWEEN pour identifier les zones premium..."
  
  curl --noproxy localhost -X POST -H "Content-Type: application/json" \
    -d '{
      "tableName": "parquet_file",
      "columns": ["PULocationID"],
      "conditions": [
        {"columnName": "PULocationID", "operator": "IS_NOT_NULL"},
        {"columnName": "PULocationID", "operator": "BETWEEN", "value": [1, 300]}
      ],
      "groupBy": ["PULocationID"],
      "aggregates": {
        "COUNT_STAR": "COUNT",
        "SUM_total_amount": "SUM",
        "AVG_fare_amount": "AVG",
        "AVG_trip_distance": "AVG",
        "MAX_fare_amount": "MAX"
      },
      "havingConditions": [
        {"columnName": "COUNT_STAR", "operator": "GREATER_THAN", "value": 500},
        {"columnName": "AVG_fare_amount", "operator": "BETWEEN", "value": [20, 100]}
      ],
      "orderBy": [{"column": "SUM_total_amount", "ascending": false}],
      "distributed": true
    }' \
    http://$NODE1/api/query | jq '.'
}

# 💳 Analyse Paiements Premium - Types de paiement avec HAVING pour identifier les segments rentables
analytics_payment_analysis() {
  echo "💳 ANALYSE PAIEMENTS PREMIUM - Types avec >10000 transactions et pourboires moyens >2$"
  echo "Utilisation de HAVING et IN pour analyser les segments de paiement rentables..."
  
  curl --noproxy localhost -X POST -H "Content-Type: application/json" \
    -d '{
      "tableName": "parquet_file",
      "columns": ["payment_type"],
      "conditions": [
        {"columnName": "payment_type", "operator": "IN", "value": [1, 2, 3, 4, 5]},
        {"columnName": "tip_amount", "operator": "GREATER_THAN_OR_EQUALS", "value": 0}
      ],
      "groupBy": ["payment_type"],
      "aggregates": {
        "COUNT_STAR": "COUNT",
        "SUM_total_amount": "SUM",
        "AVG_fare_amount": "AVG",
        "AVG_tip_amount": "AVG",
        "SUM_tip_amount": "SUM"
      },
      "havingConditions": [
        {"columnName": "COUNT_STAR", "operator": "GREATER_THAN", "value": 10000},
        {"columnName": "AVG_tip_amount", "operator": "GREATER_THAN", "value": 2}
      ],
      "orderBy": [{"column": "SUM_total_amount", "ascending": false}],
      "distributed": true
    }' \
    http://$NODE1/api/query | jq '.'
}

# 👥 Analyse Groupes Rentables - Groupes de passagers avec HAVING et BETWEEN pour identifier les segments premium
analytics_passenger_analysis() {
  echo "👥 ANALYSE GROUPES RENTABLES - Groupes avec >5000 trajets et revenus moyens >25$"
  echo "Utilisation de BETWEEN et HAVING pour identifier les groupes de passagers premium..."
  
  curl --noproxy localhost -X POST -H "Content-Type: application/json" \
    -d '{
      "tableName": "parquet_file",
      "columns": ["passenger_count"],
      "conditions": [
        {"columnName": "passenger_count", "operator": "BETWEEN", "value": [1, 6]},
        {"columnName": "fare_amount", "operator": "GREATER_THAN", "value": 5}
      ],
      "groupBy": ["passenger_count"],
      "aggregates": {
        "COUNT_STAR": "COUNT",
        "AVG_fare_amount": "AVG",
        "AVG_tip_amount": "AVG",
        "SUM_total_amount": "SUM",
        "AVG_trip_distance": "AVG"
      },
      "havingConditions": [
        {"columnName": "COUNT_STAR", "operator": "GREATER_THAN", "value": 5000},
        {"columnName": "AVG_fare_amount", "operator": "GREATER_THAN", "value": 25}
      ],
      "orderBy": [{"column": "AVG_fare_amount", "ascending": false}],
      "distributed": true
    }' \
    http://$NODE1/api/query | jq '.'
}

# 📏 Analyse Trajets Longue Distance - Trajets premium avec HAVING pour identifier les routes rentables
analytics_distance_analysis() {
  echo "📏 ANALYSE TRAJETS LONGUE DISTANCE - Routes avec >100 trajets et tarifs moyens >30$"
  echo "Utilisation de BETWEEN et HAVING pour analyser les trajets longue distance rentables..."
  
  curl --noproxy localhost -X POST -H "Content-Type: application/json" \
    -d '{
      "tableName": "parquet_file",
      "columns": ["VendorID"],
      "conditions": [
        {"columnName": "trip_distance", "operator": "BETWEEN", "value": [5, 50]},
        {"columnName": "fare_amount", "operator": "GREATER_THAN", "value": 10},
        {"columnName": "VendorID", "operator": "IN", "value": [1, 2, 3, 4]}
      ],
      "groupBy": ["VendorID"],
      "aggregates": {
        "total_trips": "COUNT",
        "avg_fare": "AVG",
        "total_revenue": "SUM",
        "avg_distance": "AVG",
        "fare_per_mile": "AVG",
        "max_distance": "MAX"
      },
      "aggregateColumns": {
        "avg_fare": "fare_amount",
        "total_revenue": "total_amount",
        "avg_distance": "trip_distance",
        "fare_per_mile": "fare_amount",
        "max_distance": "trip_distance"
      },
      "havingConditions": [
        {"columnName": "total_trips", "operator": "GREATER_THAN", "value": 100},
        {"columnName": "avg_fare", "operator": "GREATER_THAN", "value": 30},
        {"columnName": "avg_distance", "operator": "GREATER_THAN", "value": 8}
      ],
      "orderBy": [{"column": "fare_per_mile", "ascending": false}],
      "distributed": true
    }' \
    http://$NODE1/api/query | jq '.'
}

# 🏆 Analyse Segments VIP - Combinaison avancée de tous les nouveaux opérateurs SQL
analytics_vip_segments() {
  echo "🏆 ANALYSE SEGMENTS VIP - Zones premium avec opérateurs SQL avancés"
  echo "Utilisation de IN, BETWEEN, HAVING pour identifier les segments VIP ultra-rentables..."
  
  curl --noproxy localhost -X POST -H "Content-Type: application/json" \
    -d '{
      "tableName": "parquet_file",
      "columns": ["PULocationID", "payment_type"],
      "conditions": [
        {"columnName": "PULocationID", "operator": "IN", "value": [1, 4, 7, 13, 48, 50, 68, 79, 87, 90, 100, 107, 113, 114, 125, 127, 128, 140, 141, 142, 148, 151, 152, 158, 161, 162, 163, 164, 166, 170, 186, 194, 202, 209, 211, 224, 229, 230, 231, 232, 233, 234, 236, 237, 238, 239, 243, 244, 246, 249, 261, 262, 263]},
        {"columnName": "payment_type", "operator": "IN", "value": [1, 2]},
        {"columnName": "fare_amount", "operator": "BETWEEN", "value": [15, 200]},
        {"columnName": "tip_amount", "operator": "GREATER_THAN", "value": 3},
        {"columnName": "passenger_count", "operator": "BETWEEN", "value": [1, 4]}
      ],
      "groupBy": ["PULocationID", "payment_type"],
      "aggregates": {
        "vip_trips": "COUNT",
        "total_vip_revenue": "SUM",
        "avg_vip_fare": "AVG",
        "avg_vip_tip": "AVG",
        "avg_vip_distance": "AVG",
        "vip_tip_ratio": "AVG"
      },
      "aggregateColumns": {
        "total_vip_revenue": "total_amount",
        "avg_vip_fare": "fare_amount",
        "avg_vip_tip": "tip_amount",
        "avg_vip_distance": "trip_distance",
        "vip_tip_ratio": "tip_amount"
      },
      "havingConditions": [
        {"columnName": "vip_trips", "operator": "GREATER_THAN", "value": 200},
        {"columnName": "avg_vip_fare", "operator": "BETWEEN", "value": [25, 150]},
        {"columnName": "avg_vip_tip", "operator": "GREATER_THAN", "value": 5},
        {"columnName": "total_vip_revenue", "operator": "GREATER_THAN", "value": 10000}
      ],
      "orderBy": [{"column": "total_vip_revenue", "ascending": false}],
      "limit": 20,
      "distributed": true
    }' \
    http://$NODE1/api/query | jq '.'
}

# ⚡ Benchmark de Performance - Test sur l'ensemble des données
analytics_performance_benchmark() {
  echo "⚡ BENCHMARK DE PERFORMANCE - Test sur l'ensemble des données"
  echo "Exécution du benchmark de performance..."
  
  curl --noproxy localhost -X POST -H "Content-Type: application/json" \
    -d '{
      "tableName": "parquet_file",
      "columns": ["*"],
      "conditions": [
        {"columnName": "trip_distance", "operator": "GREATER_THAN", "value": 0},
        {"columnName": "fare_amount", "operator": "GREATER_THAN", "value": 0}
      ],
      "aggregates": {
        "total_trips": "COUNT",
        "avg_distance": "AVG",
        "max_distance": "MAX",
        "avg_fare_per_mile": "AVG",
        "total_revenue": "SUM"
      },
      "aggregateColumns": {
        "avg_distance": "trip_distance",
        "max_distance": "trip_distance",
        "avg_fare_per_mile": "fare_amount",
        "total_revenue": "total_amount"
      },
      "distributed": true
    }' \
    http://$NODE1/api/query | jq '.'
}

# ========================================
# FONCTIONS D'AIDE ET UTILITAIRES
# ========================================

# Afficher l'aide avec toutes les commandes disponibles
show_help() {
  echo "=== COMMANDES MEMORYDB ==="
  echo ""
  echo "📋 GESTION DES TABLES:"
  echo "  create_table          - Créer la table parquet_file"
  echo "  list_tables          - Lister toutes les tables"
  echo "  stats                - Afficher les statistiques"
  echo ""
  echo "📥 CHARGEMENT DE DONNÉES:"
  echo "  load_binary          - Chargement binaire optimisé"
  echo ""
  echo "📊 REQUÊTES ANALYTIQUES AVANCÉES:"
  echo "  analytics_overview           - 📊 Vue d'ensemble globale (agrégations)"
  echo "  analytics_sample_data        - 📋 Échantillon de données brutes"
  echo "  analytics_expensive_trips    - 💰 Trajets les plus chers"
  echo "  analytics_vendor_performance - 🚕 Vendeurs performants (HAVING)"
  echo "  analytics_pickup_zones       - 📍 Zones premium (BETWEEN + HAVING)"
  echo "  analytics_payment_analysis   - 💳 Paiements rentables (IN + HAVING)"
  echo "  analytics_passenger_analysis - 👥 Groupes rentables (BETWEEN + HAVING)"
  echo "  analytics_distance_analysis  - 📏 Trajets longue distance (IN + HAVING)"
  echo "  analytics_vip_segments       - 🏆 Segments VIP (tous opérateurs)"
  echo "  analytics_performance_benchmark - ⚡ Benchmark de performance"
  echo ""
  echo "🧪 TESTS DES NOUVEAUX OPÉRATEURS SQL:"
  echo "  test_string_operators        - 🔍 Test CONTAINS, STARTS_WITH, ENDS_WITH"
  echo "  test_in_operator            - 📊 Test opérateur IN"
  echo "  test_between_operator       - 🎯 Test opérateur BETWEEN"
  echo "  test_having_clause          - 🔥 Test clause HAVING"
  echo ""
  echo "🔧 UTILITAIRES:"
  echo "  help                 - Afficher cette aide"
  echo ""
  echo "💡 EXEMPLES D'USAGE:"
  echo "  ./curl-commands.sh create_table"
  echo "  ./curl-commands.sh load_binary"
  echo "  ./curl-commands.sh analytics_overview"
  echo "  ./curl-commands.sh analytics_vendor_performance"
  echo ""
  echo "📁 CONFIGURATION:"
  echo "  PARQUET_FILE=$PARQUET_FILE"
  echo "  NODE1=$NODE1"
  echo "  NODE2=$NODE2"
  echo "  NODE3=$NODE3"
}

# Exécuter toutes les requêtes analytiques en séquence
run_all_analytics() {
  echo "🚀 EXÉCUTION DE TOUTES LES REQUÊTES ANALYTIQUES"
  echo "================================================"
  echo ""
  
  echo "1/8 - Vue d'ensemble globale..."
  analytics_overview
  echo ""
  
  echo "2/8 - Trajets les plus chers..."
  analytics_expensive_trips
  echo ""
  
  echo "3/8 - Performance des vendeurs..."
  analytics_vendor_performance
  echo ""
  
  echo "4/8 - Top zones de pickup..."
  analytics_pickup_zones
  echo ""
  
  echo "5/8 - Analyse des paiements..."
  analytics_payment_analysis
  echo ""
  
  echo "6/8 - Analyse par passagers..."
  analytics_passenger_analysis
  echo ""
  
  echo "7/8 - Analyse des distances..."
  analytics_distance_analysis
  echo ""
  
  echo "8/9 - Segments VIP..."
  analytics_vip_segments
  echo ""
  
  echo "9/9 - Benchmark de performance..."
  analytics_performance_benchmark
  echo ""
  
  echo "✅ TOUTES LES REQUÊTES ANALYTIQUES TERMINÉES"
}

# ========================================
# POINT D'ENTRÉE PRINCIPAL
# ========================================

# Si aucun argument n'est fourni, afficher l'aide
if [ $# -eq 0 ]; then
  show_help
  exit 0
fi

# Exécuter la commande demandée
case "$1" in
  "create_table")
    create_table
    ;;
  "list_tables")
    list_tables
    ;;
  "load_binary")
    load_binary "$2" "$3" "$4" "$5"
    ;;
  "stats")
    stats
    ;;
  "analytics_overview")
    analytics_overview
    ;;
  "analytics_sample_data")
    analytics_sample_data
    ;;
  "analytics_expensive_trips")
    analytics_expensive_trips
    ;;
  "analytics_vendor_performance")
    analytics_vendor_performance
    ;;
  "analytics_pickup_zones")
    analytics_pickup_zones
    ;;
  "analytics_payment_analysis")
    analytics_payment_analysis
    ;;
  "analytics_passenger_analysis")
    analytics_passenger_analysis
    ;;
  "analytics_distance_analysis")
    analytics_distance_analysis
    ;;
  "analytics_performance_benchmark")
    analytics_performance_benchmark
    ;;
  "analytics_vip_segments")
    analytics_vip_segments
    ;;
  "test_string_operators")
    test_string_operators
    ;;
  "test_in_operator")
    test_in_operator
    ;;
  "test_between_operator")
    test_between_operator
    ;;
  "test_having_clause")
    test_having_clause
    ;;
  "run_all_analytics")
    run_all_analytics
    ;;
  "help")
    show_help
    ;;
  *)
    echo "❌ Commande inconnue: $1"
    echo ""
    show_help
    exit 1
    ;;
esac

# ========================================
# NOUVELLES FONCTIONS DE TEST
# ========================================

# 🔍 Test des nouveaux opérateurs CONTAINS, STARTS_WITH, ENDS_WITH
test_string_operators() {
  echo "🔍 TEST DES NOUVEAUX OPÉRATEURS DE CHAÎNES"
  echo "Test de l'opérateur CONTAINS..."
  
  curl --noproxy localhost -X POST -H "Content-Type: application/json" \
    -d '{
      "tableName": "parquet_file",
      "columns": ["VendorID", "payment_type"],
      "conditions": [
        {"columnName": "payment_type", "operator": "CONTAINS", "value": "1"}
      ],
      "limit": 100
    }' \
    "http://$NODE1/api/query" | jq '.'
}

# 📊 Test de l'opérateur IN
test_in_operator() {
  echo "📊 TEST DE L'OPÉRATEUR IN"
  echo "Test avec VendorID IN (1, 2)..."
  
  curl --noproxy localhost -X POST -H "Content-Type: application/json" \
    -d '{
      "tableName": "parquet_file",
      "columns": ["VendorID"],
      "conditions": [
        {"columnName": "VendorID", "operator": "IN", "value": [1, 2]}
      ],
      "groupBy": ["VendorID"],
      "aggregates": {"count": "COUNT"},
      "aggregateColumns": {"count": "*"},
      "limit": 10
    }' \
    "http://$NODE1/api/query" | jq '.'
}

# 🎯 Test de l'opérateur BETWEEN
test_between_operator() {
  echo "🎯 TEST DE L'OPÉRATEUR BETWEEN"
  echo "Test avec fare_amount BETWEEN 10 AND 50..."
  
  curl --noproxy localhost -X POST -H "Content-Type: application/json" \
    -d '{
      "tableName": "parquet_file",
      "columns": ["trip_distance", "fare_amount"],
      "conditions": [
        {"columnName": "fare_amount", "operator": "BETWEEN", "value": [10, 50]}
      ],
      "limit": 100
    }' \
    "http://$NODE1/api/query" | jq '.'
}

# 🔥 Test de la clause HAVING avec agrégations
test_having_clause() {
  echo "🔥 TEST DE LA CLAUSE HAVING AVEC AGRÉGATIONS"
  echo "Test avec HAVING total_trips > 1000 AND avg_fare > 10..."
  
  curl --noproxy localhost -X POST -H "Content-Type: application/json" \
    -d '{
      "tableName": "parquet_file",
      "columns": ["VendorID"],
      "groupBy": ["VendorID"],
      "aggregates": {"total_trips": "COUNT", "avg_fare": "AVG"},
      "aggregateColumns": {"total_trips": "*", "avg_fare": "fare_amount"},
      "havingConditions": [
        {"columnName": "total_trips", "operator": "GREATER_THAN", "value": 1000},
        {"columnName": "avg_fare", "operator": "GREATER_THAN", "value": 10}
      ],
      "limit": 10
    }' \
    "http://$NODE1/api/query" | jq '.'
}
