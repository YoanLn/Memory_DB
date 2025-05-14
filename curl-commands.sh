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
  curl -X POST \
    -F "file=@$PARQUET_FILE" \
    -F "rowLimit=3" \
    -F "batchSize=1000" \
    http://$NODE1/api/tables/parquet_file/load-distributed-upload
}

# Chargement distribué - Grand nombre de lignes
load_distributed_large() {
  echo "Chargement distribué avec un grand nombre de lignes (1000)..."
  curl -X POST \
    -F "file=@$PARQUET_FILE" \
    -F "rowLimit=1000" \
    -F "batchSize=100000" \
    http://$NODE1/api/tables/parquet_file/load-distributed-upload
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
  curl -X DELETE http://$NODE1/api/tables/parquet_file
  sleep 2
  
  # 2. Recréer la table sur tous les nœuds
  create_table
  sleep 1
  
  # 3. Charger les données uniquement sur le nœud coordonnateur (nœud 1)
  echo "Chargement des données uniquement sur le nœud coordonnateur..."
  
  # Utilisation de l'option -F avec le chemin absolu complet
  echo "Exécution de: curl -X POST -F \"file=@$file_path\" [...] http://$NODE1/api/tables/parquet_file/load"
  curl -X POST \
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
  curl -X POST \
    -F "file=@$PARQUET_FILE" \
    -F "rowLimit=-1" \
    -F "batchSize=100000" \
    http://$NODE1/api/tables/parquet_file/load-distributed-upload
}

# Solution pour configuration universitaire avec 2 machines Linux
# Où une seule machine a le fichier Parquet localement
university_setup() {
  # Paramètres
  local row_limit="${1:-100}"
  local file_machine="${2:-$NODE1}"  # L'adresse de la machine avec le fichier (par défaut NODE1)
  local file_path="${3:-$PARQUET_FILE}"  # Chemin du fichier sur la machine source
  
  echo "======================================================================="
  echo "Configuration pour environnement universitaire avec machines distinctes"
  echo "======================================================================="
  echo "Machine avec le fichier: $file_machine"
  echo "Chemin du fichier: $file_path"
  echo "Nombre de lignes à charger: $row_limit"
  echo ""
  
  # 1. Vérifie si le fichier existe (uniquement si l'on est sur la machine avec le fichier)
  local is_file_machine=false
  if [[ "$file_machine" == "localhost:"* || "$file_machine" == "127.0.0.1:"* ]]; then
    is_file_machine=true
    if [ ! -f "$file_path" ]; then
      echo "ERREUR: Le fichier Parquet n'existe pas à l'emplacement: $file_path"
      return 1
    else
      echo "Fichier trouvé localement: $file_path"
      # Vérifie la taille du fichier
      local file_size=$(du -h "$file_path" | cut -f1)
      echo "Taille du fichier: $file_size"
    fi
  else
    echo "Supposant que le fichier existe sur la machine distante: $file_machine"
  fi
  
  echo ""
  echo "1. Suppression de la table sur tous les nœuds..."
  curl -X DELETE http://$NODE1/api/tables/parquet_file
  curl -X DELETE http://$NODE2/api/tables/parquet_file
  if [ "$NODE3" != "" ]; then
    curl -X DELETE http://$NODE3/api/tables/parquet_file
  fi
  sleep 2
  
  echo ""
  echo "2. Création de la table sur tous les nœuds..."
  create_table
  sleep 1
  
  echo ""
  echo "3. Chargement des données uniquement sur la machine avec le fichier ($file_machine)..."
  # Construit l'URL pour la machine avec le fichier
  local load_url="http://$file_machine/api/tables/parquet_file/load-distributed-upload"
  
  local load_success=false
  
  # Utiliser une commande curl appropriée selon que nous sommes sur la machine avec le fichier ou non
  if [ "$is_file_machine" = true ]; then
    # Nous sommes sur la machine avec le fichier, on peut faire un curl direct avec @file
    echo "Exécution de: curl -X POST -F \"file=@$file_path\" -F \"rowLimit=$row_limit\" [...] $load_url"
    curl_output=$(curl -X POST \
      -F "file=@$file_path" \
      -F "rowLimit=$row_limit" \
      -F "batchSize=1000" \
      -s $load_url)
      
    echo "$curl_output"
    
    # Vérifie si le chargement a réussi en analysant la sortie JSON
    if echo "$curl_output" | grep -q """rowsLoaded"""; then
      load_success=true
    fi
  else
    # Nous sommes sur une autre machine, afficher les instructions
    echo "ATTENTION: Vous n'êtes pas sur la machine avec le fichier."
    echo "Pour que cela fonctionne correctement, exécutez cette commande sur la machine qui a le fichier:"
    echo ""
    echo "  curl -X POST \\"
    echo "    -F \"file=@$file_path\" \\"
    echo "    -F \"rowLimit=$row_limit\" \\"
    echo "    -F \"batchSize=1000\" \\"
    echo "    http://localhost:$(echo $file_machine | cut -d':' -f2)/api/tables/parquet_file/load-distributed-upload"
    echo ""
    echo "Une fois cette commande exécutée sur la machine avec le fichier, vous pourrez"
    echo "exécuter des requêtes distribuées depuis n'importe quelle machine."
    load_success=false
  fi
  
  echo -e "\nRemarque: Les données doivent être chargées uniquement sur la machine avec le fichier."
  echo "Les requêtes seront exécutées de manière distribuée entre les nœuds."
  echo ""
  
  # 4. Vérification et exécution d'une requête de test
  echo "4. Vérification des statistiques sur chaque nœud:"
  echo "Statistiques sur nœud 1:"
  node1_stats=$(curl -s http://$NODE1/api/tables/parquet_file/stats)
  echo "$node1_stats"
  echo ""
  echo "Statistiques sur nœud 2:"
  curl -s http://$NODE2/api/tables/parquet_file/stats
  
  # 5. Exécute une requête de test si le chargement a réussi
  if [ "$is_file_machine" = true ] && [ "$load_success" = true ]; then
    echo ""
    echo "5. Exécution d'une requête de test distribuée:"
    echo "Requête GROUP BY PULocationID avec agrégation sur trip_miles..."
    
    # Extrait le nombre de lignes pour vérifier si des données ont été chargées
    row_count=$(echo "$node1_stats" | grep -o '"rowCount":[0-9]*' | cut -d':' -f2)
    
    if [ "$row_count" -gt 0 ]; then
      query_group_by_aggregates "PULocationID" "trip_miles"
    else
      echo "ATTENTION: Aucune ligne n'a été chargée dans la table. La requête de test est ignorée."
      echo "Vérifiez les journaux du serveur pour plus de détails sur ce qui a pu se passer."
    fi
  fi
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
      "tableName": "parquet_file",
      "columns": ["*"],
      "limit": 10,
      "distributed": true
    }' \
    http://$NODE1/api/query | jq '.'
}

# Requête de test avec GROUP BY
query_group_by() {
  local column="${1:-group_column}"
  local distributed="${2:-true}"
  echo "Exécution d'une requête GROUP BY sur la colonne $column (distributed=$distributed)..."
  
  # Print the full curl command for diagnostics
  echo "Curl command:"
  echo "curl -X POST -H 'Content-Type: application/json' \
    -d '{
      \"tableName\": \"parquet_file\",
      \"columns\": [\"$column\"],
      \"groupBy\": [\"$column\"],
      \"aggregates\": {\"count\": \"COUNT\"},
      \"limit\": 10,
      \"distributed\": $distributed
    }' \
    http://$NODE1/api/query"
    
  # Execute the actual curl command
  curl -v -X POST -H "Content-Type: application/json" \
    -d "{
      \"tableName\": \"parquet_file\",
      \"columns\": [\"$column\"],
      \"groupBy\": [\"$column\"],
      \"aggregates\": {\"count\": \"COUNT\"},
      \"limit\": 10,
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
      \"limit\": 100,
      \"distributed\": true
    }"
  
  echo "Requête JSON:"
  echo "$query_json" | jq '.'
  
  # Exécute la requête
  echo "Résultat de la requête:"
  curl -s -X POST -H "Content-Type: application/json" \
    -d "$query_json" \
    http://$NODE1/api/query | jq '.'
}

# Supprimer une table
delete_table() {
  local table_name="${1:-parquet_file}"
  echo "Suppression de la table $table_name..."
  curl -X DELETE http://$NODE1/api/tables/$table_name
}

# Vérifier l'état de santé du cluster
check_health() {
  echo "Vérification de l'état de santé du cluster..."
  curl -s -X GET -H "Accept: application/json" \
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
  stats)
    check_stats
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
    echo "Usage: $0 [create|list|load-small|load-large|load-full|stats|query|group-by|aggregates|delete|health|logs|debug|test-small|test-large|test-coordinator|university-setup]"
    echo ""
    echo "Exemples:"
    echo "  $0 create             - Crée la table parquet_file"
    echo "  $0 list               - Liste toutes les tables sur les 3 nœuds"
    echo "  $0 load-small         - Charge 3 lignes (depuis fichier local)"
    echo "  $0 load-large         - Charge 1000 lignes (depuis fichier local)"
    echo "  $0 load-full          - Charge le fichier complet (depuis fichier local)"
    echo "  $0 stats              - Affiche les statistiques de la table sur tous les nœuds"
    echo "  $0 query              - Exécute une requête simple"
    echo "  $0 group-by column    - Exécute une requête avec GROUP BY sur la colonne spécifiée"
    echo "  $0 aggregates col val - Exécute une requête avec GROUP BY et fonctions d'agrégation"
    echo "  $0 delete nom         - Supprime la table spécifiée (par défaut: parquet_file)"
    echo "  $0 health             - Vérifie l'état de santé du cluster"
    ;;
esac
