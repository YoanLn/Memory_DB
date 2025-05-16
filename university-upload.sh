#!/bin/bash
# Script simplifié pour l'environnement universitaire Linux
# Ce script facilite le chargement et l'interrogation des données Parquet
# dans un environnement où les machines n'ont pas accès aux mêmes fichiers

# Configuration par défaut
NODE1="localhost:8080"  # Adresse du nœud principal
NODE2="localhost:8081"  # Adresse du deuxième nœud 
NODE3=""                # Laisser vide si pas de troisième nœud
ROW_LIMIT=100000        # Nombre de lignes à charger par défaut

# Fonction d'aide
show_help() {
  echo "Usage: $0 <command> [options]"
  echo ""
  echo "Commands:"
  echo "  create <table_name>                - Crée une nouvelle table"
  echo "  upload <table_name> <file_path>    - Charge un fichier Parquet (machine avec fichier)"
  echo "  query <table_name>                 - Exécute une requête simple"
  echo "  stats <table_name>                 - Affiche les statistiques de la table"
  echo "  delete <table_name>                - Supprime une table"
  echo ""
  echo "Options:"
  echo "  --limit N     - Limite le nombre de lignes à charger (par défaut: $ROW_LIMIT)"
  echo "  --nodes       - Affiche les nœuds configurés"
  echo ""
  echo "Exemples:"
  echo "  $0 create Table                   - Crée une table nommée 'Table'"
  echo "  $0 upload Table /path/to/file.parquet - Charge des données dans la table 'Table'"
  echo "  $0 query Table                    - Exécute une requête simple sur la table"
  echo "  $0 upload Table /path/file.parquet --limit 1000 - Limite à 1000 lignes"
  echo ""
  echo "Note: Ce script doit être exécuté sur la machine qui a accès au fichier Parquet."
}

# Affiche les nœuds configurés
show_nodes() {
  echo "Nœuds configurés:"
  echo "  Nœud 1: $NODE1"
  echo "  Nœud 2: $NODE2"
  if [ -n "$NODE3" ]; then
    echo "  Nœud 3: $NODE3"
  fi
}

# Crée une table (structure générique)
create_table() {
  if [ -z "$1" ]; then
    echo "Erreur: Nom de table requis"
    echo "Usage: $0 create <table_name>"
    return 1
  fi
  
  local table_name="$1"
  echo "Création de la table $table_name..."
  
  curl --noproxy localhost --location "http://$NODE1/api/table" \
    --header 'Content-Type: application/json' \
    --data '{
      "name": "'$table_name'",
      "columns": {
        "surcharge": {"values": [],"type": "DOUBLE"},
        "Total_Amt": {"values": [],"type": "DOUBLE"},
        "Trip_Dropoff_DateTime": {"values": [],"type": "BINARY"},
        "Start_Lon": {"values": [],"type": "DOUBLE"},
        "Fare_Amt": {"values": [],"type": "DOUBLE"},
        "Tolls_Amt": {"values": [],"type": "DOUBLE"},
        "Rate_Code": {"values": [],"type": "DOUBLE"},
        "vendor_name": {"values": [],"type": "BINARY"},
        "Tip_Amt": {"values": [],"type": "DOUBLE"},
        "End_Lat": {"values": [],"type": "DOUBLE"},
        "Payment_Type": {"values": [],"type": "BINARY"},
        "Start_Lat": {"values": [],"type": "DOUBLE"},
        "store_and_forward": {"values": [],"type": "DOUBLE"},
        "Trip_Distance": {"values": [],"type": "DOUBLE"},
        "End_Lon": {"values": [],"type": "DOUBLE"},
        "Passenger_Count": {"values": [],"type": "INT64"},
        "mta_tax": {"values": [],"type": "DOUBLE"},
        "Trip_Pickup_DateTime": {"values": [],"type": "BINARY"}
      }
    }'
}

# Charge un fichier Parquet
upload_file() {
  if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Erreur: Nom de table et chemin du fichier requis"
    echo "Usage: $0 upload <table_name> <file_path> [--limit N]"
    return 1
  fi
  
  local table_name="$1"
  local file_path="$2"
  local limit="$ROW_LIMIT"
  
  # Traite l'option --limit si présente
  if [ "$3" = "--limit" ] && [ -n "$4" ]; then
    limit="$4"
  fi
  
  # Vérifie si le fichier existe
  if [ ! -f "$file_path" ]; then
    echo "Erreur: Fichier non trouvé: $file_path"
    return 1
  fi
  
  echo "Chargement du fichier: $file_path"
  echo "Table: $table_name"
  echo "Limite de lignes: $limit"
  
  # Affiche la taille du fichier
  local file_size=$(du -h "$file_path" | cut -f1)
  echo "Taille du fichier: $file_size"
  
  # Utilise l'API d'upload simple avec --noproxy
  echo "Exécution du chargement..."
  curl --noproxy localhost --location "http://$NODE1/api/upload?tableName=$table_name&limite=$limit" \
    --header 'Content-Type: application/octet-stream' \
    --data-binary "@$file_path"
    
  echo -e "\nVérification des statistiques après chargement:"
  show_stats "$table_name"
}

# Affiche les statistiques d'une table
show_stats() {
  if [ -z "$1" ]; then
    echo "Erreur: Nom de table requis"
    echo "Usage: $0 stats <table_name>"
    return 1
  fi
  
  local table_name="$1"
  
  echo "Statistiques de la table $table_name sur nœud 1:"
  curl --noproxy localhost --location "http://$NODE1/api/tables/$table_name/stats"
  
  echo -e "\nStatistiques de la table $table_name sur nœud 2:"
  curl --noproxy localhost --location "http://$NODE2/api/tables/$table_name/stats"
  
  if [ -n "$NODE3" ]; then
    echo -e "\nStatistiques de la table $table_name sur nœud 3:"
    curl --noproxy localhost --location "http://$NODE3/api/tables/$table_name/stats"
  fi
}

# Exécute une requête simple
query_table() {
  if [ -z "$1" ]; then
    echo "Erreur: Nom de table requis"
    echo "Usage: $0 query <table_name>"
    return 1
  fi
  
  local table_name="$1"
  
  echo "Exécution d'une requête simple sur la table $table_name:"
  curl --noproxy localhost --location "http://$NODE1/api/query" \
    --header 'Content-Type: application/json' \
    --data '{
      "tableName": "'$table_name'",
      "columns": ["*"],
      "limit": 10,
      "distributed": true
    }'
}

# Supprime une table
delete_table() {
  if [ -z "$1" ]; then
    echo "Erreur: Nom de table requis"
    echo "Usage: $0 delete <table_name>"
    return 1
  fi
  
  local table_name="$1"
  
  echo "Suppression de la table $table_name..."
  curl --noproxy localhost -X DELETE "http://$NODE1/api/table/$table_name"
  
  # Si la méthode ci-dessus ne fonctionne pas, essayez l'alternative
  echo "\nSi la méthode ci-dessus ne fonctionne pas, essayez:\n"
  echo "curl --noproxy localhost -X DELETE \"http://$NODE1/api/tables/$table_name\""
}

# Traitement des arguments
case "$1" in
  create)
    create_table "$2"
    ;;
  upload)
    upload_file "$2" "$3" "$4" "$5"
    ;;
  query)
    query_table "$2"
    ;;
  stats)
    show_stats "$2"
    ;;
  delete)
    delete_table "$2"
    ;;
  --nodes)
    show_nodes
    ;;
  *)
    show_help
    ;;
esac
