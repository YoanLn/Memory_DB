#!/bin/bash
# Script optimisé pour environnement universitaire avec quota disque limité
# Utilise des fichiers de petite taille et des variables d'environnement pour le contrôle

# Configuration
NODE="localhost:8080"  # Adresse du nœud principal (ajuster si nécessaire)
NODE2="localhost:8081" # Deuxième nœud si disponible
TEMP_DIR="/tmp"       # Utilise /tmp qui n'est pas sous quota utilisateur
MAX_ROWS=1000         # Nombre de lignes par défaut

show_help() {
  echo "Usage: $0 <command> [options]"
  echo ""
  echo "Commandes optimisées pour environnement avec quota disque limité:"
  echo "  create <table>             - Crée une nouvelle table"
  echo "  upload <url> <table> <max> - Charge un fichier distant sans le télécharger"
  echo "  stats <table>              - Affiche les statistiques de la table"
  echo "  query <table>              - Exécute une requête simple"
  echo "  delete <table>             - Supprime une table"
  echo ""
  echo "Exemples:"
  echo "  $0 create mytable          - Crée une table nommée 'mytable'"
  echo "  $0 upload http://exemple.com/data.parquet mytable 1000"
  echo "  $0 stats mytable           - Affiche les statistiques de la table"
}

# Fonction pour créer une table simple (générique)
create_table() {
  if [ -z "$1" ]; then
    echo "Erreur: Nom de table requis"
    return 1
  fi
  
  local table_name="$1"
  echo "Création de la table $table_name..."
  
  # Utilise l'endpoint exact qui fonctionne dans ton exemple
  curl --noproxy localhost --location 'http://$NODE/api/table' \
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

# Fonction pour charger un fichier DIRECTEMENT depuis une URL
# Évite de télécharger le fichier localement pour économiser l'espace disque
upload_from_url() {
  if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Erreur: URL et nom de table requis"
    echo "Usage: $0 upload <url_fichier> <nom_table> [max_lignes]"
    return 1
  fi
  
  local file_url="$1"
  local table_name="$2"
  local max_rows="${3:-$MAX_ROWS}"
  
  echo "Chargement depuis URL: $file_url"
  echo "Vers la table: $table_name"
  echo "Nombre max de lignes: $max_rows"
  
  # Utilise curl pour récupérer le contenu directement depuis l'URL et le transmet directement à l'API
  # Format exact de la commande qui fonctionne dans ton exemple
  echo "Transmission directe depuis l'URL vers l'API (pas de stockage local)..."
  curl --noproxy localhost --location "$file_url" | \
    curl --noproxy localhost --location 'http://$NODE/api/upload?tableName=$table_name&limite=$max_rows' \
      --header 'Content-Type: application/octet-stream' \
      --data-binary @-
      
  echo -e "\nVérification des statistiques:"
  show_stats "$table_name"
}

# Fonction pour afficher les statistiques
show_stats() {
  if [ -z "$1" ]; then
    echo "Erreur: Nom de table requis"
    return 1
  fi
  
  local table_name="$1"
  echo "Statistiques de la table $table_name:"
  curl --noproxy localhost --location "http://$NODE/api/tables/$table_name/stats"
  
  # Vérifier aussi sur le deuxième nœud si configuré
  if [ -n "$NODE2" ]; then
    echo -e "\n\nStatistiques sur le second nœud ($NODE2):"
    curl --noproxy localhost --location "http://$NODE2/api/tables/$table_name/stats"
  fi
}

# Fonction pour exécuter une requête simple
query_table() {
  if [ -z "$1" ]; then
    echo "Erreur: Nom de table requis"
    return 1
  fi
  
  local table_name="$1"
  local limit="${2:-10}"
  
  echo "Exécution d'une requête simple sur $table_name (limite: $limit):"
  curl --noproxy localhost --location "http://$NODE/api/query" \
    --header 'Content-Type: application/json' \
    --data '{
      "tableName": "'$table_name'",
      "columns": ["*"],
      "limit": '$limit',
      "distributed": true
    }'
}

# Fonction pour supprimer une table
delete_table() {
  if [ -z "$1" ]; then
    echo "Erreur: Nom de table requis"
    return 1
  fi
  
  local table_name="$1"
  echo "Suppression de la table $table_name..."
  
  # Essaie les deux endpoints possibles
  curl --noproxy localhost -X DELETE "http://$NODE/api/table/$table_name"
  echo ""
  curl --noproxy localhost -X DELETE "http://$NODE/api/tables/$table_name"
}

# Fonction pour charger un fichier local TRÈS petit
upload_small_file() {
  if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Erreur: Nom de table et chemin du fichier requis"
    return 1
  fi
  
  local table_name="$1"
  local file_path="$2"
  local max_rows="${3:-100}"
  
  # Vérifie que le fichier existe
  if [ ! -f "$file_path" ]; then
    echo "Erreur: Fichier introuvable: $file_path"
    return 1
  fi
  
  # Vérifie la taille du fichier
  local file_size=$(du -h "$file_path" | cut -f1)
  echo "Chargement du fichier: $file_path ($file_size)"
  echo "Table: $table_name"
  echo "Limite: $max_rows lignes"
  
  # Utilise le répertoire temporaire system (hors quota)
  echo "Utilisation du répertoire temporaire: $TEMP_DIR"
  
  # Chargement avec l'API simple - format EXACTEMENT comme ton exemple fonctionnel
  echo "Chargement via API simplifiée..."
  curl --noproxy localhost --location "http://$NODE/api/upload?tableName=$table_name&limite=$max_rows" \
    --header 'Content-Type: application/octet-stream' \
    --data-binary "@$file_path"
}

# Fonction pour créer un set de test minimal
create_test_data() {
  local table_name="${1:-test_table}"
  local nb_rows="${2:-10}"
  
  echo "Création d'un set de données minimal pour tests..."
  echo "Table: $table_name"
  echo "Nombre de lignes: $nb_rows"
  
  # D'abord, créer la table
  create_table "$table_name"
  
  # Ensuite, insérer quelques lignes directement via API
  # Cette méthode n'utilise pas de fichier et évite les problèmes de quota
  echo "Insertion de $nb_rows lignes de test..."
  
  # Génération d'un petit JSON avec données de test
  local test_data="{\"data\": ["  
  for ((i=1; i<=$nb_rows; i++)); do
    if [ $i -gt 1 ]; then
      test_data="$test_data,"
    fi
    test_data="$test_data{\"Trip_Distance\": $i, \"Passenger_Count\": $((i % 4 + 1))}"
  done
  test_data="$test_data]}"
  
  # Insertion via API directe (pas de fichier impliqué)
  curl --noproxy localhost --location "http://$NODE/api/tables/$table_name/insert" \
    --header 'Content-Type: application/json' \
    --data "$test_data"
    
  # Afficher les stats
  echo -e "\nVérification des statistiques après insertion:"
  show_stats "$table_name"
}

# Traitement principal
case "$1" in
  create)
    create_table "$2"
    ;;
  upload)
    upload_from_url "$2" "$3" "$4"
    ;;
  stats)
    show_stats "$2"
    ;;
  query)
    query_table "$2" "$3"
    ;;
  delete)
    delete_table "$2"
    ;;
  upload-local)
    upload_small_file "$2" "$3" "$4"
    ;;
  test-data)
    create_test_data "$2" "$3"
    ;;
  *)
    show_help
    ;;
esac
