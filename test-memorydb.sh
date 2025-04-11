#!/bin/bash
# Script de test pour MemoryDB
# Utilise cURL pour tester les principales fonctionnalités de l'API

BASE_URL="http://localhost:8093"
CONTENT_TYPE="application/json"
PARQUET_FILE="/Users/yoanln/MemoryDB/data/test.parquet"

# Fonction d'aide pour les requêtes
# Paramètres: $1 = méthode, $2 = endpoint, $3 = data (optionnel)
function request() {
    method=$1
    endpoint=$2
    data=$3
    
    echo -e "\n\033[1;34m=== Requête: $method $endpoint ===\033[0m"
    
    if [ -z "$data" ]; then
        curl -s -X "$method" "$BASE_URL$endpoint" \
             -H "Content-Type: $CONTENT_TYPE" \
             -H "Accept: $CONTENT_TYPE" | jq .
    else
        curl -s -X "$method" "$BASE_URL$endpoint" \
             -H "Content-Type: $CONTENT_TYPE" \
             -H "Accept: $CONTENT_TYPE" \
             -d "$data" | jq .
    fi
    
    echo -e "\033[1;32m=== Fin de la requête ===\033[0m\n"
    sleep 1
}

echo -e "\033[1;33m=== TESTS MEMORYDB ===\033[0m"
echo "Serveur: $BASE_URL"

# Test de la santé du cluster
request "GET" "/api/cluster/health"

# Informations du nœud local
request "GET" "/api/cluster/info"

# Liste des nœuds du cluster
request "GET" "/api/cluster/nodes"

# Création d'une table users
request "POST" "/api/tables" '{
  "name": "users",
  "columns": [
    {"name": "id", "type": "INTEGER", "nullable": false},
    {"name": "name", "type": "STRING", "nullable": false},
    {"name": "age", "type": "INTEGER", "nullable": true},
    {"name": "active", "type": "BOOLEAN", "nullable": false}
  ]
}'

# Pause pour laisser le temps à la table d'être créée
sleep 2

# Chargement d'un fichier Parquet dans la table users avec limite de lignes
request "POST" "/api/tables/users/load-parquet" "{
  \"filePath\": \"$PARQUET_FILE\",
  \"rowLimit\": 100
}"

# Création d'une table standard à partir d'un fichier Parquet
request "POST" "/api/tables/from-parquet" "{
  \"tableName\": \"parquet_data\",
  \"filePath\": \"$PARQUET_FILE\"
}"

# Création d'une table à partir d'un fichier Parquet avec limite de lignes
request "POST" "/api/tables/from-parquet" "{
  \"tableName\": \"parquet_limited\",
  \"filePath\": \"$PARQUET_FILE\",
  \"rowLimit\": 50
}"

# Création d'une table à partir d'un fichier Parquet sans charger les données
request "POST" "/api/tables/from-parquet" "{
  \"tableName\": \"parquet_schema_only_1\",
  \"filePath\": \"$PARQUET_FILE\",
  \"loadData\": false
}"

# Création uniquement du schéma d'une table à partir d'un fichier Parquet
request "POST" "/api/tables/schema-from-parquet" "{
  \"tableName\": \"parquet_schema_only_2\",
  \"filePath\": \"$PARQUET_FILE\"
}"

# Requête simple sur la table à lignes limitées
request "POST" "/api/query" '{
  "tableName": "parquet_limited",
  "columns": ["*"]
}'

# Requête simple sur la table users
request "POST" "/api/query" '{
  "tableName": "users",
  "columns": ["id", "name", "age", "active"]
}'

# Requête avec condition
request "POST" "/api/query" '{
  "tableName": "users",
  "columns": ["id", "name", "age"],
  "conditions": [
    {
      "columnName": "age",
      "operator": "GREATER_THAN",
      "value": 25
    }
  ]
}'

# Requête avec plusieurs conditions
request "POST" "/api/query" '{
  "tableName": "users",
  "columns": ["id", "name", "age", "active"],
  "conditions": [
    {
      "columnName": "age",
      "operator": "GREATER_THAN",
      "value": 18
    },
    {
      "columnName": "active",
      "operator": "EQUALS",
      "value": true
    }
  ]
}'

# Requête sur les données parquet
request "POST" "/api/query" '{
  "tableName": "parquet_data",
  "columns": ["*"]
}'

# Requête avec ordre de tri
request "POST" "/api/query" '{
  "tableName": "users",
  "columns": ["id", "name", "age"],
  "orderBy": "age",
  "orderByAscending": false,
  "limit": 10
}'

# Requête simplifiée via GET
request "GET" "/api/query/users?column=age&value=30&limit=5"

# Export CSV (en-têtes seulement - pour éviter trop de données)
echo -e "\n\033[1;34m=== Requête: GET /api/export/parquet_data/csv ===\033[0m"
curl -s -X "GET" "$BASE_URL/api/export/parquet_data/csv?limit=5" \
     -H "Accept: text/csv" | head -n 3
echo -e "\n\033[1;32m=== Fin de la requête ===\033[0m\n"

# Export JSON (limité à 5 lignes)
echo -e "\n\033[1;34m=== Requête: GET /api/export/parquet_data/json ===\033[0m"
curl -s -X "GET" "$BASE_URL/api/export/parquet_data/json?limit=5" \
     -H "Accept: application/json" | jq '.[0:2]'
echo -e "\n\033[1;32m=== Fin de la requête ===\033[0m\n"

echo -e "\033[1;33m=== TESTS TERMINÉS ===\033[0m" 