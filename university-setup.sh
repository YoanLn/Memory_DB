#!/bin/bash
# ===============================================
# Script pour l'environnement universitaire MemoryDB
# Ce script permet d'utiliser MemoryDB dans un environnement universitaire
# avec des contraintes de proxy et des configurations spécifiques
# ===============================================

# Configuration par défaut
NODE1="localhost:8081"
NODE2="localhost:8082"
NODE3="localhost:8083"
PARQUET_FILE="data/test2.parquet"
PROXY=""
PROXY_SETTINGS=""

# Fonction pour configurer l'environnement
setup_env() {
  echo "========================================================="
  echo "Configuration de l'environnement universitaire MemoryDB"
  echo "========================================================="
  
  # Configuration des nœuds (remplacer par les adresses réelles)
  read -p "Adresse du nœud 1 [default: $NODE1]: " input_node1
  NODE1=${input_node1:-$NODE1}
  
  read -p "Adresse du nœud 2 [default: $NODE2]: " input_node2
  NODE2=${input_node2:-$NODE2}
  
  read -p "Adresse du nœud 3 [default: $NODE3]: " input_node3
  NODE3=${input_node3:-$NODE3}
  
  # Configuration du fichier Parquet
  read -p "Chemin du fichier Parquet [default: $PARQUET_FILE]: " input_file
  PARQUET_FILE=${input_file:-$PARQUET_FILE}
  
  # Configuration du proxy
  read -p "Proxy (laisser vide pour ignorer tous les proxys): " input_proxy
  PROXY=$input_proxy
  
  if [ -n "$PROXY" ]; then
    PROXY_SETTINGS="-x $PROXY"
    echo "Proxy configuré: $PROXY"
  else
    # Ignorer tous les proxys
    PROXY_SETTINGS="--noproxy '*'"
    echo "Mode sans proxy activé"
  fi
  
  echo -e "\nConfiguration enregistrée:"
  echo "- Nœud 1: $NODE1"
  echo "- Nœud 2: $NODE2"
  echo "- Nœud 3: $NODE3"
  echo "- Fichier Parquet: $PARQUET_FILE"
  echo "- Proxy: $PROXY"
  
  save_config
}

# Fonction pour sauvegarder la configuration
save_config() {
  cat > .university_config <<EOF
NODE1="$NODE1"
NODE2="$NODE2"
NODE3="$NODE3"
PARQUET_FILE="$PARQUET_FILE"
PROXY="$PROXY"
PROXY_SETTINGS="$PROXY_SETTINGS"
EOF
  echo -e "\nConfiguration sauvegardée dans .university_config"
}

# Fonction pour charger la configuration
load_config() {
  if [ -f .university_config ]; then
    source .university_config
    echo "Configuration chargée depuis .university_config"
  else
    echo "Aucune configuration trouvée. Utilisation des valeurs par défaut."
  fi
}

# Fonction pour tester la connectivité
test_connectivity() {
  echo "========================================================="
  echo "Test de connectivité aux nœuds MemoryDB"
  echo "========================================================="
  echo "- Nœud 1: $NODE1"
  echo "- Nœud 2: $NODE2"
  echo "- Nœud 3: $NODE3"
  echo "- Proxy: $PROXY"
  
  # Vérifier la connectivité à chaque nœud
  echo -e "\nVérification de la connectivité..."
  
  echo "Test de connectivité au nœud 1 ($NODE1)..."
  RESULT1=$(curl $PROXY_SETTINGS -s -o /dev/null -w "%{http_code}" "http://$NODE1/api/health" 2>/dev/null || echo "FAILED")
  
  echo "Test de connectivité au nœud 2 ($NODE2)..."
  RESULT2=$(curl $PROXY_SETTINGS -s -o /dev/null -w "%{http_code}" "http://$NODE2/api/health" 2>/dev/null || echo "FAILED")
  
  echo "Test de connectivité au nœud 3 ($NODE3)..."
  RESULT3=$(curl $PROXY_SETTINGS -s -o /dev/null -w "%{http_code}" "http://$NODE3/api/health" 2>/dev/null || echo "FAILED")
  
  # Vérifier le fichier Parquet
  echo -e "\nVérification du fichier Parquet: $PARQUET_FILE"
  if [ -f "$PARQUET_FILE" ]; then
    FILE_SIZE=$(du -h "$PARQUET_FILE" 2>/dev/null | cut -f1 || echo "inconnu")
    echo "Le fichier existe et occupe $FILE_SIZE d'espace disque"
  else
    echo "ERREUR: Le fichier n'existe pas ou n'est pas accessible"
  fi
  
  # Résumé des résultats
  echo -e "\nRésumé des tests:"
  echo "- Nœud 1: $([ "$RESULT1" = "200" ] && echo "OK (code $RESULT1)" || echo "ECHEC (code $RESULT1)")"
  echo "- Nœud 2: $([ "$RESULT2" = "200" ] && echo "OK (code $RESULT2)" || echo "ECHEC (code $RESULT2)")"
  echo "- Nœud 3: $([ "$RESULT3" = "200" ] && echo "OK (code $RESULT3)" || echo "ECHEC (code $RESULT3)")"
  echo "- Fichier Parquet: $([ -f "$PARQUET_FILE" ] && echo "OK" || echo "ECHEC")"
}

# Charger des données en tenant compte du proxy
load_data() {
  echo "========================================================="
  echo "Chargement des données depuis le fichier Parquet"
  echo "========================================================="
  echo "- Fichier source: $PARQUET_FILE"
  echo "- Nœud cible: $NODE1"
  echo "- Proxy: $PROXY"
  
  if [ ! -f "$PARQUET_FILE" ]; then
    echo "ERREUR: Le fichier $PARQUET_FILE n'existe pas!"
    return 1
  fi
  
  read -p "Nombre de lignes à charger (-1 pour tout): " row_limit
  row_limit=${row_limit:--1}
  
  read -p "Taille du lot (batch size) [default: 100000]: " batch_size
  batch_size=${batch_size:-100000}
  
  echo -e "\nChargement en cours..."
  curl $PROXY_SETTINGS -X POST \
    -F "file=@$PARQUET_FILE" \
    -F "rowLimit=$row_limit" \
    -F "batchSize=$batch_size" \
    -F "skipRows=0" \
    "http://$NODE1/api/tables/parquet_file/load-distributed-upload"
    
  echo -e "\nChargement terminé."
}

# Obtenir les statistiques consolidées
get_stats() {
  echo "========================================================="
  echo "Statistiques consolidées de la table"
  echo "========================================================="
  
  # Récupérer les statistiques de chaque nœud
  echo "Récupération des statistiques depuis tous les nœuds..."
  
  NODE1_STATS=$(curl $PROXY_SETTINGS -s -X GET "http://$NODE1/api/tables/parquet_file/stats")
  NODE2_STATS=$(curl $PROXY_SETTINGS -s -X GET "http://$NODE2/api/tables/parquet_file/stats")
  NODE3_STATS=$(curl $PROXY_SETTINGS -s -X GET "http://$NODE3/api/tables/parquet_file/stats")
  
  # Extraire les nombres de lignes
  NODE1_ROWS=$(echo $NODE1_STATS | grep -o '"rowCount":[0-9]*' | cut -d':' -f2)
  NODE2_ROWS=$(echo $NODE2_STATS | grep -o '"rowCount":[0-9]*' | cut -d':' -f2)
  NODE3_ROWS=$(echo $NODE3_STATS | grep -o '"rowCount":[0-9]*' | cut -d':' -f2)
  
  # Si extraction échouée, mettre à 0
  NODE1_ROWS=${NODE1_ROWS:-0}
  NODE2_ROWS=${NODE2_ROWS:-0}
  NODE3_ROWS=${NODE3_ROWS:-0}
  
  # Calculer le total
  TOTAL_ROWS=$((NODE1_ROWS + NODE2_ROWS + NODE3_ROWS))
  
  # Afficher le résumé
  echo -e "\n----- Résumé -----"
  echo "Nœud 1: $NODE1_ROWS lignes"
  echo "Nœud 2: $NODE2_ROWS lignes"
  echo "Nœud 3: $NODE3_ROWS lignes"
  echo "Total: $TOTAL_ROWS lignes"
  
  # Calculer la distribution en pourcentage
  if [ $TOTAL_ROWS -gt 0 ]; then
    NODE1_PCT=$(echo "scale=2; $NODE1_ROWS * 100 / $TOTAL_ROWS" | bc)
    NODE2_PCT=$(echo "scale=2; $NODE2_ROWS * 100 / $TOTAL_ROWS" | bc)
    NODE3_PCT=$(echo "scale=2; $NODE3_ROWS * 100 / $TOTAL_ROWS" | bc)
    
    echo -e "\nDistribution:"
    echo "Nœud 1: $NODE1_PCT%"
    echo "Nœud 2: $NODE2_PCT%"
    echo "Nœud 3: $NODE3_PCT%"
  fi
}

# Fonction d'aide
show_help() {
  echo "Usage: $0 [option]"
  echo ""
  echo "Options:"
  echo "  setup     - Configure l'environnement universitaire"
  echo "  test      - Teste la connectivité aux nœuds"
  echo "  load      - Charge des données depuis le fichier Parquet"
  echo "  stats     - Affiche les statistiques consolidées"
  echo "  help      - Affiche cette aide"
  echo ""
}

# Fonction principale
main() {
  # Charger la configuration existante si disponible
  load_config
  
  # Traiter les arguments
  case "$1" in
    setup)
      setup_env
      ;;
    test)
      test_connectivity
      ;;
    load)
      load_data
      ;;
    stats)
      get_stats
      ;;
    help|*)
      show_help
      ;;
  esac
}

# Exécuter la fonction principale avec les arguments
main "$@"
