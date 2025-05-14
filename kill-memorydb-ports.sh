#!/bin/bash

# Script pour trouver et tuer les processus utilisant les ports du cluster MemoryDB
# Ce script recherche les processus sur les ports 8081, 8082 et 8083 et les tue

echo "Recherche des processus utilisant les ports du cluster MemoryDB..."

# Fonction pour tuer le processus sur un port spécifique
kill_process_on_port() {
    local port=$1
    echo "Vérification du port $port..."
    
    # Trouver le PID utilisant le port
    local pid=$(lsof -ti:$port)
    
    if [ -n "$pid" ]; then
        echo "Processus trouvé sur le port $port: PID $pid - Tentative de kill..."
        kill -9 $pid
        echo "Processus sur le port $port tué avec succès."
    else
        echo "Aucun processus n'utilise le port $port."
    fi
}

# Tuer les processus pour chaque port
kill_process_on_port 8081
kill_process_on_port 8082
kill_process_on_port 8083

echo "Terminé. Tous les ports du cluster MemoryDB devraient être libérés."
