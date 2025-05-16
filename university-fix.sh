#!/bin/bash
# ===========================================================
# SCRIPT DE CORRECTION POUR ENVIRONNEMENTS UNIVERSITAIRES
# Ce script permet de résoudre les problèmes courants de MemoryDB
# dans les environnements universitaires avec ressources limitées
# ===========================================================

# Variables par défaut
NODE1="localhost:8081"
NODE2="localhost:8082"
NODE3="localhost:8083"
PARQUET_FILE="data/test2.parquet"
DEFAULT_UPLOAD_DIR="$HOME/memorydb_uploads"
UNIVERSITY_ENV=true
MEMORY_SETTINGS="-Xmx512m"
HADOOP_HOME="/tmp/hadoop_home"

# Couleurs pour les messages
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Crée le répertoire d'upload personnalisé
create_upload_dir() {
    echo -e "${BLUE}Configuration du répertoire d'uploads...${NC}"
    
    # Crée le répertoire d'upload dans le répertoire utilisateur
    mkdir -p "$DEFAULT_UPLOAD_DIR"
    
    if [ -d "$DEFAULT_UPLOAD_DIR" ] && [ -w "$DEFAULT_UPLOAD_DIR" ]; then
        echo -e "${GREEN}✓ Répertoire d'upload créé: $DEFAULT_UPLOAD_DIR${NC}"
        # Assure que les permissions sont correctes
        chmod 755 "$DEFAULT_UPLOAD_DIR"
    else
        echo -e "${RED}✗ Impossible de créer le répertoire d'upload. Tentative alternative...${NC}"
        
        # Essaie un répertoire alternatif
        ALT_DIR="./uploads"
        mkdir -p "$ALT_DIR"
        
        if [ -d "$ALT_DIR" ] && [ -w "$ALT_DIR" ]; then
            DEFAULT_UPLOAD_DIR="$ALT_DIR"
            echo -e "${GREEN}✓ Répertoire d'upload alternatif créé: $DEFAULT_UPLOAD_DIR${NC}"
        else
            echo -e "${RED}✗ Impossible de créer un répertoire d'upload. Le système utilisera /tmp${NC}"
            echo -e "${YELLOW}! ATTENTION: Cela peut causer des problèmes de corruption de fichiers${NC}"
        fi
    fi
}

# Configure l'environnement Java pour les ressources limitées
configure_java_env() {
    echo -e "${BLUE}Configuration de l'environnement Java...${NC}"
    
    # Configure Hadoop home pour éviter les problèmes avec les implémentations de système de fichiers
    mkdir -p "$HADOOP_HOME/bin"
    
    if [ -d "$HADOOP_HOME" ]; then
        export HADOOP_HOME="$HADOOP_HOME"
        echo -e "${GREEN}✓ Variable HADOOP_HOME définie: $HADOOP_HOME${NC}"
        
        # Crée un script fictif pour éviter les avertissements d'utilisation de winutils.exe
        cat > "$HADOOP_HOME/bin/winutils.exe" << EOF
#!/bin/bash
echo "Mock winutils for Hadoop"
exit 0
EOF
        chmod +x "$HADOOP_HOME/bin/winutils.exe"
    else
        echo -e "${RED}✗ Impossible de créer le répertoire HADOOP_HOME${NC}"
    fi
    
    # Configure des propriétés système Java pour MemoryDB
    export JAVA_OPTS="$JAVA_OPTS $MEMORY_SETTINGS -DUNIVERSITY_ENV=true -Dhadoop.home.dir=$HADOOP_HOME -Djava.io.tmpdir=$DEFAULT_UPLOAD_DIR"
    echo -e "${GREEN}✓ Options Java configurées: $JAVA_OPTS${NC}"
}

# Vérifie si le fichier Parquet est valide
check_parquet_file() {
    local file="$1"
    
    if [ ! -f "$file" ]; then
        echo -e "${RED}✗ Le fichier n'existe pas: $file${NC}"
        return 1
    fi
    
    # Vérifie la taille du fichier (doit être >100K)
    local size=$(stat -c%s "$file" 2>/dev/null || stat -f%z "$file" 2>/dev/null)
    if [ "$size" -lt 102400 ]; then
        echo -e "${YELLOW}! ATTENTION: Le fichier est petit ($size octets), peut être incomplet${NC}"
    else
        echo -e "${GREEN}✓ Taille du fichier correcte: $size octets${NC}"
    fi
    
    # Vérifie la présence de l'entête Parquet (PAR1)
    if hexdump -n 4 "$file" 2>/dev/null | grep -q "PAR1"; then
        echo -e "${GREEN}✓ En-tête Parquet (PAR1) détecté${NC}"
    else
        echo -e "${RED}✗ En-tête Parquet non trouvé, le fichier peut être corrompu${NC}"
        return 1
    fi
    
    return 0
}

# Upload sécurisé d'un fichier Parquet
safe_parquet_upload() {
    local file="$1"
    local node="$2"
    local table="$3"
    local rows="$4"
    local batch="$5"
    
    # Vérifie le fichier
    echo -e "${BLUE}Vérification du fichier Parquet avant l'envoi...${NC}"
    check_parquet_file "$file"
    
    # Prépare le fichier
    local temp_file="${DEFAULT_UPLOAD_DIR}/$(basename "$file")"
    
    # Copie le fichier dans le répertoire d'upload
    echo -e "${BLUE}Copie du fichier vers $temp_file...${NC}"
    cp "$file" "$temp_file"
    
    # Vérifie que la copie est correcte
    if check_parquet_file "$temp_file"; then
        echo -e "${GREEN}✓ Fichier correctement préparé pour l'upload${NC}"
    else
        echo -e "${RED}✗ Problème lors de la copie du fichier${NC}"
        return 1
    fi
    
    # Envoie le fichier avec curl en utilisant --data-binary pour plus de fiabilité
    echo -e "${BLUE}Envoi du fichier au serveur ($node)...${NC}"
    curl --noproxy "*" -X POST \
        -F "file=@$temp_file" \
        -F "rowLimit=${rows:--1}" \
        -F "batchSize=${batch:-10000}" \
        -F "skipRows=0" \
        "http://$node/api/tables/${table:-parquet_file}/load-distributed-upload"
    
    local result=$?
    if [ $result -eq 0 ]; then
        echo -e "${GREEN}✓ Fichier envoyé avec succès${NC}"
    else
        echo -e "${RED}✗ Erreur lors de l'envoi du fichier (code $result)${NC}"
    fi
    
    return $result
}

# Affiche l'aide du script
show_help() {
    echo -e "${BLUE}=================================================${NC}"
    echo -e "${BLUE}SCRIPT DE CORRECTION POUR ENVIRONNEMENTS UNIVERSITAIRES${NC}"
    echo -e "${BLUE}=================================================${NC}"
    echo
    echo -e "Usage: $0 [option]"
    echo
    echo -e "${YELLOW}Options:${NC}"
    echo -e "  ${GREEN}setup${NC}      - Configure l'environnement pour les machines universitaires"
    echo -e "  ${GREEN}upload${NC}     - Upload sécurisé d'un fichier Parquet (évite corruption)"
    echo -e "  ${GREEN}check${NC}      - Vérifie si un fichier Parquet est valide"
    echo -e "  ${GREEN}help${NC}       - Affiche cette aide"
    echo
    echo -e "${YELLOW}Exemples:${NC}"
    echo -e "  $0 setup                               - Configure l'environnement"
    echo -e "  $0 upload data.parquet localhost:8081  - Upload sécurisé vers le nœud 1"
    echo -e "  $0 check data.parquet                 - Vérifie si le fichier est valide"
    echo
}

# Fonction principale
main() {
    case "$1" in
        setup)
            create_upload_dir
            configure_java_env
            echo -e "\n${GREEN}✓ Configuration terminée. Utilisez les commandes suivantes pour démarrer MemoryDB:${NC}"
            echo -e "${YELLOW}export JAVA_OPTS=\"$JAVA_OPTS\"${NC}"
            echo -e "${YELLOW}export HADOOP_HOME=\"$HADOOP_HOME\"${NC}"
            ;;
        upload)
            if [ -z "$2" ]; then
                echo -e "${RED}✗ Fichier Parquet non spécifié${NC}"
                show_help
                return 1
            fi
            
            create_upload_dir
            local file="$2"
            local node="${3:-$NODE1}"
            local table="${4:-parquet_file}"
            local rows="${5:--1}"
            local batch="${6:-10000}"
            
            safe_parquet_upload "$file" "$node" "$table" "$rows" "$batch"
            ;;
        check)
            if [ -z "$2" ]; then
                echo -e "${RED}✗ Fichier Parquet non spécifié${NC}"
                show_help
                return 1
            fi
            
            check_parquet_file "$2"
            ;;
        help|*)
            show_help
            ;;
    esac
}

# Exécuter la fonction principale avec les arguments
main "$@"
