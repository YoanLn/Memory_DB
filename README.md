# MemoryDB - Base de Données Distribuée In-Memory

Une base de données distribuée in-memory en Java avec support pour le format Parquet et des requêtes de type SQL.

## Fonctionnalités

- Stockage en mémoire optimisé pour la performance
- Ingestion rapide de fichiers Parquet
- Requêtes SQL-like (SELECT ... WHERE ... GROUP BY)
- Distribution sur plusieurs nœuds
- API REST pour l'interaction

## Architecture

Le projet est organisé en modules distincts :

- **Core** : Le moteur principal de la base de données
- **Storage** : Structures de données pour le stockage en mémoire
- **Query** : Traitement et exécution des requêtes
- **Parquet** : Chargement et lecture des fichiers Parquet
- **REST** : API REST pour l'interaction client
- **Distribution** : Communication entre les nœuds
- **Common** : Utilitaires partagés

## Prérequis

- Java 11+
- Maven 3.6+

## Installation et démarrage

```bash
# Cloner le dépôt
git clone https://github.com/votre-username/MemoryDB.git
cd MemoryDB

# Compiler
mvn clean package

# Démarrer le serveur
java -jar target/memory-db-1.0-SNAPSHOT-runner.jar
```

## Utilisation de l'API REST

### Création d'une table
```
POST /api/tables
Content-Type: application/json

{
  "name": "users",
  "columns": [
    {"name": "id", "type": "INTEGER"},
    {"name": "name", "type": "STRING"},
    {"name": "age", "type": "INTEGER"},
    {"name": "active", "type": "BOOLEAN"}
  ]
}
```

### Chargement d'un fichier Parquet
```
POST /api/tables/{tableName}/load-parquet
Content-Type: application/json

{
  "filePath": "/chemin/vers/fichier.parquet"
}
```

### Chargement d'un fichier Parquet avec limite de lignes
```
POST /api/tables/{tableName}/load-parquet
Content-Type: application/json

{
  "filePath": "/chemin/vers/fichier.parquet",
  "rowLimit": 1000
}
```

### Création d'une table à partir d'un fichier Parquet
```
POST /api/tables/from-parquet
Content-Type: application/json

{
  "tableName": "nouvelle_table",
  "filePath": "/chemin/vers/fichier.parquet"
}
```

### Création d'une table à partir d'un fichier Parquet avec limite de lignes
```
POST /api/tables/from-parquet
Content-Type: application/json

{
  "tableName": "nouvelle_table",
  "filePath": "/chemin/vers/fichier.parquet",
  "rowLimit": 500
}
```

### Création d'une table à partir d'un fichier Parquet sans charger les données
```
POST /api/tables/from-parquet
Content-Type: application/json

{
  "tableName": "nouvelle_table",
  "filePath": "/chemin/vers/fichier.parquet",
  "loadData": false
}
```

### Création uniquement du schéma d'une table à partir d'un fichier Parquet
```
POST /api/tables/schema-from-parquet
Content-Type: application/json

{
  "tableName": "nouvelle_table",
  "filePath": "/chemin/vers/fichier.parquet"
}
```

### Exécution d'une requête
```