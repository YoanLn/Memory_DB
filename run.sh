#!/bin/bash

# Create data directory if it doesn't exist
mkdir -p data

# Set the classpath with all dependencies
CLASSPATH="target/memory-db-1.0-SNAPSHOT.jar:target/lib/*"

# Run the application
java -Djava.security.manager=allow -Dquarkus.http.host=0.0.0.0 -Dquarkus.http.port=8092 -cp "$CLASSPATH" com.memorydb.MemoryDbApplication 