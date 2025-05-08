#!/bin/bash

# Create data directory if it doesn't exist
mkdir -p data

# Set the classpath with all dependencies
CLASSPATH="target/memory-db-1.0-SNAPSHOT.jar:target/lib/*"

# Port used by the application
PORT=8092

# Function to stop the application
stop_app() {
  echo "Stopping MemoryDB application..."
  PID=$(lsof -t -i:$PORT 2>/dev/null)
  if [ -n "$PID" ]; then
    kill -9 $PID
    echo "Application stopped (PID: $PID)"
  else
    echo "No application running on port $PORT"
  fi
  exit 0
}

# Check for stop command
if [ "$1" = "stop" ]; then
  stop_app
fi

# Run the application
echo "Starting MemoryDB application on port $PORT..."
java -Djava.security.manager=allow -Dquarkus.http.host=0.0.0.0 -Dquarkus.http.port=$PORT -cp "$CLASSPATH" com.memorydb.MemoryDbApplication