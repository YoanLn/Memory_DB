#!/bin/bash
echo "Starting MemoryDB Node 1 on physical machine 1..."

# IP addresses of all machines in your network
IP_NODE1="10.25.0.4"  # Replace with actual IP of machine 1
IP_NODE2="10.25.0.5"  # Replace with actual IP of machine 2
PORT=8080  # Use the same port on all machines

# Create necessary directories with proper permissions
echo "Ensuring necessary directories exist with proper permissions..."
mkdir -p ./data ./tmp ./logs
chmod 755 ./data ./tmp ./logs

# Build the application if needed
if [ ! -f "target/quarkus-app/quarkus-run.jar" ]; then
  echo "Building application..."
  mvn package -DskipTests
fi

# Set specific Hadoop and Java temp directories within the project
HADOOP_TMP="./tmp/hadoop"
JAVA_TMP="./tmp/java"
mkdir -p "$HADOOP_TMP" "$JAVA_TMP"
chmod -R 755 "$HADOOP_TMP" "$JAVA_TMP"

# Run the application with Java directly, setting all required parameters
java -Dquarkus.http.port=$PORT \
  -Dmemorydb.node.id=node2 \
  -Dmemorydb.node.hostname=$IP_NODE1:$PORT \
  -Dmemorydb.cluster.enabled=true \
  -Dmemorydb.cluster.nodes=$IP_NODE1:$PORT,$IP_NODE2:$PORT \
  -Djava.security.manager=allow \
  -Dhadoop.home.dir="$HADOOP_TMP" \
  -Djava.io.tmpdir="$JAVA_TMP" \
  -Dio.netty.tryReflectionSetAccessible=true \
  -Djava.awt.headless=true \
  -jar target/quarkus-app/quarkus-run.jar
