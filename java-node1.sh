#!/bin/bash
echo "Starting MemoryDB Node 1 on port 8081 using direct Java..."

# Build the application if needed
if [ ! -f "target/quarkus-app/quarkus-run.jar" ]; then
  echo "Building application..."
  mvn package -DskipTests
fi

# Run the application with Java directly, setting all required parameters
java -Dquarkus.http.port=8081 \
  -Dmemorydb.node.id=node1 \
  -Dmemorydb.node.hostname=localhost:8081 \
  -Dmemorydb.cluster.enabled=true \
  -Dmemorydb.cluster.nodes=localhost:8081,localhost:8082,localhost:8083 \
  -Djava.security.manager=allow \
  -Dhadoop.home.dir=/tmp \
  -Dio.netty.tryReflectionSetAccessible=true \
  -jar target/quarkus-app/quarkus-run.jar
