#!/bin/bash
echo "Starting MemoryDB Node 1 on port 8081..."

# Set JVM options for security manager via MAVEN_OPTS
export MAVEN_OPTS="-Djava.security.manager=allow -Dhadoop.home.dir=/tmp -Dio.netty.tryReflectionSetAccessible=true"

# Run with Quarkus dev mode
mvn quarkus:dev \
  -Dquarkus.http.port=8081 \
  -Dmemorydb.node.id=node1 \
  -Dmemorydb.node.hostname=localhost:8081 \
  -Dmemorydb.cluster.enabled=true \
  -Dmemorydb.cluster.nodes=localhost:8081,localhost:8082,localhost:8083
