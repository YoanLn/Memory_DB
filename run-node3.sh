#!/bin/bash
echo "Starting MemoryDB Node 3 on port 8083..."

# Set JVM options for security manager via MAVEN_OPTS
export MAVEN_OPTS="-Djava.security.manager=allow -Dhadoop.home.dir=/tmp -Dio.netty.tryReflectionSetAccessible=true"

# Run with Quarkus dev mode
mvn quarkus:dev \
  -Dquarkus.http.port=8083 \
  -Dmemorydb.node.id=node3 \
  -Dmemorydb.node.hostname=localhost:8083 \
  -Dmemorydb.cluster.enabled=true \
  -Dmemorydb.cluster.nodes=localhost:8081,localhost:8082,localhost:8083
