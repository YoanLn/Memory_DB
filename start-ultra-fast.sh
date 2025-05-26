#!/bin/bash

# Ultra-Fast MemoryDB Startup Script
# This script applies extreme performance optimizations for maximum throughput

echo "🚀 Starting MemoryDB with EXTREME performance optimizations..."

# Set CPU affinity for better performance (adjust core numbers based on your system)
export JAVA_OPTS="
-server
-Xms8g
-Xmx16g
-XX:MaxDirectMemorySize=32g
-XX:+UseG1GC
-XX:MaxGCPauseMillis=10
-XX:G1HeapRegionSize=32m
-XX:+UseStringDeduplication
-XX:+OptimizeStringConcat
-XX:+UseFastAccessorMethods
-XX:+AggressiveOpts
-XX:+UseBiasedLocking
-XX:BiasedLockingStartupDelay=0
-XX:+EliminateLocks
-XX:+DoEscapeAnalysis
-XX:+UseCompressedOops
-XX:+UseCompressedClassPointers
-XX:+AlwaysPreTouch
-XX:+UseLargePages
-XX:LargePageSizeInBytes=2m
-XX:+UseTransparentHugePages
-XX:+UnlockExperimentalVMOptions
-XX:+UseJVMCICompiler
-XX:+EnableJVMCI
-XX:+UseZGC
-XX:+UnlockDiagnosticVMOptions
-XX:+LogVMOutput
-XX:+TraceClassLoading
-XX:+PrintGCDetails
-XX:+PrintGCTimeStamps
-XX:+PrintGCApplicationStoppedTime
-Djava.awt.headless=true
-Dfile.encoding=UTF-8
-Dio.netty.allocator.type=pooled
-Dio.netty.allocator.directMemoryCacheAlignment=64
-Dio.netty.leakDetection.level=disabled
-Dio.netty.recycler.maxCapacityPerThread=0
-Dio.netty.allocator.numDirectArenas=16
-Dio.netty.allocator.numHeapArenas=16
-Dio.netty.allocator.pageSize=8192
-Dio.netty.allocator.maxOrder=11
-Djdk.nio.maxCachedBufferSize=262144
-Djdk.attach.allowAttachSelf=true
"

# Set system-level optimizations
echo "⚡ Applying system-level optimizations..."

# Increase file descriptor limits
ulimit -n 1048576

# Set TCP buffer sizes for high-throughput networking
echo 'net.core.rmem_max = 134217728' | sudo tee -a /etc/sysctl.conf
echo 'net.core.wmem_max = 134217728' | sudo tee -a /etc/sysctl.conf
echo 'net.ipv4.tcp_rmem = 4096 87380 134217728' | sudo tee -a /etc/sysctl.conf
echo 'net.ipv4.tcp_wmem = 4096 65536 134217728' | sudo tee -a /etc/sysctl.conf
echo 'net.core.netdev_max_backlog = 30000' | sudo tee -a /etc/sysctl.conf
echo 'net.ipv4.tcp_congestion_control = bbr' | sudo tee -a /etc/sysctl.conf

# Apply sysctl changes
sudo sysctl -p

# Set CPU governor to performance mode
echo "🔥 Setting CPU to performance mode..."
echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor

# Disable swap for better performance
echo "💾 Disabling swap for maximum performance..."
sudo swapoff -a

# Set I/O scheduler to deadline for better SSD performance
echo "💿 Optimizing I/O scheduler..."
echo deadline | sudo tee /sys/block/*/queue/scheduler

# Increase shared memory limits for Chronicle Map
echo "🗺️ Configuring shared memory for Chronicle Map..."
echo 'kernel.shmmax = 68719476736' | sudo tee -a /etc/sysctl.conf
echo 'kernel.shmall = 4294967296' | sudo tee -a /etc/sysctl.conf

# Set process priority
echo "⚡ Setting high process priority..."
sudo nice -n -20 bash -c "

# Build the application with native optimizations
echo '🔨 Building with native optimizations...'
./mvnw clean package -Pnative -Dquarkus.native.additional-build-args='--initialize-at-build-time=org.slf4j.LoggerFactory,org.slf4j.impl.StaticLoggerBinder'

# Start the application
echo '🚀 Starting MemoryDB with EXTREME performance settings...'
java \$JAVA_OPTS -jar target/quarkus-app/quarkus-run.jar

"

echo "✅ MemoryDB started with EXTREME performance optimizations!"
echo "📊 Monitor performance with: jstat -gc -t [PID] 1s"
echo "🔍 Monitor off-heap usage with: jcmd [PID] VM.native_memory summary" 