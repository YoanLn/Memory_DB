#!/bin/bash

echo "🚀 Starting MemoryDB Web Demo..."
echo "=================================="

# Check if Python 3 is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is required but not installed."
    echo "💡 Please install Python 3 and try again."
    exit 1
fi

# Check if MemoryDB is running
echo "🔍 Checking if MemoryDB is running..."
if ! curl -s http://localhost:8081/api/tables > /dev/null 2>&1; then
    echo "⚠️  MemoryDB doesn't seem to be running on localhost:8081"
    echo "💡 Start MemoryDB first with: ./start-ultra-fast.sh"
    echo "   Or use the simple startup: ./mvnw quarkus:dev"
    echo ""
    echo "🚀 Starting demo anyway (you can start MemoryDB later)..."
else
    echo "✅ MemoryDB is running!"
fi

echo ""
echo "🌐 Starting web demo server with CORS proxy..."
echo "📁 Demo URL: http://localhost:8080"
echo "🔗 MemoryDB Nodes: localhost:8081, localhost:8082, localhost:8083"
echo ""
echo "Press Ctrl+C to stop the demo server"
echo "=================================="

# Start the Python server
cd "$(dirname "$0")"
python3 server.py 