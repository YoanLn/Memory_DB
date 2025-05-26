#!/usr/bin/env python3
"""
MemoryDB Demo Web Server with CORS Proxy
This server serves the web demo and proxies API requests to avoid CORS issues.
"""

import http.server
import socketserver
import urllib.request
import urllib.parse
import json
import os
import sys
from urllib.error import URLError, HTTPError

class CORSProxyHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        # MemoryDB nodes configuration
        self.memorydb_nodes = [
            'localhost:8081',
            'localhost:8082', 
            'localhost:8083'
        ]
        super().__init__(*args, **kwargs)

    def end_headers(self):
        # Add CORS headers to all responses
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        self.send_header('Access-Control-Max-Age', '86400')
        super().end_headers()

    def do_OPTIONS(self):
        # Handle preflight requests
        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        if self.path.startswith('/api/'):
            self.proxy_to_memorydb()
        else:
            super().do_GET()

    def do_POST(self):
        if self.path.startswith('/api/'):
            self.proxy_to_memorydb()
        else:
            self.send_error(404)

    def do_DELETE(self):
        if self.path.startswith('/api/'):
            self.proxy_to_memorydb()
        else:
            self.send_error(404)

    def proxy_to_memorydb(self):
        """Proxy API requests to MemoryDB backend"""
        try:
            # Extract node from query parameters or use primary node
            query_params = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            node = query_params.get('node', [self.memorydb_nodes[0]])[0]
            
            # Remove node parameter from path
            parsed_path = urllib.parse.urlparse(self.path)
            clean_query = urllib.parse.urlencode({k: v[0] for k, v in query_params.items() if k != 'node'})
            clean_path = parsed_path.path
            if clean_query:
                clean_path += '?' + clean_query

            # Build target URL
            target_url = f'http://{node}{clean_path}'
            
            print(f"🌐 Proxying {self.command} {self.path} -> {target_url}")

            # Prepare request
            req_data = None
            if self.command in ['POST', 'PUT']:
                content_length = int(self.headers.get('Content-Length', 0))
                if content_length > 0:
                    req_data = self.rfile.read(content_length)

            # Create request
            req = urllib.request.Request(target_url, data=req_data, method=self.command)
            
            # Copy headers (except host)
            for header, value in self.headers.items():
                if header.lower() not in ['host', 'connection']:
                    req.add_header(header, value)

            # Make request to MemoryDB
            try:
                with urllib.request.urlopen(req, timeout=30) as response:
                    # Send response
                    self.send_response(response.getcode())
                    
                    # Copy response headers
                    for header, value in response.headers.items():
                        if header.lower() not in ['connection', 'transfer-encoding']:
                            self.send_header(header, value)
                    
                    self.end_headers()
                    
                    # Copy response body
                    self.wfile.write(response.read())
                    
                    print(f"✅ Proxy success: {response.getcode()}")

            except HTTPError as e:
                print(f"❌ MemoryDB HTTP Error: {e.code} - {e.reason}")
                self.send_response(e.code)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                error_response = {
                    'error': f'MemoryDB Error: {e.reason}',
                    'code': e.code,
                    'node': node
                }
                self.wfile.write(json.dumps(error_response).encode())

            except URLError as e:
                print(f"❌ MemoryDB Connection Error: {e.reason}")
                self.send_response(503)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                error_response = {
                    'error': f'Cannot connect to MemoryDB node {node}: {e.reason}',
                    'suggestion': 'Make sure MemoryDB is running with: ./start-ultra-fast.sh'
                }
                self.wfile.write(json.dumps(error_response).encode())

        except Exception as e:
            print(f"❌ Proxy Error: {e}")
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            error_response = {
                'error': f'Proxy error: {str(e)}',
                'type': type(e).__name__
            }
            self.wfile.write(json.dumps(error_response).encode())

    def log_message(self, format, *args):
        # Custom logging
        if not self.path.startswith('/api/'):
            print(f"📁 Serving: {self.path}")

def main():
    PORT = 8080
    
    # Change to demo-web directory
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    print("🚀 Starting MemoryDB Demo Web Server...")
    print(f"📁 Serving from: {os.getcwd()}")
    print(f"🌐 Server URL: http://localhost:{PORT}")
    print(f"🔗 MemoryDB Nodes: {', '.join(['localhost:8081', 'localhost:8082', 'localhost:8083'])}")
    print("=" * 60)
    
    try:
        with socketserver.TCPServer(("", PORT), CORSProxyHandler) as httpd:
            print(f"✅ Server started on port {PORT}")
            print(f"🌐 Open your browser to: http://localhost:{PORT}")
            print("Press Ctrl+C to stop the server")
            httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n🛑 Server stopped by user")
    except OSError as e:
        if e.errno == 48:  # Address already in use
            print(f"❌ Port {PORT} is already in use")
            print("💡 Try a different port or stop the existing server")
        else:
            print(f"❌ Server error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 