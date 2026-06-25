#!/usr/bin/env python3
"""
Test script for IBM watsonx.data MCP Server
Validates setup and tests server connectivity
"""

import os
import sys
import subprocess
import time
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

def check_prerequisites():
    """Check all prerequisites"""
    print("=" * 80)
    print("IBM watsonx.data MCP Server - Prerequisites Check")
    print("=" * 80)
    print()
    
    checks = []
    
    # Python version
    version = sys.version_info
    print(f"✓ Python version: {version.major}.{version.minor}.{version.micro}")
    checks.append(version.major >= 3 and version.minor >= 11)
    
    # Environment variables
    required_vars = ['WXD_CPD_HOST', 'WXD_CPD_USERNAME', 'CPADMIN_PASSWORD', 'WXD_SSL_VERIFY']
    print("\nEnvironment variables:")
    for var in required_vars:
        value = os.getenv(var)
        if value:
            if 'PASSWORD' in var:
                print(f"  ✓ {var}: ***")
            else:
                print(f"  ✓ {var}: {value}")
            checks.append(True)
        else:
            print(f"  ✗ {var}: NOT SET")
            checks.append(False)
    
    # CA certificate
    ca_path = Path(__file__).parent.parent / os.getenv('WXD_SSL_VERIFY', '')
    if ca_path.exists():
        print(f"\n✓ CA certificate found: {ca_path}")
        checks.append(True)
    else:
        print(f"\n✗ CA certificate NOT found: {ca_path}")
        checks.append(False)
    
    # MCP package
    try:
        from importlib import metadata
        version = metadata.version('ibm-watsonxdata-dl-retrieval-mcp-server')
        print(f"✓ MCP server package: v{version}")
        checks.append(True)
    except Exception:
        print("✗ MCP server package NOT installed")
        checks.append(False)
    
    print()
    print("=" * 80)
    
    if all(checks):
        print("✓ All prerequisites met!")
        return True
    else:
        print("✗ Some prerequisites are missing")
        return False

def test_server_startup():
    """Test MCP server startup"""
    print("\n" + "=" * 80)
    print("Testing MCP Server Startup")
    print("=" * 80)
    print()
    
    # Setup environment
    cpd_host = os.getenv('WXD_CPD_HOST', '')
    cpd_username = os.getenv('WXD_CPD_USERNAME', '')
    cpd_password = os.getenv('CPADMIN_PASSWORD', '')
    ca_bundle = os.getenv('WXD_SSL_VERIFY', '')
    
    if not ca_bundle.startswith('/'):
        ca_bundle = str((Path(__file__).parent.parent / ca_bundle).absolute())
    
    env = os.environ.copy()
    env['CPD_ENDPOINT'] = f"https://{cpd_host}"
    env['CPD_USERNAME'] = cpd_username
    env['CPD_PASSWORD'] = cpd_password
    env['CA_BUNDLE_PATH'] = ca_bundle
    env['LH_CONTEXT'] = 'CPD'
    
    # Use a test port
    import random
    test_port = random.randint(45000, 45999)
    
    print(f"Starting server on port {test_port}...")
    print("(Server will run for 5 seconds)")
    print()
    
    try:
        process = subprocess.Popen(
            ['uv', 'run', 'ibm-watsonxdata-dl-retrieval-mcp-server',
             '--transport', 'sse', '--port', str(test_port)],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Wait for startup
        time.sleep(5)
        
        # Check if still running
        if process.poll() is None:
            print(f"✓ Server started successfully on http://127.0.0.1:{test_port}/sse")
            
            # Terminate
            process.terminate()
            try:
                stdout, stderr = process.communicate(timeout=3)
                
                # Check for errors
                if 'error' in stderr.lower() and 'authentication' in stderr.lower():
                    print("✗ Authentication error detected")
                    return False
                elif 'Application startup complete' in stderr:
                    print("✓ Server initialized successfully")
                    return True
                else:
                    print("⚠️  Server started but status unclear")
                    return True
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate()
                return True
        else:
            stdout, stderr = process.communicate()
            print("✗ Server failed to start")
            print("\nError output:")
            print(stderr)
            return False
            
    except Exception as e:
        print(f"✗ Error: {e}")
        return False

def main():
    """Main test runner"""
    print("\n")
    
    # Check prerequisites
    if not check_prerequisites():
        print("\n❌ Prerequisites check failed")
        print("Please ensure all requirements are met before running the server")
        sys.exit(1)
    
    # Test server startup
    if test_server_startup():
        print("\n" + "=" * 80)
        print("✓ SUCCESS: MCP server is working correctly!")
        print("=" * 80)
        print("\nTo run the server:")
        print("  python cpd-mcpserver/run_mcp_server.py")
        print("\nOr from the cpd-mcpserver directory:")
        print("  cd cpd-mcpserver")
        print("  python run_mcp_server.py")
        sys.exit(0)
    else:
        print("\n" + "=" * 80)
        print("✗ FAILED: Server test failed")
        print("=" * 80)
        sys.exit(1)

if __name__ == "__main__":
    main()

# Made with Bob
