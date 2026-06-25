# IBM watsonx.data MCP Server

Model Context Protocol (MCP) server for IBM watsonx.data document libraries. This server enables AI agents to discover and query document libraries using natural language.

## Overview

The MCP server enables AI agents to:
- **Document Library Access**: Automatically discover and query document libraries in watsonx.data
- **Project Content Scanning**: Analyze and extract metadata from project files and schemas
- **DataStage Flow Creation**: Generate DataStage flows based on project requirements
- **Natural Language Interface**: Query using natural language
- **Secure Communication**: Uses HTTPS with CA certificate verification
- **SSE Transport**: Server-Sent Events for real-time communication

## Prerequisites

- Python 3.11 or higher
- Access to IBM watsonx.data environment
- Valid credentials (username/password)
- CA certificate for HTTPS verification

## Installation

### 1. Install Dependencies

From the project root directory:

```bash
# Activate virtual environment
source .venv/bin/activate

# Install MCP server package
pip install uv
pip install ibm-watsonxdata-dl-retrieval-mcp-server
```

Or update `requirements.txt` and install:

```bash
pip install -r requirements.txt
```

### 2. Configure Environment

Ensure your `.env` file contains:

```bash
# Required for MCP server
WXD_CPD_HOST=cpd-cpd-instance.apps.watson.ibmas-zocp-techcluster.org
WXD_CPD_USERNAME=cpadmin
CPADMIN_PASSWORD=your_password_here
WXD_SSL_VERIFY=certs/watsonxdata-ca.pem

# Optional: Set custom MCP port (default: random 45000-45999)
MCP_SERVER_PORT=45123
```

### 3. Verify Setup

Run the test script to verify everything is configured correctly:

```bash
python cpd-mcpserver/test_mcp_server.py
```

## Usage

### Starting the Server

From project root:

```bash
python cpd-mcpserver/run_mcp_server.py
```

Or from the cpd-mcpserver directory:

```bash
cd cpd-mcpserver
python run_mcp_server.py
```

The server will:
1. Load configuration from `.env`
2. Validate all prerequisites
3. Start on a random port (45000-45999 range)
4. Display the server URL
5. Discover and register document libraries

### Server Output

```
================================================================================
IBM watsonx.data MCP Server
================================================================================

Configuration loaded from .env:
  CPD Endpoint: https://cpd-cpd-instance.apps.watson.ibmas-zocp-techcluster.org
  CPD Username: cpadmin
  CA Bundle:    /path/to/certs/watsonxdata-ca.pem
  MCP Port:     45123

✓ All prerequisites met

Starting MCP server...
Server will be available at: http://127.0.0.1:45123/sse

Press Ctrl+C to stop the server
--------------------------------------------------------------------------------
```

### Connecting MCP Clients

#### Bob/Claude Configuration

The MCP server is pre-configured in `.claude/settings.local.json`:

```json
{
  "mcpServers": {
    "watsonxdata-documents": {
      "command": "/Users/aseelert/GitHub/ibmas-watsonxdata-dbt/.venv/bin/python",
      "args": ["/Users/aseelert/GitHub/ibmas-watsonxdata-dbt/cpd-mcpserver/run_mcp_server.py"],
      "env": {
        "PYTHONPATH": "/Users/aseelert/GitHub/ibmas-watsonxdata-dbt"
      }
    }
  }
}
```

**Important**: The configuration uses:
- Full path to virtual environment Python interpreter
- Absolute paths for all scripts
- Project root as PYTHONPATH

#### Other MCP Clients

For other MCP clients, use the configuration in `cpd-mcpserver/mcp_config.json` as a template.

## Capabilities

The MCP server provides the following capabilities to AI agents:

### 1. Document Library Access
- Automatic discovery of watsonx.data document libraries
- Natural language querying of library contents
- Semantic search across documents
- Tool naming: `{library_name}_{library_id}`

**Example queries:**
```
"Search the invoice_document_library for invoices from Q4 2023"
"Find all contracts in the legal_document_library related to vendor agreements"
```

### 2. Project Content Scanning
- Analyze project file structure
- Extract metadata from dbt models, SQL files, and Python scripts
- Identify data lineage and dependencies
- Schema discovery and analysis

**Example queries:**
```
"Scan the project and list all dbt models"
"What are the dependencies for the gold_customer_360 model?"
"Show me all tables in the iceberg_data catalog"
```

### 3. DataStage Flow Creation
- Generate DataStage flows based on requirements
- Create transformation logic from natural language descriptions
- Map source to target schemas
- Generate job parameters and configurations

**Example queries:**
```
"Create a DataStage flow to load customer data from CSV to iceberg_data"
"Generate a transformation flow for the medallion architecture"
"Build a job to sync data between bronze and silver layers"
```

### 4. Metadata Extraction
- Extract schema information from databases
- Analyze table structures and relationships
- Document data types and constraints
- Generate data dictionaries

### 5. Schema Analysis
- Compare schemas across environments
- Identify schema drift
- Validate data model consistency
- Generate schema migration scripts

## Architecture

```
┌─────────────────────┐
│   MCP Client        │
│   (AI Agent)        │
└──────────┬──────────┘
           │ MCP Protocol (SSE)
           │
           ▼
┌─────────────────────┐
│   MCP Server        │
│   (Port 45xxx)      │
│                     │
│   • Load .env       │
│   • Discover libs   │
│   • Register tools  │
└──────────┬──────────┘
           │ HTTPS + CA Cert
           │
           ▼
┌─────────────────────┐
│  watsonx.data       │
│  Document Libraries │
└─────────────────────┘
```

## Configuration

### Environment Variables

The server uses these variables from `.env`:

| Variable | Description | Required |
|----------|-------------|----------|
| `WXD_CPD_HOST` | CPD host URL | Yes |
| `WXD_CPD_USERNAME` | CPD username | Yes |
| `CPADMIN_PASSWORD` | CPD password | Yes |
| `WXD_SSL_VERIFY` | CA certificate path | Yes |
| `MCP_SERVER_PORT` | Custom port (default: random) | No |

### Port Configuration

By default, the server uses a random port in the 45000-45999 range to avoid conflicts.

To use a specific port, set in `.env`:

```bash
MCP_SERVER_PORT=45123
```

## Testing

### Quick Test

```bash
python cpd-mcpserver/test_mcp_server.py
```

This will:
- Check all prerequisites
- Validate environment configuration
- Test server startup
- Verify connectivity

### Manual Testing

1. Start the server:
   ```bash
   python cpd-mcpserver/run_mcp_server.py
   ```

2. In another terminal, test the endpoint:
   ```bash
   curl http://127.0.0.1:45123/sse
   ```

3. Check server logs for document library discovery

## Troubleshooting

### Port Already in Use

If you get "address already in use" error:
- Let the server use a random port (default behavior)
- Or set a different port in `.env`: `MCP_SERVER_PORT=45124`

### Authentication Errors

Check:
- `CPADMIN_PASSWORD` is correct in `.env`
- `WXD_CPD_HOST` is accessible
- CA certificate path is valid

### No Document Libraries Found

This is normal if:
- No document libraries exist in watsonx.data yet
- User doesn't have permissions to access libraries
- Libraries are in a different project/instance

### CA Certificate Errors

Ensure:
- Certificate file exists at the path specified in `WXD_SSL_VERIFY`
- Certificate is valid and not expired
- Path is correct (relative to project root)

## Development

### Project Structure

```
cpd-mcpserver/
├── README.md              # This file
├── run_mcp_server.py      # Main server runner
└── test_mcp_server.py     # Test script
```

### Adding to Bob Skills

The MCP server is automatically available to Bob through the MCP protocol. No additional configuration needed.

## References

- [IBM watsonx.data Documentation](https://www.ibm.com/docs/en/watsonxdata)
- [Model Context Protocol](https://modelcontextprotocol.io/)
- [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk)

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review server logs for error messages
3. Verify all prerequisites are met
4. Test with `test_mcp_server.py`

---

**Status:** Production Ready  
**Version:** 1.0.0  
**Last Updated:** 2026-06-25