# IBM Bob Integration for watsonx.data MCP Server

## Overview

IBM Bob (VS Code AI assistant) is configured to use the watsonx.data MCP server through the `.bob/mcp.json` configuration file.

## Configuration Status

✅ **MCP Server is configured and ready for IBM Bob to use**

### Configuration Location
`.bob/mcp.json`

### Current Configuration

```json
{
  "mcpServers": {
    "watsonx-projects": {
      "command": "/Users/aseelert/GitHub/ibmas-watsonxdata-dbt/.venv/bin/python",
      "args": [
        "/Users/aseelert/GitHub/ibmas-watsonxdata-dbt/cpd-mcpserver/watsonx_projects_mcp_server.py"
      ],
      "env": {
        "PYTHONPATH": "/Users/aseelert/GitHub/ibmas-watsonxdata-dbt"
      },
      "alwaysAllow": [
        "validate_watsonx_connection",
        "list_projects",
        "check_project_exists",
        "get_project_details"
      ],
      "disabled": false
    },
    "watsonxdata-documents": {
      "command": "/Users/aseelert/GitHub/ibmas-watsonxdata-dbt/.venv/bin/python",
      "args": [
        "/Users/aseelert/GitHub/ibmas-watsonxdata-dbt/cpd-mcpserver/run_mcp_server.py"
      ],
      "env": {
        "PYTHONPATH": "/Users/aseelert/GitHub/ibmas-watsonxdata-dbt"
      },
      "alwaysAllow": [],
      "disabled": false
    }
  }
}
```

## How IBM Bob Uses the MCP Server

When IBM Bob (VS Code extension) starts, it will:

1. **Read `.bob/mcp.json`** configuration
2. **Auto-start the MCP server** using the configured command
3. **Connect via MCP protocol** to communicate with watsonx.data
4. **Discover available tools** (document libraries, project scanning, etc.)
5. **Use tools in responses** when relevant to user queries

## Available Capabilities for IBM Bob

IBM Bob can now:

### 1. Query Document Libraries
```
User: "Search the invoice_document_library for Q4 2023 invoices"
Bob: [Uses MCP tool to query watsonx.data document library]
```

### 2. Scan Project Content
```
User: "What dbt models are in this project?"
Bob: [Uses MCP to scan project structure and list models]
```

### 3. Create DataStage Flows
```
User: "Create a DataStage flow to load customer data"
Bob: [Uses MCP to generate DataStage flow definition]
```

### 4. Extract Metadata
```
User: "Show me the schema for the gold_customer_360 table"
Bob: [Uses MCP to extract and display schema information]
```

### 5. Analyze Schemas
```
User: "Compare schemas between bronze and silver layers"
Bob: [Uses MCP to analyze and compare schemas]
```

## Verification

To verify Bob can use the MCP server:

1. **Check server availability:**
   ```bash
   python cpd-mcpserver/test_mcp_server.py
   ```

2. **Ask Bob to use it:**
   ```
   "Can you check if the watsonx.data MCP server is available?"
   ```

3. **Test a capability:**
   ```
   "List all document libraries in watsonx.data"
   ```

## Troubleshooting

### IBM Bob can't connect to MCP server

1. **Check if server is configured:**
   ```bash
   cat .bob/mcp.json
   ```

2. **Verify Python path:**
   ```bash
   ls -la /Users/aseelert/GitHub/ibmas-watsonxdata-dbt/.venv/bin/python
   ```

3. **Test server manually:**
   ```bash
   python cpd-mcpserver/test_mcp_server.py
   ```

### IBM Bob doesn't see MCP tools

1. **Restart VS Code** to reload Bob configuration
2. **Check `.bob/mcp-servers.json`** exists and is valid JSON
3. **Check server logs** for errors
4. **Verify .env variables** are set correctly

## Architecture

```
┌─────────────────────┐
│     IBM Bob         │
│  (VS Code Extension)│
└──────────┬──────────┘
           │ Reads .bob/mcp-servers.json
           │ MCP Protocol (Auto-started)
           ▼
┌─────────────────────┐
│   MCP Server        │
│  run_mcp_server.py  │
│   (Port 45xxx)      │
└──────────┬──────────┘
           │ Loads .env
           │ HTTPS + CA Cert
           ▼
┌─────────────────────┐
│  watsonx.data       │
│  Document Libraries │
└─────────────────────┘
```

## Enabling MCP in IBM Bob

To enable MCP support in IBM Bob:

1. **Ensure `.bob/mcp-servers.json` exists** (already created)
2. **Restart VS Code** to load the configuration
3. **IBM Bob will auto-discover** the MCP server
4. **Tools will be available** in Bob's context

## Next Steps

1. **Restart VS Code** to load the MCP configuration
2. **Ask IBM Bob to use the MCP server** - It's ready to go!
3. **Create document libraries** in watsonx.data if none exist
4. **Monitor usage** through server logs

---

**Status:** ✅ Configured and Ready  
**Last Updated:** 2026-06-25