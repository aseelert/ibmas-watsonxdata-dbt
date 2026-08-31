# Agent tools and MCP

The local MCP server is deliberately separate from the demo runtime. It reads
credentials from the repository `.env`; no credential-bearing client config is
committed. Configure Codex, Claude, or Bob to run the same stdio command:

```sh
python3 09-agent-tools/mcp-server/watsonx_projects_mcp_server.py
```

It exposes project discovery and connection validation only. Its README must
not claim document-library search or DataStage authoring unless those tools are
implemented and tested.
