---
name : datastage-data-integration
description : Use this skill when working with IBM watsonx.data Agentic Data Integration (DataStage) via the remote MCP server. Trigger when the user says things like "create a DataStage job", "build a data integration pipeline", "design an ETL flow", "run a DataStage flow", "integrate data with DataStage", "orchestrate a data pipeline", "transform data with DataStage", or "use agentic data integration".
---

# IBM Agentic Data Integration (DataStage) — MCP Guide

## Overview

This skill covers the **IBM Agentic Data Integration MCP server** (`ibm-agentic-data-integration-mcp-remote`), which exposes IBM DataStage pipeline capabilities as MCP tools via a remote streamable-HTTP endpoint hosted on IBM Cloud (`api.ca-tor.dai.cloud.ibm.com`).

Use this skill any time the user wants to:
- Design, build, or run DataStage data integration flows
- Orchestrate ETL/ELT pipelines from natural language descriptions
- Integrate data between sources and targets (databases, files, object storage, streaming)
- Generate DataStage job definitions or pipeline YAML from a description
- Trigger or monitor DataStage flow executions

> **Connection**: `streamable-http` remote — no local process needed. Requires a valid IBM Cloud API key in the `Authorization` header.

---

## Phase 0: Setup & Connectivity Check

Before attempting any pipeline work, verify the server is reachable and the API key is configured:

1. Check that `ibm-agentic-data-integration-mcp-remote` is listed as an active MCP server in `.bob/mcp.json`.
2. Confirm the `Authorization` header contains a real IBM Cloud API key (replace `<your-api-key>` with the actual key).
3. If the user gets a `401 Unauthorized` or `403 Forbidden` error, ask them to provide a valid IBM Cloud API key.

> **API key format**: `ApiKey <IBM_CLOUD_APIKEY>` — the prefix `ApiKey ` (with a space) is required.

---

## Phase 1: Understand the Integration Request

Gather the following before calling any tools:

| Question | Why it matters |
|----------|---------------|
| What are the **source(s)**? | DB2, PostgreSQL, S3, Kafka, flat file, watsonx.data, etc. |
| What are the **target(s)**? | Where should the data land? |
| What **transformations** are needed? | Filter, join, aggregate, cleanse, enrich? |
| What is the **schedule / trigger**? | On-demand, scheduled, event-driven? |
| Is this a **new flow** or modifying an existing one? | Creation vs update path |

If any required detail is missing, ask the user before proceeding.

---

## Phase 2: Design the Pipeline

Use the Agentic Data Integration MCP tools to translate the user's description into a DataStage pipeline definition.

Typical tool invocation pattern (tool names may vary — discover available tools from the server):

1. **Describe the pipeline** — pass the natural language description to the relevant "design" or "generate" tool.
2. **Review the generated flow definition** — present the output to the user for confirmation before submission.
3. **Refine if needed** — iterate on the definition based on user feedback.

### Pipeline Design Best Practices

- Always confirm source/target connection names match what is configured in the DataStage project.
- For join operations, confirm the join keys with the user.
- For aggregations, confirm the grouping columns and aggregate functions.
- If data cleansing is needed, describe the rules explicitly (null handling, type casting, deduplication).

---

## Phase 3: Submit & Execute

Once the pipeline definition is confirmed:

1. Submit the flow definition via the appropriate MCP tool.
2. Capture the returned **flow ID** or **job run ID**.
3. Monitor execution status — poll or stream status updates until the run completes.
4. Report the outcome:
   - Records read / written
   - Any errors or warnings
   - Execution duration

---

## Phase 4: Validation & Handoff

After a successful run:

1. Optionally trigger a data quality check on the target asset using the `ibm-watsonx-data-intelligence` MCP server:
   ```
   get_data_quality_for_asset(asset_id_or_name=<target>, container_id_or_name=<project>, container_type="project")
   ```
2. Summarise what was integrated:
   - Source → Target
   - Records processed
   - Transformations applied
   - Next scheduled run (if applicable)

---

## Configuration Reference

The server entry in [`.bob/mcp.json`](.bob/mcp.json):

```json
"ibm-agentic-data-integration-mcp-remote": {
  "type": "streamable-http",
  "url": "https://api.ca-tor.dai.cloud.ibm.com/data_intg_ai/v1/mcp",
  "headers": {
    "Authorization": "ApiKey <your-api-key>"
  },
  "disabled": false,
  "alwaysAllow": []
}
```

**To activate**: replace `<your-api-key>` with a real IBM Cloud API key that has access to the DataStage / Data Integration AI service.

**Region**: `ca-tor` (Toronto). If your DataStage instance is in a different region, update the `url` hostname accordingly (e.g. `api.us-south.dai.cloud.ibm.com`).

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `401 Unauthorized` | Missing or invalid API key | Replace `<your-api-key>` in `mcp.json` headers |
| `403 Forbidden` | API key lacks DataStage service permissions | Grant IAM access to the Data Integration AI service |
| `404 Not Found` on the MCP URL | Wrong region or service not provisioned | Check service URL and that the service is enabled in your IBM Cloud account |
| Tool list empty | Server connected but no tools exposed | Verify the API key has the correct service role |
| `disabled: true` | Server is turned off | Set `"disabled": false` in `mcp.json` |

---

[//]: # (Copyright [2026] [IBM])
[//]: # (Licensed under the Apache License, Version 2.0 \(http://www.apache.org/licenses/LICENSE-2.0\))
[//]: # (See the LICENSE file in the project root for license information.)
