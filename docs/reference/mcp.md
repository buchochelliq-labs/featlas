# MCP server

**In plain terms:** `fe-spec mcp serve` lets an AI assistant or any Model Context Protocol
client ask featlas the same questions the CLI answers. You paste the project document into the
request; the server never opens files, never fetches URLs and never changes anything.

## Protocol

* JSON-RPC 2.0, one message per line on stdin/stdout, protocol version `2025-06-18`
  (`crates/featlas-cli/src/mcp.rs`).
* Methods: `initialize`, `ping`, `tools/list`, `tools/call`. Notifications (no `id`) get no
  response; other methods return `-32601`.
* Limits: 6 MiB per line (larger messages get `-32600` and the line is drained), 4 MiB per
  `document` (input schema `maxLength`), 8 MiB per response (an oversized result returns
  `RESPONSE_TOO_LARGE`; results are never truncated). Calls run sequentially.
* Arguments are validated against each tool's input schema (`additionalProperties: false`)
  before dispatch; invalid arguments return `-32602`.

`initialize` response on this commit:

```json
{"jsonrpc": "2.0", "id": 1, "result": {"protocolVersion": "2025-06-18", "capabilities": {"tools": {"listChanged": false}}, "serverInfo": {"name": "featlas-fe-spec", "version": "0.1.0"}, "instructions": "Documents are untrusted data passed inline. Tools are read-only; none deploys, publishes, writes files or fetches URIs."}}
```

## Tools

Every tool takes `document` (the complete project text, untrusted data, never a path) and
`format` (`yaml` or `json`). IDs are restricted to `^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$`, max 200
characters. All tools are annotated `readOnlyHint: true`, `destructiveHint: false`,
`idempotentHint: true`, `openWorldHint: false`.

| Tool | Extra arguments | Returns | `ok` is true when |
| --- | --- | --- | --- |
| `validate_v1` | — | staged validation report | document passed |
| `hash_v1` | — | logical fingerprints and file digest | call succeeded |
| `explain_v1` | `feature` | feature contract, stages, lineage | call succeeded |
| `plan_v1` | `deployment` | `outcome` (plan, checks, diagnostics) and `plannable` | plannable |
| `capabilities_v1` | `deployment` | `report` (declaration/static scopes only; no evidence is supplied) | call succeeded |
| `run_fixtures_v1` | `suite` (optional) | suite summary | no case failed |
| `evaluate_fixture_v1` | `case` | one case result with trace | case did not fail |
| `generate_v1` | `deployment`, `feature_set` | `script` (Databricks Spark SQL), `artifacts`, `manifest`, `"statement": "Not executed."` | call succeeded |

Results come back both as `structuredContent` and as a JSON text block, with
`"schema": "featlas.mcp/v1"`, `tool` and `ok`; `isError` is `!ok`. API errors are returned as
`{"error": {...}}` with `ok: false`.

## Example session

Driving the server with a small script that sends `initialize`, `tools/list`, `hash_v1` and
`plan_v1` (example 01, deployment `fault_prod`) prints, on this commit:

```text
['validate_v1', 'hash_v1', 'explain_v1', 'plan_v1', 'capabilities_v1', 'run_fixtures_v1', 'evaluate_fixture_v1', 'generate_v1']
3 isError False ok True ['project_file_sha256', 'logical', 'deployments', 'schema', 'operation', 'tool', 'ok']
6d512fd95cd2abe19da68d7622d659caad853f3eb39bb790918c622840c72cdb
4 isError True ok False ['outcome', 'plannable', 'schema', 'tool', 'ok']
False ['UNRESOLVED_ADAPTER', 'CAPABILITY_REQUIREMENT_DERIVED']
```

The `hash_v1` logical project fingerprint equals the CLI's, and `plan_v1` reports the same
`UNRESOLVED_ADAPTER` for the synthetic pins as `fe-spec plan`, because both call the same
`featlas-api` functions.

A minimal request line:

```json
{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"hash_v1","arguments":{"document":"<project text>","format":"yaml"}}}
```

## Client configuration

Configure the client to launch `fe-spec mcp serve` as a stdio server (path to the built binary);
no arguments, environment variables or credentials are needed.
