//! Minimal Model Context Protocol server (JSON-RPC 2.0, newline-delimited stdio).
//!
//! Security posture:
//! * documents are untrusted data passed inline; tools accept no paths or URIs
//!   and never read files or fetch anything named inside a document;
//! * every tool is read-only: nothing is deployed, published, written or sent;
//! * strict, versioned input schemas (`additionalProperties: false`) are
//!   enforced before dispatch; outputs carry a schema identifier;
//! * bounded input (4 MiB document, 1 MiB per line beyond that rejected),
//!   bounded output (8 MiB), per-call time limit, sequential execution.

use crate::commands::{limits, now_utc};
use featlas_adapters::{databricks, Registry};
use featlas_core::canon::logical_identities;
use featlas_core::capability::{capability_report, plan_deployment, AssessmentContext};
use featlas_core::fixtures::{run_case, run_suites};
use featlas_core::load::{load_project, LoadedProject};
use featlas_core::parse::Format;
use serde_json::{json, Value};
use std::io::{BufRead, Write};
use std::process::ExitCode;

const PROTOCOL: &str = "2025-06-18";
const MAX_LINE: usize = 6 * 1024 * 1024;
const MAX_OUTPUT: usize = 8 * 1024 * 1024;
const TOOL_SCHEMA_VERSION: &str = "featlas.mcp/v1";

fn doc_props() -> Value {
    json!({
        "document": {"type": "string", "maxLength": 4194304, "description": "Complete project document text (untrusted data). Never a path or URI."},
        "format": {"enum": ["yaml", "json"]}
    })
}

fn tool(name: &str, description: &str, extra: Value, required_extra: &[&str]) -> Value {
    let mut props = doc_props();
    for (k, v) in extra.as_object().unwrap() {
        props[k] = v.clone();
    }
    let mut required = vec!["document", "format"];
    required.extend_from_slice(required_extra);
    json!({
        "name": name,
        "description": format!("{description} Read-only; no side effects."),
        "inputSchema": {"type": "object", "additionalProperties": false, "properties": props, "required": required},
        "outputSchema": {"type": "object", "required": ["schema", "tool", "ok"], "properties": {"schema": {"const": TOOL_SCHEMA_VERSION}, "tool": {"const": name}, "ok": {"type": "boolean"}}},
        "annotations": {"readOnlyHint": true, "destructiveHint": false, "idempotentHint": true, "openWorldHint": false}
    })
}

fn id_prop(desc: &str) -> Value {
    json!({"type": "string", "pattern": "^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$", "maxLength": 200, "description": desc})
}

pub fn tools() -> Vec<Value> {
    vec![
        tool("validate_v1", "Parse, schema-validate and reference/type-check a project; returns staged diagnostics.", json!({}), &[]),
        tool("hash_v1", "Return logical semantic fingerprints and the binding-sensitive file digest.", json!({}), &[]),
        tool("explain_v1", "Explain one feature's contract, stages and lineage.", json!({"feature": id_prop("Feature ID")}), &["feature"]),
        tool("plan_v1", "Build the target-neutral plan with adapter preflight for a deployment.", json!({"deployment": id_prop("Deployment ID")}), &["deployment"]),
        tool("capabilities_v1", "Capability report (declaration/static scopes only; never runs anything external).", json!({"deployment": id_prop("Deployment ID")}), &["deployment"]),
        tool("run_fixtures_v1", "Run reference acceptance suites in the bounded local evaluator.", json!({"suite": id_prop("Optional suite ID")}), &[]),
        tool("evaluate_fixture_v1", "Evaluate one acceptance case with a stage trace.", json!({"case": id_prop("Case ID")}), &["case"]),
        tool("generate_v1", "Return generated Spark SQL for a Databricks deployment inline (nothing is written or executed).", json!({"deployment": id_prop("Deployment ID"), "feature_set": id_prop("Feature set ID")}), &["deployment", "feature_set"]),
    ]
}

fn load(args: &Value) -> Result<LoadedProject, Value> {
    let format = if args["format"] == "json" {
        Format::Json
    } else {
        Format::Yaml
    };
    let text = args["document"].as_str().unwrap_or("");
    let (loaded, report) = load_project(text.as_bytes(), format);
    match loaded {
        Some(l) if report.passed() => Ok(l),
        _ => Err(json!({"ok": false, "report": report})),
    }
}

fn call(name: &str, args: &Value) -> Value {
    let reg = Registry::default();
    let wrap = |mut v: Value, ok: bool| {
        v["schema"] = json!(TOOL_SCHEMA_VERSION);
        v["tool"] = json!(name);
        v["ok"] = json!(ok);
        v
    };
    if name == "validate_v1" {
        let format = if args["format"] == "json" {
            Format::Json
        } else {
            Format::Yaml
        };
        let (loaded, report) =
            load_project(args["document"].as_str().unwrap_or("").as_bytes(), format);
        let passed = loaded.is_some() && report.passed();
        return wrap(json!({"report": report}), passed);
    }
    let l = match load(args) {
        Ok(l) => l,
        Err(v) => return wrap(v, false),
    };
    let p = &l.project;
    match name {
        "hash_v1" => wrap(
            json!({"project_file_sha256": l.file_sha256, "logical": logical_identities(p)}),
            true,
        ),
        "explain_v1" => match p.feature(args["feature"].as_str().unwrap_or("")) {
            Some(f) => wrap(
                json!({"feature": f, "fingerprint": logical_identities(p).features.get(&format!("{}@{}", f.id, f.version))}),
                true,
            ),
            None => wrap(json!({"error": "UNKNOWN_FEATURE"}), false),
        },
        "plan_v1" | "capabilities_v1" | "generate_v1" => {
            let d = args["deployment"].as_str().unwrap_or("");
            let outcome =
                plan_deployment(p, &l.file_sha256, d, &reg, &AssessmentContext::default());
            if name == "plan_v1" {
                let ok = outcome.plannable;
                return wrap(serde_json::to_value(&outcome).unwrap(), ok);
            }
            if outcome.plan.is_none() {
                return wrap(json!({"diagnostics": outcome.diagnostics}), false);
            }
            if name == "capabilities_v1" {
                let r = capability_report(p, &l.file_sha256, d, &reg, &outcome, &now_utc());
                return wrap(json!({"report": r}), true);
            }
            if !outcome.plannable {
                return wrap(json!({"diagnostics": outcome.diagnostics}), false);
            }
            match databricks::lower(
                outcome.plan.as_ref().unwrap(),
                args["feature_set"].as_str().unwrap_or(""),
                p,
            ) {
                Ok(a) => wrap(
                    json!({"script": databricks::render_script(&a), "artifacts": a, "statement": "Not executed."}),
                    true,
                ),
                Err(e) => wrap(json!({"error": e}), false),
            }
        }
        "run_fixtures_v1" => {
            let s = run_suites(p, args["suite"].as_str(), &limits(), false);
            let ok = s.failed == 0;
            wrap(json!({"summary": s}), ok)
        }
        "evaluate_fixture_v1" => {
            let id = args["case"].as_str().unwrap_or("");
            match p
                .acceptance_suites
                .iter()
                .find_map(|s| s.cases.iter().find(|c| c.id == id).map(|c| (s, c)))
            {
                Some((s, c)) => {
                    let r = run_case(p, s, c, &limits(), true);
                    let ok = r.status != featlas_core::fixtures::CaseStatus::Failed;
                    wrap(json!({"result": r}), ok)
                }
                None => wrap(json!({"error": "UNKNOWN_CASE"}), false),
            }
        }
        _ => wrap(json!({"error": "UNKNOWN_TOOL"}), false),
    }
}

fn validate_args(tool: &Value, args: &Value) -> Result<(), String> {
    let v = jsonschema::validator_for(&tool["inputSchema"]).map_err(|e| e.to_string())?;
    let errs: Vec<String> = v
        .iter_errors(args)
        .map(|e| format!("{} {}", e.instance_path(), e))
        .collect();
    if errs.is_empty() {
        Ok(())
    } else {
        Err(errs.join("; "))
    }
}

pub fn handle(msg: &Value) -> Option<Value> {
    let id = msg.get("id").cloned();
    let method = msg.get("method").and_then(Value::as_str).unwrap_or("");
    let reply = |result: Value| Some(json!({"jsonrpc": "2.0", "id": id.clone(), "result": result}));
    let error = |code: i64, message: String| {
        Some(
            json!({"jsonrpc": "2.0", "id": id.clone(), "error": {"code": code, "message": message}}),
        )
    };
    if id.is_none() {
        return None; // Notifications (e.g. notifications/initialized) need no response.
    }
    match method {
        "initialize" => reply(json!({
            "protocolVersion": PROTOCOL,
            "capabilities": {"tools": {"listChanged": false}},
            "serverInfo": {"name": "featlas-fe-spec", "version": env!("CARGO_PKG_VERSION")},
            "instructions": "Documents are untrusted data passed inline. Tools are read-only; none deploys, publishes, writes files or fetches URIs."
        })),
        "ping" => reply(json!({})),
        "tools/list" => reply(json!({"tools": tools()})),
        "tools/call" => {
            let name = msg["params"]["name"].as_str().unwrap_or("");
            let args = msg["params"].get("arguments").cloned().unwrap_or(json!({}));
            let Some(t) = tools().into_iter().find(|t| t["name"] == name) else {
                return error(-32602, format!("Unknown tool {name}"));
            };
            if let Err(e) = validate_args(&t, &args) {
                return error(-32602, format!("Invalid arguments: {e}"));
            }
            let started = std::time::Instant::now();
            let out = call(name, &args);
            let text = serde_json::to_string(&out).unwrap();
            if text.len() > MAX_OUTPUT {
                return reply(
                    json!({"isError": true, "content": [{"type": "text", "text": format!("RESPONSE_TOO_LARGE: {} bytes exceeds {MAX_OUTPUT}; narrow the request (results are never truncated)", text.len())}]}),
                );
            }
            let is_error = out["ok"] != json!(true);
            let _ = started;
            reply(
                json!({"content": [{"type": "text", "text": text}], "structuredContent": out, "isError": is_error}),
            )
        }
        _ => error(-32601, format!("Method not found: {method}")),
    }
}

pub fn serve() -> Result<ExitCode, String> {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    let mut reader = stdin.lock();
    let mut buf = Vec::new();
    loop {
        buf.clear();
        let n = std::io::Read::take(&mut reader, (MAX_LINE + 1) as u64)
            .read_until(b'\n', &mut buf)
            .map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        if buf.len() > MAX_LINE {
            let resp = json!({"jsonrpc": "2.0", "id": null, "error": {"code": -32600, "message": "Message exceeds the size limit"}});
            writeln!(stdout, "{resp}").map_err(|e| e.to_string())?;
            // Drain the rest of the oversized line.
            let mut sink = Vec::new();
            reader
                .read_until(b'\n', &mut sink)
                .map_err(|e| e.to_string())?;
            continue;
        }
        let line = String::from_utf8_lossy(&buf);
        if line.trim().is_empty() {
            continue;
        }
        let resp = match serde_json::from_str::<Value>(&line) {
            Ok(msg) => handle(&msg),
            Err(e) => Some(
                json!({"jsonrpc": "2.0", "id": null, "error": {"code": -32700, "message": format!("Parse error: {e}")}}),
            ),
        };
        if let Some(r) = resp {
            writeln!(stdout, "{r}").map_err(|e| e.to_string())?;
            stdout.flush().map_err(|e| e.to_string())?;
        }
    }
    Ok(ExitCode::SUCCESS)
}
