//! Transport tests: the CLI and MCP server expose the same core results.

use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_fe-spec")
}

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn example(name: &str) -> String {
    root()
        .join("spec/feature-engineering-spec-v0.2.0/examples")
        .join(name)
        .to_string_lossy()
        .into()
}

fn project(name: &str) -> String {
    root().join("projects").join(name).to_string_lossy().into()
}

fn run(args: &[&str]) -> (i32, String) {
    let out = Command::new(bin()).args(args).output().unwrap();
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).into(),
    )
}

fn json_of(text: &str) -> Value {
    serde_json::from_str(text).unwrap_or_else(|e| panic!("{e}: {text}"))
}

#[test]
fn validate_and_test_examples() {
    for name in [
        "00_minimal_template.yaml",
        "01_fault_count.yaml",
        "06_store_migration.yaml",
    ] {
        let (code, out) = run(&["validate", &example(name), "--format", "json"]);
        assert_eq!(code, 0, "{out}");
        assert_eq!(json_of(&out)["passed"], json!(true));
    }
    let (code, out) = run(&["test", &example("01_fault_count.yaml"), "--format", "json"]);
    assert_eq!(code, 0);
    let v = json_of(&out);
    assert_eq!(v["summary"]["passed"], json!(13));
    let (code, out) = run(&[
        "test",
        &example("04_fitted_and_custom_contracts.yaml"),
        "--format",
        "json",
    ]);
    assert_eq!(code, 0);
    let v = json_of(&out);
    assert_eq!(v["summary"]["passed"], json!(3));
    assert_eq!(v["summary"]["not_executed"], json!(3));
}

#[test]
fn broken_document_fails_with_located_diagnostic() {
    let dir = std::env::temp_dir().join(format!("featlas-cli-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let text = std::fs::read_to_string(example("00_minimal_template.yaml"))
        .unwrap()
        .replacen(
            "kind: FeatureEngineeringProject",
            "kind: FeatureEngineeringProject\nkind: Other",
            1,
        );
    let path = dir.join("broken.yaml");
    std::fs::write(&path, text).unwrap();
    let (code, out) = run(&["validate", path.to_str().unwrap(), "--format", "json"]);
    assert_eq!(code, 1);
    let v = json_of(&out);
    let d = &v["report"]["diagnostics"][0];
    assert_eq!(d["code"], json!("PARSE_DUPLICATE_KEY"));
    assert_eq!(d["rule"], json!("SEM-PARSE-001"));
    assert_eq!(d["location"]["line"], json!(5));
}

#[test]
fn synthetic_adapter_pins_do_not_plan() {
    let (code, out) = run(&[
        "plan",
        &example("01_fault_count.yaml"),
        "--deployment",
        "fault_prod",
    ]);
    assert_eq!(code, 1);
    let v = json_of(&out);
    assert!(v["diagnostics"]
        .as_array()
        .unwrap()
        .iter()
        .any(|d| d["code"] == "UNRESOLVED_ADAPTER"));
}

#[test]
fn capability_reports_positive_and_rejecting() {
    let evidence = std::env::temp_dir().join(format!("featlas-evidence-{}", std::process::id()));
    // Local deployment: supported with local_reference evidence, execution-ready locally.
    let (code, out) = run(&[
        "capabilities",
        &project("01_fault_count.yaml"),
        "--deployment",
        "fault_dev",
        "--evidence-dir",
        evidence.to_str().unwrap(),
        "--assessed-at",
        "2026-11-09T00:00:00Z",
    ]);
    assert_eq!(code, 0, "{out}");
    let v = json_of(&out);
    assert_eq!(v["execution_scope"], json!("local_reference"));
    assert_eq!(v["execution_ready"], json!(true));
    assert!(v["checks"]
        .as_array()
        .unwrap()
        .iter()
        .all(|c| c["evidence_scope"] == "local_reference"
            && !c["evidence"].as_array().unwrap().is_empty()));
    // Databricks deployment: static/declaration only, not ready.
    let (code, out) = run(&[
        "capabilities",
        &project("01_fault_count.yaml"),
        "--deployment",
        "fault_prod",
        "--assessed-at",
        "2026-11-09T00:00:00Z",
    ]);
    assert_eq!(code, 0);
    let v = json_of(&out);
    assert_eq!(v["execution_scope"], json!("external_target"));
    assert_eq!(v["execution_ready"], json!(false));
    assert!(v["checks"]
        .as_array()
        .unwrap()
        .iter()
        .all(|c| c["status"] == "unverified"));
    // Custom contract on Databricks: rejected as unsupported.
    let (_, out) = run(&[
        "capabilities",
        &project("04_fitted_and_custom_contracts.yaml"),
        "--deployment",
        "contracts_prod",
        "--assessed-at",
        "2026-11-09T00:00:00Z",
    ]);
    let v = json_of(&out);
    assert!(v["checks"]
        .as_array()
        .unwrap()
        .iter()
        .any(|c| c["capability"] == "custom.priority_score.v1" && c["status"] == "unsupported"));
    assert_eq!(v["execution_ready"], json!(false));
}

#[test]
fn kit_sample_report_lacks_planner_derived_requirements() {
    let report = root().join("spec/feature-engineering-spec-v0.2.0/examples/reports/databricks_capabilities_unverified.json");
    let (code, out) = run(&[
        "check-report",
        report.to_str().unwrap(),
        "--project",
        &example("06_store_migration.yaml"),
    ]);
    assert_eq!(code, 1);
    let v = json_of(&out);
    let codes: Vec<&str> = v["diagnostics"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["code"].as_str().unwrap())
        .collect();
    assert!(
        codes.contains(&"REPORT_REQUIRED_CAPABILITY_MISSING"),
        "{codes:?}"
    );
    assert!(
        !codes.contains(&"REPORT_PROJECT_HASH_MISMATCH"),
        "{codes:?}"
    );
}

#[test]
fn generate_writes_manifest_and_refuses_unsupported() {
    let out = std::env::temp_dir().join(format!("featlas-gen-{}", std::process::id()));
    let (code, _) = run(&[
        "generate",
        &project("02_historical_temperature.yaml"),
        "--deployment",
        "temperature_prod",
        "--out",
        out.to_str().unwrap(),
    ]);
    assert_eq!(code, 0);
    let manifest: Value =
        serde_json::from_str(&std::fs::read_to_string(out.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest["execution_ready"], json!(false));
    assert!(manifest["files"]
        .as_array()
        .unwrap()
        .iter()
        .any(|f| f["path"] == "databricks/device_temperature/script.sql"));
    let script =
        std::fs::read_to_string(out.join("databricks/device_temperature/script.sql")).unwrap();
    // Availability filtering precedes revision selection, which precedes the event window and predicate.
    let a = script
        .find("fl_temperature_readings_ai_available AS")
        .unwrap();
    let r = script
        .find("fl_temperature_readings_ai_selected AS")
        .unwrap();
    let w = script
        .find("fl_feature_device_hot_reading_count_24h AS")
        .unwrap();
    assert!(a < r && r < w);
    let (code, _) = run(&[
        "generate",
        &project("04_fitted_and_custom_contracts.yaml"),
        "--deployment",
        "contracts_prod",
        "--out",
        out.join("x").to_str().unwrap(),
    ]);
    assert_eq!(code, 1);
    assert!(
        !out.join("x").exists(),
        "nothing is written when planning fails"
    );
}

#[test]
fn mcp_server_is_strict_and_read_only() {
    let mut child = Command::new(bin())
        .args(["mcp", "serve"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut ask = |msg: Value| -> Value {
        writeln!(stdin, "{msg}").unwrap();
        stdin.flush().unwrap();
        let mut line = String::new();
        stdout.read_line(&mut line).unwrap();
        serde_json::from_str(&line).unwrap()
    };
    let init = ask(
        json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "test", "version": "0"}}}),
    );
    assert_eq!(
        init["result"]["serverInfo"]["name"],
        json!("featlas-fe-spec")
    );
    let tools = ask(json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}));
    let names: Vec<&str> = tools["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"validate_v1") && names.contains(&"generate_v1"));
    assert!(tools["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .all(|t| t["inputSchema"]["additionalProperties"] == json!(false)
            && t["annotations"]["readOnlyHint"] == json!(true)));
    let doc = std::fs::read_to_string(example("01_fault_count.yaml")).unwrap();
    let r = ask(
        json!({"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "run_fixtures_v1", "arguments": {"document": doc, "format": "yaml"}}}),
    );
    assert_eq!(
        r["result"]["structuredContent"]["summary"]["passed"],
        json!(13)
    );
    assert_eq!(
        r["result"]["structuredContent"]["schema"],
        json!("featlas.mcp/v1")
    );
    // Paths are not accepted: unknown arguments fail schema validation.
    let r = ask(
        json!({"jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": {"name": "validate_v1", "arguments": {"document": "", "format": "yaml", "path": "/etc/passwd"}}}),
    );
    assert_eq!(r["error"]["code"], json!(-32602));
    let r = ask(
        json!({"jsonrpc": "2.0", "id": 5, "method": "tools/call", "params": {"name": "validate_v1", "arguments": {"document": "a: [", "format": "yaml"}}}),
    );
    assert_eq!(r["result"]["isError"], json!(true));
    // Transport parity: same diagnostics code as the CLI would emit.
    assert_eq!(
        r["result"]["structuredContent"]["report"]["diagnostics"][0]["code"],
        json!("PARSE_SYNTAX")
    );
    drop(stdin);
    child.wait().unwrap();
}

/// Text output is rendered by rs-rich: piped output carries no ANSI escapes, one-line
/// summaries stay on one line, and project values are printed literally (markup escaped).
#[test]
fn text_output_is_plain_when_piped() {
    let (code, out) = run(&["validate", &example("01_fault_count.yaml")]);
    assert_eq!(code, 0, "{out}");
    assert!(
        !out.contains('\u{1b}'),
        "no ANSI escapes when piped: {out:?}"
    );
    let last = out.lines().last().unwrap();
    assert!(
        last.starts_with("PASS: ") && last.ends_with("not evaluated by validate"),
        "{out}"
    );
    let (code, out) = run(&["test", &example("01_fault_count.yaml")]);
    assert_eq!(code, 0, "{out}");
    assert!(!out.contains('\u{1b}'));
    assert!(
        out.contains("PASS")
            && out
                .lines()
                .last()
                .unwrap()
                .starts_with("executed=13 passed=13 failed=0"),
        "{out}"
    );
    let (code, out) = run(&[
        "plan",
        &project("01_fault_count.yaml"),
        "--deployment",
        "fault_prod",
        "--format",
        "text",
    ]);
    assert_eq!(code, 0, "{out}");
    assert!(
        out.contains("Capability checks: fault_prod")
            && out.ends_with("plannable=true execution_ready=false\n"),
        "{out}"
    );
}
