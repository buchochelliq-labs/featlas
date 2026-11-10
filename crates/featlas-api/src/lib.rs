//! Transport-neutral JSON API over the featlas core and adapters.
//!
//! The CLI, the MCP server and the Python bindings all call these functions,
//! so every transport returns the same structures and diagnostics. Inputs are
//! exact document bytes (hashed as given) plus JSON values; nothing here reads
//! files named inside documents, fetches URIs, deploys or publishes.

use featlas_adapters::{databricks, LocalStores, Registry};
use featlas_core::canon::{deployment_identity, logical_identities};
use featlas_core::capability::{
    capability_report, check_report as check_report_core, plan_deployment, AssessmentContext,
};
use featlas_core::consumer::{
    serve, ConsumerRequest, PhysicalReadRequest, PublicationStore, StoreCommit, StoreRegistry,
};
use featlas_core::diag::EvalError;
use featlas_core::eval::{coverage_state, Limits};
use featlas_core::fixtures::{run_case, run_suites};
use featlas_core::load::{load_project, sha256_hex, LoadedProject};
use featlas_core::model::*;
use featlas_core::parse::Format;
use featlas_core::time::Instant;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

pub const API_SCHEMA: &str = "featlas.api/v1";
pub const MAX_REQUEST_ROWS: usize = 100_000;

/// A failure with a stable code; `detail` carries diagnostics or the core error.
#[derive(Debug, Clone)]
pub struct ApiError {
    pub code: String,
    pub message: String,
    pub detail: Value,
}

impl ApiError {
    fn new(code: &str, message: impl Into<String>, detail: Value) -> Self {
        ApiError {
            code: code.into(),
            message: message.into(),
            detail,
        }
    }
    pub fn to_json(&self) -> Value {
        json!({"code": self.code, "message": self.message, "detail": self.detail})
    }
}

impl From<EvalError> for ApiError {
    fn from(e: EvalError) -> Self {
        ApiError::new(&e.code, e.message.clone(), json!({"rule": e.rule}))
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

pub type ApiResult = Result<Value, ApiError>;

pub fn parse_format(format: &str) -> Result<Format, ApiError> {
    match format {
        "yaml" => Ok(Format::Yaml),
        "json" => Ok(Format::Json),
        other => Err(ApiError::new(
            "FORMAT_UNSUPPORTED",
            format!("format must be yaml or json, got {other:?}"),
            Value::Null,
        )),
    }
}

pub fn limits(seconds: u64) -> Limits {
    Limits {
        deadline: Some(std::time::Instant::now() + std::time::Duration::from_secs(seconds)),
        ..Default::default()
    }
}

pub fn now_utc() -> String {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    Instant {
        micros: (d.as_secs() as i64) * 1_000_000,
    }
    .to_string()
}

/// Load through the reference/type stage or fail with the staged report.
pub fn load(bytes: &[u8], format: Format) -> Result<LoadedProject, ApiError> {
    let (loaded, report) = load_project(bytes, format);
    match loaded {
        Some(l) if report.passed() => Ok(l),
        _ => Err(ApiError::new(
            "DOCUMENT_INVALID",
            "The project did not pass parse, schema and reference validation",
            serde_json::to_value(&report).unwrap_or(Value::Null),
        )),
    }
}

fn envelope(op: &str, mut body: Value) -> Value {
    body["schema"] = json!(API_SCHEMA);
    body["operation"] = json!(op);
    body
}

pub fn validate(bytes: &[u8], format: Format) -> Value {
    let (loaded, report) = load_project(bytes, format);
    let passed = loaded.is_some() && report.passed();
    let ids = loaded
        .as_ref()
        .filter(|_| passed)
        .map(|l| logical_identities(&l.project));
    envelope(
        "validate",
        json!({"passed": passed, "report": report, "logical_identities": ids, "levels_not_evaluated": ["planning_and_capabilities", "data_and_fixture", "runtime_and_acceptance"]}),
    )
}

pub fn hash(bytes: &[u8], format: Format) -> ApiResult {
    let l = load(bytes, format)?;
    let deployments: Map<String, Value> = l
        .project
        .deployments
        .iter()
        .map(|d| {
            (
                format!("{}@{}", d.id, d.version),
                json!(deployment_identity(&l.project, &d.id)),
            )
        })
        .collect();
    Ok(envelope(
        "hash",
        json!({"project_file_sha256": l.file_sha256, "logical": logical_identities(&l.project), "deployments": deployments}),
    ))
}

pub fn explain(bytes: &[u8], format: Format, feature: &str) -> ApiResult {
    let l = load(bytes, format)?;
    let f = l.project.feature(feature).ok_or_else(|| {
        ApiError::new(
            "UNKNOWN_FEATURE",
            format!("Unknown feature {feature}"),
            Value::Null,
        )
    })?;
    let fp = logical_identities(&l.project)
        .features
        .get(&format!("{}@{}", f.id, f.version))
        .cloned();
    Ok(envelope(
        "explain",
        json!({"feature": f, "fingerprint": fp, "sources": featlas_core::validate::feature_sources(&l.project, feature)}),
    ))
}

pub fn plan(bytes: &[u8], format: Format, deployment: &str) -> ApiResult {
    let l = load(bytes, format)?;
    let outcome = plan_deployment(
        &l.project,
        &l.file_sha256,
        deployment,
        &Registry::default(),
        &AssessmentContext::default(),
    );
    Ok(envelope("plan", serde_json::to_value(&outcome).unwrap()))
}

pub fn capabilities(
    bytes: &[u8],
    format: Format,
    deployment: &str,
    assessed_at: Option<&str>,
    local_evidence: Vec<ReportEvidence>,
) -> ApiResult {
    let l = load(bytes, format)?;
    let reg = Registry::default();
    let ctx = AssessmentContext {
        local_evidence,
        ..Default::default()
    };
    let outcome = plan_deployment(&l.project, &l.file_sha256, deployment, &reg, &ctx);
    if outcome.plan.is_none() {
        return Err(ApiError::new(
            "PLANNING_FAILED",
            "No plan could be built for this deployment",
            serde_json::to_value(&outcome.diagnostics).unwrap(),
        ));
    }
    let report = capability_report(
        &l.project,
        &l.file_sha256,
        deployment,
        &reg,
        &outcome,
        assessed_at.unwrap_or(&now_utc()),
    );
    let value = serde_json::to_value(&report).unwrap();
    let consistency = check_report_core(&value, &l.project, &l.file_sha256);
    if !consistency.is_empty() {
        return Err(ApiError::new(
            "REPORT_INCONSISTENT",
            "Generated report failed its own consistency check",
            serde_json::to_value(&consistency).unwrap(),
        ));
    }
    Ok(value)
}

/// Run local suites and return digest-pinned evidence plus the evidence body to persist.
pub fn local_evidence(
    bytes: &[u8],
    format: Format,
    uri: &str,
) -> Result<(Vec<ReportEvidence>, String), ApiError> {
    let l = load(bytes, format)?;
    let summary = run_suites(&l.project, None, &limits(60), false);
    if summary.failed > 0 {
        return Err(ApiError::new(
            "LOCAL_ACCEPTANCE_FAILED",
            format!(
                "{} local acceptance cases failed; no support evidence is produced",
                summary.failed
            ),
            Value::Null,
        ));
    }
    let body = serde_json::to_string_pretty(&json!({"project_file_sha256": l.file_sha256, "core": featlas_core::plan::CORE_VERSION, "summary": {"executed": summary.executed, "passed": summary.passed, "not_executed": summary.not_executed}, "cases": summary.cases})).unwrap();
    let ev = ReportEvidence {
        uri: uri.into(),
        sha256: sha256_hex(body.as_bytes()),
        description: format!(
            "Local reference acceptance run: {} passed, {} not executed",
            summary.passed, summary.not_executed
        ),
    };
    Ok((vec![ev], body))
}

pub fn check_report(report: &Value, bytes: &[u8], format: Format) -> ApiResult {
    let l = load(bytes, format)?;
    let diags = check_report_core(report, &l.project, &l.file_sha256);
    Ok(envelope(
        "check_report",
        json!({"consistent": diags.is_empty(), "diagnostics": diags, "scope": "Record consistency only; evidence is not fetched or authenticated and readiness is not certified."}),
    ))
}

pub fn run_fixtures(bytes: &[u8], format: Format, suite: Option<&str>) -> ApiResult {
    let l = load(bytes, format)?;
    if let Some(s) = suite {
        if l.project.suite(s).is_none() {
            return Err(ApiError::new(
                "UNKNOWN_SUITE",
                format!("Unknown suite {s}"),
                Value::Null,
            ));
        }
    }
    let summary = run_suites(&l.project, suite, &limits(60), false);
    Ok(envelope(
        "run_fixtures",
        json!({"project_file_sha256": l.file_sha256, "summary": summary}),
    ))
}

pub fn evaluate_case(bytes: &[u8], format: Format, case_id: &str) -> ApiResult {
    let l = load(bytes, format)?;
    let found = l
        .project
        .acceptance_suites
        .iter()
        .find_map(|s| s.cases.iter().find(|c| c.id == case_id).map(|c| (s, c)));
    let (suite, case) = found.ok_or_else(|| {
        ApiError::new(
            "UNKNOWN_CASE",
            format!("Unknown case {case_id}"),
            Value::Null,
        )
    })?;
    Ok(envelope(
        "evaluate_case",
        json!({"result": run_case(&l.project, suite, case, &limits(60), true)}),
    ))
}

pub fn coverage(
    bytes: &[u8],
    format: Format,
    deployment: &str,
    feature_set: &str,
    as_of: &str,
    statuses: &Value,
) -> ApiResult {
    let l = load(bytes, format)?;
    let p = &l.project;
    let as_of =
        Instant::parse(as_of).map_err(|m| ApiError::new("INVALID_TIMESTAMP", m, Value::Null))?;
    let statuses: Vec<SourceStatus> = serde_json::from_value(statuses.clone()).map_err(|e| {
        ApiError::new(
            "STATUS_INVALID",
            format!("source-status records: {e}"),
            Value::Null,
        )
    })?;
    let plan = featlas_core::plan::build_plan(p, &l.file_sha256, deployment)
        .ok_or_else(|| ApiError::new("UNKNOWN_DEPLOYMENT", deployment.to_string(), Value::Null))?;
    let fs = plan
        .logical
        .feature_sets
        .iter()
        .find(|x| x.id == feature_set)
        .ok_or_else(|| {
            ApiError::new(
                "UNKNOWN_FEATURE_SET",
                "Feature set is not targeted by the deployment",
                Value::Null,
            )
        })?;
    let mut out = Map::new();
    for f in &fs.features {
        let model = p
            .feature(&f.id)
            .ok_or_else(|| ApiError::new("UNKNOWN_FEATURE", f.id.clone(), Value::Null))?;
        let st = coverage_state(p, model, as_of, &statuses)?;
        let policy = match st {
            Some("source_unavailable") => Some(model.missingness.source_unavailable),
            Some(_) => Some(model.missingness.insufficient_history),
            None => None,
        };
        out.insert(f.id.clone(), json!({"coverage_state": st.unwrap_or("established"), "policy": policy, "variant": if st.is_some() { "degraded" } else { "computed" }}));
    }
    Ok(envelope(
        "coverage",
        json!({"as_of": as_of.to_string(), "features": out}),
    ))
}

/// Generate plan, report and adapter code in memory; `files` maps relative paths to contents.
pub fn generate(
    bytes: &[u8],
    format: Format,
    deployment: &str,
    generated_at: Option<&str>,
) -> ApiResult {
    let l = load(bytes, format)?;
    let reg = Registry::default();
    let outcome = plan_deployment(
        &l.project,
        &l.file_sha256,
        deployment,
        &reg,
        &AssessmentContext::default(),
    );
    if !outcome.plannable {
        return Err(ApiError::new(
            "PLANNING_FAILED",
            "Planning failed: unsupported or unresolved requirements block code generation",
            serde_json::to_value(&outcome.diagnostics).unwrap(),
        ));
    }
    let plan = outcome.plan.as_ref().unwrap();
    let report = capability_report(
        &l.project,
        &l.file_sha256,
        deployment,
        &reg,
        &outcome,
        generated_at.unwrap_or(&now_utc()),
    );
    let mut files: BTreeMap<String, String> = BTreeMap::new();
    files.insert(
        "plan.json".into(),
        serde_json::to_string_pretty(plan).unwrap(),
    );
    files.insert(
        "capability-report.json".into(),
        serde_json::to_string_pretty(&report).unwrap(),
    );
    files.insert(
        "planning-diagnostics.json".into(),
        serde_json::to_string_pretty(&outcome.diagnostics).unwrap(),
    );
    let d = l.project.deployment(deployment).unwrap();
    let compute = d.adapter(&d.execution.compute).unwrap();
    let mut artifacts = Map::new();
    let mut label_artifacts = Map::new();
    if compute.implementation == databricks::IMPLEMENTATION {
        for fs in &plan.logical.feature_sets {
            let art = databricks::lower(plan, &fs.id, &l.project)
                .map_err(|m| ApiError::new("LOWERING_FAILED", m, Value::Null))?;
            let dir = format!("databricks/{}", fs.id.replace(['/', '\\'], "_"));
            files.insert(
                format!("{dir}/artifacts.json"),
                serde_json::to_string_pretty(&art).unwrap(),
            );
            files.insert(format!("{dir}/script.sql"), databricks::render_script(&art));
            artifacts.insert(fs.id.clone(), serde_json::to_value(&art).unwrap());
        }
        // Scheduled deployments: a paused Databricks job running every feature set (SEM-DEPLOY-003).
        if d.execution.orchestrator.is_some() && matches!(d.schedule, featlas_core::model::Schedule::Interval { .. }) {
            let job = databricks::job_bundle(plan, d, &l.project.metadata.id)
                .map_err(|m| ApiError::new("LOWERING_FAILED", m, Value::Null))?;
            files.insert("databricks/job.yml".into(), serde_json::to_string_pretty(&job).unwrap());
        }
        // Label datasets for label consumers of the planned feature sets (SEM-LABEL-001).
        for c in &l.project.consumers {
            let Some(label) = &c.label else { continue };
            let in_plan = plan.logical.feature_sets.iter().any(|f| f.id == c.feature_set.feature_set);
            if !in_plan || !plan.logical.sources.contains_key(&label.source) {
                continue;
            }
            let art = databricks::lower_labels(plan, &l.project, &c.id)
                .map_err(|m| ApiError::new("LOWERING_FAILED", m, Value::Null))?;
            files.insert(
                format!("databricks/labels/{}/artifacts.json", c.id.replace(['/', '\\'], "_")),
                serde_json::to_string_pretty(&art).unwrap(),
            );
            label_artifacts.insert(c.id.clone(), serde_json::to_value(&art).unwrap());
        }
    }
    let digests: Vec<Value> = files
        .iter()
        .map(|(p, c)| json!({"path": p, "sha256": sha256_hex(c.as_bytes()), "bytes": c.len()}))
        .collect();
    let manifest = json!({
        "schema": "featlas.generation-manifest/v1",
        "generator": {"core": featlas_core::plan::CORE_VERSION, "api": env!("CARGO_PKG_VERSION")},
        "project_file_sha256": l.file_sha256,
        "deployment": {"id": d.id, "version": d.version, "identity": plan.bindings.deployment_identity},
        "plan_digest": plan.plan_digest,
        "logical_plan_digest": plan.logical_plan_digest,
        "compute_adapter": {"implementation": compute.implementation, "version": compute.version},
        "execution_ready": outcome.execution_ready,
        "files": digests,
        "statement": "Generated code has not been executed by this operation. No resource was created and nothing was published.",
    });
    Ok(envelope(
        "generate",
        json!({"plan": plan, "capability_report": report, "diagnostics": outcome.diagnostics, "artifacts": artifacts, "label_artifacts": label_artifacts, "files": files, "manifest": manifest}),
    ))
}

/// Physical records already read by an external host for the bound adapter.
struct HostRecords {
    rows: Vec<Map<String, Value>>,
}

impl PublicationStore for HostRecords {
    fn read(
        &self,
        _: &AdapterInstance,
        req: &PhysicalReadRequest,
    ) -> Result<Vec<Map<String, Value>>, EvalError> {
        Ok(self
            .rows
            .iter()
            .filter(|r| {
                req.keys.iter().any(|k| {
                    req.key_columns
                        .iter()
                        .zip(k)
                        .all(|(c, v)| r.get(c) == Some(v))
                })
            })
            .cloned()
            .collect())
    }
    fn write(
        &self,
        _: &AdapterInstance,
        _: &Location,
        _: Vec<Map<String, Value>>,
    ) -> Result<StoreCommit, EvalError> {
        Err(EvalError::new(
            "READ_ONLY",
            "SEM-CONF-002",
            "Host-supplied records are read-only",
        ))
    }
}

impl StoreRegistry for HostRecords {
    fn store(&self, _: &str, _: &str) -> Option<&dyn PublicationStore> {
        Some(self)
    }
}

/// Serve a target-neutral consumer request. With `records`, the host has read the bound
/// target's physical rows; otherwise local stores are used.
pub fn consume(
    bytes: &[u8],
    format: Format,
    request: &Value,
    records: Option<&Value>,
) -> ApiResult {
    let l = load(bytes, format)?;
    let req: ConsumerRequest = serde_json::from_value(request.clone()).map_err(|e| {
        ApiError::new(
            "REQUEST_INVALID",
            format!("consumer request: {e}"),
            Value::Null,
        )
    })?;
    let resp = match records {
        Some(r) => {
            let rows: Vec<Map<String, Value>> = serde_json::from_value(r.clone())
                .map_err(|e| ApiError::new("RECORDS_INVALID", e.to_string(), Value::Null))?;
            serve(&l.project, &HostRecords { rows }, &req, MAX_REQUEST_ROWS)?
        }
        None => serve(
            &l.project,
            &LocalStores::new(now_utc),
            &req,
            MAX_REQUEST_ROWS,
        )?,
    };
    Ok(envelope("consume", json!({"response": resp})))
}

pub fn migration_plan(bytes: &[u8], format: Format, from: &str, to: &str) -> ApiResult {
    let l = load(bytes, format)?;
    let plan = featlas_core::migration::migration_plan(
        &l.project,
        &l.file_sha256,
        from,
        to,
        &Registry::default(),
    )
    .map_err(|m| ApiError::new("MIGRATION_PLAN_FAILED", m, Value::Null))?;
    Ok(serde_json::to_value(&plan).unwrap())
}

pub fn labels(bytes: &[u8], format: Format, consumer: &str, input: &Value) -> ApiResult {
    let l = load(bytes, format)?;
    let li: featlas_core::label::LabelInput = serde_json::from_value(input.clone())
        .map_err(|e| ApiError::new("LABEL_INPUT_INVALID", e.to_string(), Value::Null))?;
    let ds = featlas_core::label::build_labels(&l.project, consumer, &li, &limits(60))?;
    Ok(envelope("labels", json!({"dataset": ds})))
}

/// The data-free part of label construction: eligible and excluded requests.
/// `input` is `{"rows": [...], "source_status": {...}}`.
pub fn label_requests(bytes: &[u8], format: Format, consumer: &str, input: &Value) -> ApiResult {
    let l = load(bytes, format)?;
    let rows: Vec<serde_json::Map<String, Value>> = serde_json::from_value(input["rows"].clone())
        .map_err(|e| ApiError::new("LABEL_INPUT_INVALID", e.to_string(), Value::Null))?;
    let status: featlas_core::model::SourceStatus = serde_json::from_value(input["source_status"].clone())
        .map_err(|e| ApiError::new("LABEL_INPUT_INVALID", e.to_string(), Value::Null))?;
    let r = featlas_core::label::label_requests(&l.project, consumer, &rows, &status)?;
    Ok(envelope("label_requests", json!({"requests": r})))
}

/// Parse an untrusted JSON value under the restricted profile (duplicate keys rejected).
pub fn parse_json(text: &str) -> Result<Value, ApiError> {
    let wrapped = format!("{{\"v\": {text}}}");
    featlas_core::parse::parse_bytes(wrapped.as_bytes(), Format::Json)
        .map(|d| d.value["v"].clone())
        .map_err(|d| {
            ApiError::new(
                "JSON_INVALID",
                "Input is not valid restricted JSON",
                serde_json::to_value(&d).unwrap(),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn example(name: &str) -> Vec<u8> {
        std::fs::read(
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../spec/feature-engineering-spec-v0.2.0/examples")
                .join(name),
        )
        .unwrap()
    }

    #[test]
    fn api_round_trip() {
        let b = example("01_fault_count.yaml");
        assert_eq!(validate(&b, Format::Yaml)["passed"], json!(true));
        assert_eq!(
            run_fixtures(&b, Format::Yaml, None).unwrap()["summary"]["passed"],
            json!(13)
        );
        assert_eq!(
            generate(&b, Format::Yaml, "fault_prod", None)
                .unwrap_err()
                .code,
            "PLANNING_FAILED"
        );
        let bad = load(b"a: [", Format::Yaml).unwrap_err();
        assert_eq!(bad.code, "DOCUMENT_INVALID");
        assert_eq!(
            parse_json(r#"{"a":1,"a":2}"#).unwrap_err().code,
            "JSON_INVALID"
        );
    }
}
