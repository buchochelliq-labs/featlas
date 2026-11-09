use crate::OutFormat;
use featlas_adapters::{databricks, LocalStores, Registry};
use featlas_core::canon::{deployment_identity, logical_identities};
use featlas_core::capability::{
    capability_report, check_report as check_report_core, plan_deployment, AssessmentContext,
};
use featlas_core::consumer::{
    serve, ConsumerRequest, PhysicalReadRequest, PublicationStore, StoreCommit, StoreRegistry,
};
use featlas_core::diag::{Diagnostic, EvalError};
use featlas_core::eval::{coverage_state, Limits};
use featlas_core::fixtures::{run_suites, CaseStatus};
use featlas_core::load::{load_path, sha256_hex, LoadReport, LoadedProject};
use featlas_core::model::*;
use featlas_core::time::Instant;
use serde_json::{json, Map, Value};
use std::path::Path;
use std::process::ExitCode;

pub const CLI_SCHEMA: &str = "featlas.cli/v1";

pub fn now_utc() -> String {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    Instant {
        micros: (d.as_secs() as i64) * 1_000_000,
    }
    .to_string()
}

pub fn limits() -> Limits {
    Limits {
        deadline: Some(std::time::Instant::now() + std::time::Duration::from_secs(60)),
        ..Default::default()
    }
}

fn print_json(v: &Value) {
    println!("{}", serde_json::to_string_pretty(v).unwrap());
}

fn print_diags(diags: &[Diagnostic]) {
    for d in diags {
        println!("{:7} {d}", format!("{:?}", d.severity).to_lowercase());
    }
}

/// Load through the reference stage; on failure print the report and return the exit code.
fn load(
    path: &Path,
    format: OutFormat,
) -> Result<Result<(LoadedProject, LoadReport), ExitCode>, String> {
    let (loaded, report) = load_path(path)?;
    match loaded {
        Some(l) if report.passed() => Ok(Ok((l, report))),
        _ => {
            if format == OutFormat::Json {
                print_json(
                    &json!({"schema": CLI_SCHEMA, "command": "load", "passed": false, "report": report}),
                );
            } else {
                print_diags(&report.diagnostics);
                println!("FAILED: {} ({})", path.display(), stage_summary(&report));
            }
            Ok(Err(ExitCode::from(1)))
        }
    }
}

fn stage_summary(r: &LoadReport) -> String {
    r.stages
        .iter()
        .map(|s| {
            format!(
                "{}={}",
                s.stage.as_str(),
                serde_json::to_value(s.status)
                    .unwrap()
                    .as_str()
                    .unwrap_or("")
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

macro_rules! loaded {
    ($path:expr, $fmt:expr) => {
        match load($path, $fmt)? {
            Ok(x) => x,
            Err(code) => return Ok(code),
        }
    };
}

pub fn validate(path: &Path, format: OutFormat) -> Result<ExitCode, String> {
    let (loaded, report) = load_path(path)?;
    let passed = loaded.is_some() && report.passed();
    let identities = loaded
        .as_ref()
        .filter(|_| passed)
        .map(|l| logical_identities(&l.project));
    if format == OutFormat::Json {
        print_json(
            &json!({"schema": CLI_SCHEMA, "command": "validate", "passed": passed, "report": report, "logical_identities": identities, "levels_not_evaluated": ["planning_and_capabilities", "data_and_fixture", "runtime_and_acceptance"]}),
        );
    } else {
        print_diags(&report.diagnostics);
        println!(
            "{}: {} ({}); planning, data and runtime levels not evaluated by validate",
            if passed { "PASS" } else { "FAILED" },
            path.display(),
            stage_summary(&report)
        );
    }
    Ok(if passed {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    })
}

pub fn hash(path: &Path) -> Result<ExitCode, String> {
    let (l, _) = loaded!(path, OutFormat::Json);
    let ids = logical_identities(&l.project);
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
    print_json(
        &json!({"schema": CLI_SCHEMA, "command": "hash", "project_file_sha256": l.file_sha256, "logical": ids, "deployments": deployments,
        "note": "Logical fingerprints exclude documentation and all deployment bindings; the project-file digest and deployment identities are binding-sensitive."}),
    );
    Ok(ExitCode::SUCCESS)
}

pub fn explain(path: &Path, feature: &str, format: OutFormat) -> Result<ExitCode, String> {
    let (l, _) = loaded!(path, format);
    let p = &l.project;
    let Some(f) = p.feature(feature) else {
        eprintln!(
            "Unknown feature {feature}; known: {:?}",
            p.features.iter().map(|f| &f.id).collect::<Vec<_>>()
        );
        return Ok(ExitCode::from(1));
    };
    let ids = logical_identities(p);
    let w = &f.time.window;
    let lb = |b: Boundary, i: &str, e: &str| {
        if b == Boundary::Inclusive {
            i.to_string()
        } else {
            e.to_string()
        }
    };
    let mut lines = vec![
        format!("{}@{} — {}", f.id, f.version, f.name),
        format!("  fingerprint: {}", ids.features.get(&format!("{}@{}", f.id, f.version)).cloned().unwrap_or_default()),
        format!("  meaning: {}", f.description),
        format!("  entity: {} (keys: {})", f.entity, p.entity(&f.entity).map(|e| e.keys.iter().map(|k| format!("{}{}", k.name, if k.is_tenant { " [tenant]" } else { "" })).collect::<Vec<_>>().join(", ")).unwrap_or_default()),
        format!("  output: {}{} unit={:?}", f.output.ty.as_str(), if f.output.nullable { "?" } else { "" }, f.output.unit),
        format!("  transform: {}", f.transform.kind()),
        format!("  event window: {}as_of − {}s, as_of{} (publication delay {}s never shifts it)", lb(w.lower, "[", "("), w.lookback_seconds, lb(w.upper, "]", ")"), f.time.publication.delay_seconds),
        format!("  availability: available_time {} as_of", lb(f.time.availability.upper, "≤", "<")),
        format!("  history: {}s contiguous, earliest_as_of {}", f.history.required_seconds, f.history.earliest_as_of),
        format!("  missingness: empty={:?} null_input={:?} insufficient_history={:?} source_unavailable={:?}", f.missingness.empty, f.missingness.null_input, f.missingness.insufficient_history, f.missingness.source_unavailable),
        "  stage order: validate rows → availability cutoff → PK duplicates → logical-key revision selection → tie rejection → tombstones → population → window → joins → predicate → grouping → transform → coverage/missingness/quality/output".into(),
    ];
    for i in &f.inputs {
        lines.push(format!(
            "  input {} = {} columns {:?}",
            i.alias, i.source, i.columns
        ));
    }
    for d in &f.dependencies {
        lines.push(format!(
            "  dependency {}@{} ({})",
            d.feature, d.version, d.alignment
        ));
    }
    for e in &f.lineage.sources {
        lines.push(format!(
            "  lineage {}.{} roles {:?}",
            e.source, e.column, e.roles
        ));
    }
    if format == OutFormat::Json {
        print_json(
            &json!({"schema": CLI_SCHEMA, "command": "explain", "feature": f, "fingerprint": ids.features.get(&format!("{}@{}", f.id, f.version)), "explanation": lines}),
        );
    } else {
        println!("{}", lines.join("\n"));
    }
    Ok(ExitCode::SUCCESS)
}

pub fn plan(path: &Path, deployment: &str, format: OutFormat) -> Result<ExitCode, String> {
    let (l, _) = loaded!(path, format);
    let reg = Registry::default();
    let outcome = plan_deployment(
        &l.project,
        &l.file_sha256,
        deployment,
        &reg,
        &AssessmentContext::default(),
    );
    if format == OutFormat::Json {
        print_json(
            &json!({"schema": CLI_SCHEMA, "command": "plan", "plannable": outcome.plannable, "execution_ready": outcome.execution_ready, "diagnostics": outcome.diagnostics, "checks": outcome.checks, "plan": outcome.plan}),
        );
    } else {
        print_diags(&outcome.diagnostics);
        for c in &outcome.checks {
            println!(
                "  {:<22} {:<14} {:<32} {:<18} {}",
                c.adapter,
                c.role.as_str(),
                c.capability,
                c.status.as_str(),
                c.evidence_scope.as_str()
            );
        }
        println!(
            "plannable={} execution_ready={}",
            outcome.plannable, outcome.execution_ready
        );
    }
    Ok(if outcome.plannable {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    })
}

/// Run local suites and write digest-pinned evidence for local adapters.
fn local_evidence(l: &LoadedProject, dir: &Path) -> Result<Vec<ReportEvidence>, String> {
    let summary = run_suites(&l.project, None, &limits(), false);
    if summary.failed > 0 {
        return Err(format!(
            "{} local acceptance cases failed; no support evidence is produced",
            summary.failed
        ));
    }
    std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    let body = serde_json::to_string_pretty(&json!({"project_file_sha256": l.file_sha256, "core": featlas_core::plan::CORE_VERSION, "summary": {"executed": summary.executed, "passed": summary.passed, "not_executed": summary.not_executed}, "cases": summary.cases})).unwrap();
    let file = dir.join("local-acceptance.json");
    std::fs::write(&file, &body).map_err(|e| e.to_string())?;
    Ok(vec![ReportEvidence {
        uri: format!("file:{}", file.display()),
        sha256: sha256_hex(body.as_bytes()),
        description: format!(
            "Local reference acceptance run: {} passed, {} not executed",
            summary.passed, summary.not_executed
        ),
    }])
}

pub fn capabilities(
    path: &Path,
    deployment: &str,
    evidence_dir: Option<&Path>,
    assessed_at: Option<String>,
) -> Result<ExitCode, String> {
    let (l, _) = loaded!(path, OutFormat::Json);
    let reg = Registry::default();
    let mut ctx = AssessmentContext::default();
    if let Some(dir) = evidence_dir {
        ctx.local_evidence = local_evidence(&l, dir)?;
    }
    let outcome = plan_deployment(&l.project, &l.file_sha256, deployment, &reg, &ctx);
    if outcome.plan.is_none() {
        print_diags(&outcome.diagnostics);
        return Ok(ExitCode::from(1));
    }
    let report = capability_report(
        &l.project,
        &l.file_sha256,
        deployment,
        &reg,
        &outcome,
        &assessed_at.unwrap_or_else(now_utc),
    );
    let value = serde_json::to_value(&report).unwrap();
    let consistency = check_report_core(&value, &l.project, &l.file_sha256);
    if !consistency.is_empty() {
        eprintln!("Generated report failed its own consistency check:");
        for d in &consistency {
            eprintln!("  {d}");
        }
        return Ok(ExitCode::from(1));
    }
    print_json(&value);
    Ok(ExitCode::SUCCESS)
}

pub fn check_report(report: &Path, project: &Path) -> Result<ExitCode, String> {
    let (l, _) = loaded!(project, OutFormat::Json);
    let bytes = std::fs::read(report).map_err(|e| e.to_string())?;
    let doc = featlas_core::parse::parse_bytes(&bytes, featlas_core::parse::Format::Json)
        .map_err(|d| format!("{:?}", d.iter().map(|x| x.to_string()).collect::<Vec<_>>()))?;
    let diags = check_report_core(&doc.value, &l.project, &l.file_sha256);
    print_json(
        &json!({"schema": CLI_SCHEMA, "command": "check-report", "consistent": diags.is_empty(), "diagnostics": diags,
        "scope": "Record consistency only; evidence is not fetched or authenticated and readiness is not certified."}),
    );
    Ok(if diags.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    })
}

pub fn test(path: &Path, suite: Option<&str>, format: OutFormat) -> Result<ExitCode, String> {
    let (l, _) = loaded!(path, format);
    if let Some(s) = suite {
        if l.project.suite(s).is_none() {
            eprintln!("Unknown suite {s}");
            return Ok(ExitCode::from(1));
        }
    }
    let summary = run_suites(&l.project, suite, &limits(), false);
    if format == OutFormat::Json {
        print_json(
            &json!({"schema": CLI_SCHEMA, "command": "test", "project_file_sha256": l.file_sha256, "summary": summary}),
        );
    } else {
        for c in &summary.cases {
            let st = match c.status {
                CaseStatus::Passed => "PASS",
                CaseStatus::Failed => "FAIL",
                CaseStatus::NotExecuted => "NOT EXECUTED",
            };
            println!(
                "{st:<13} {}/{} ({}@{}) {}",
                c.suite, c.case, c.feature, c.feature_version, c.reason
            );
        }
        println!(
            "executed={} passed={} failed={} not_executed={} (not-executed cases are not passes)",
            summary.executed, summary.passed, summary.failed, summary.not_executed
        );
    }
    Ok(if summary.failed == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    })
}

pub fn evaluate(path: &Path, case_id: &str, format: OutFormat) -> Result<ExitCode, String> {
    let (l, _) = loaded!(path, format);
    let found = l
        .project
        .acceptance_suites
        .iter()
        .find_map(|s| s.cases.iter().find(|c| c.id == case_id).map(|c| (s, c)));
    let Some((suite, case)) = found else {
        eprintln!("Unknown case {case_id}");
        return Ok(ExitCode::from(1));
    };
    let r = featlas_core::fixtures::run_case(&l.project, suite, case, &limits(), true);
    if format == OutFormat::Json {
        print_json(&json!({"schema": CLI_SCHEMA, "command": "evaluate", "result": r}));
    } else {
        println!("{:?}: {}", r.status, r.reason);
        for t in &r.trace {
            println!("  [{}] {} {}", t.stage, t.feature, t.detail);
        }
        println!(
            "{}",
            serde_json::to_string_pretty(
                &json!({"rows": r.actual_rows, "diagnostic": r.actual_diagnostic})
            )
            .unwrap()
        );
    }
    Ok(if r.status == CaseStatus::Failed {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    })
}

pub fn coverage(
    path: &Path,
    deployment: &str,
    feature_set: &str,
    as_of: &str,
    status: &Path,
) -> Result<ExitCode, String> {
    let (l, _) = loaded!(path, OutFormat::Json);
    let p = &l.project;
    let as_of = Instant::parse(as_of)?;
    let text = std::fs::read(status).map_err(|e| e.to_string())?;
    let doc =
        featlas_core::parse::parse_bytes(&wrap_array(&text)?, featlas_core::parse::Format::Json)
            .map_err(|d| format!("{:?}", d.iter().map(|x| x.to_string()).collect::<Vec<_>>()))?;
    let statuses: Vec<SourceStatus> = serde_json::from_value(doc.value["items"].clone())
        .map_err(|e| format!("status records: {e}"))?;
    let plan = featlas_core::plan::build_plan(p, &l.file_sha256, deployment)
        .ok_or("Unknown deployment")?;
    let fs = plan
        .logical
        .feature_sets
        .iter()
        .find(|x| x.id == feature_set)
        .ok_or("Feature set is not targeted by the deployment")?;
    let mut out = Map::new();
    for f in &fs.features {
        let model = p.feature(&f.id).ok_or("unknown feature")?;
        let st = coverage_state(p, model, as_of, &statuses).map_err(|e| e.to_string())?;
        let policy = match st {
            Some("source_unavailable") => Some(model.missingness.source_unavailable),
            Some(_) => Some(model.missingness.insufficient_history),
            None => None,
        };
        out.insert(f.id.clone(), json!({"coverage_state": st.unwrap_or("established"), "policy": policy, "variant": if st.is_some() { "degraded" } else { "computed" }}));
    }
    print_json(
        &json!({"schema": CLI_SCHEMA, "command": "coverage", "as_of": as_of.to_string(), "features": out}),
    );
    Ok(ExitCode::SUCCESS)
}

fn wrap_array(bytes: &[u8]) -> Result<Vec<u8>, String> {
    let s = std::str::from_utf8(bytes).map_err(|e| e.to_string())?;
    Ok(format!("{{\"items\": {s}}}").into_bytes())
}

fn write_file(out: &Path, rel: &str, body: &str, files: &mut Vec<Value>) -> Result<(), String> {
    let path = out.join(rel);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    std::fs::write(&path, body).map_err(|e| format!("{}: {e}", path.display()))?;
    files.push(json!({"path": rel, "sha256": sha256_hex(body.as_bytes()), "bytes": body.len()}));
    Ok(())
}

pub fn generate(path: &Path, deployment: &str, out: &Path) -> Result<ExitCode, String> {
    let (l, _) = loaded!(path, OutFormat::Text);
    let reg = Registry::default();
    let outcome = plan_deployment(
        &l.project,
        &l.file_sha256,
        deployment,
        &reg,
        &AssessmentContext::default(),
    );
    if !outcome.plannable {
        print_diags(&outcome.diagnostics);
        println!("Not generated: planning failed (unsupported or unresolved requirements block code generation).");
        return Ok(ExitCode::from(1));
    }
    let plan = outcome.plan.as_ref().unwrap();
    let mut files = Vec::new();
    write_file(
        out,
        "plan.json",
        &serde_json::to_string_pretty(plan).unwrap(),
        &mut files,
    )?;
    let report = capability_report(
        &l.project,
        &l.file_sha256,
        deployment,
        &reg,
        &outcome,
        &now_utc(),
    );
    write_file(
        out,
        "capability-report.json",
        &serde_json::to_string_pretty(&report).unwrap(),
        &mut files,
    )?;
    write_file(
        out,
        "planning-diagnostics.json",
        &serde_json::to_string_pretty(&outcome.diagnostics).unwrap(),
        &mut files,
    )?;
    let d = l.project.deployment(deployment).unwrap();
    let compute = d.adapter(&d.execution.compute).unwrap();
    let mut generated = Vec::new();
    if compute.implementation == databricks::IMPLEMENTATION {
        for fs in &plan.logical.feature_sets {
            let art = databricks::lower(plan, &fs.id, &l.project)?;
            let dir = format!("databricks/{}", fs.id.replace(['/', '\\'], "_"));
            write_file(
                out,
                &format!("{dir}/artifacts.json"),
                &serde_json::to_string_pretty(&art).unwrap(),
                &mut files,
            )?;
            write_file(
                out,
                &format!("{dir}/script.sql"),
                &databricks::render_script(&art),
                &mut files,
            )?;
            generated.push(dir);
        }
    }
    let manifest = json!({
        "schema": "featlas.generation-manifest/v1",
        "generator": {"core": featlas_core::plan::CORE_VERSION, "cli": env!("CARGO_PKG_VERSION"), "executable_sha256": featlas_adapters::local::executable_sha256()},
        "project_file_sha256": l.file_sha256,
        "deployment": {"id": d.id, "version": d.version, "identity": plan.bindings.deployment_identity},
        "plan_digest": plan.plan_digest,
        "logical_plan_digest": plan.logical_plan_digest,
        "compute_adapter": {"implementation": compute.implementation, "version": compute.version},
        "execution_ready": outcome.execution_ready,
        "files": files,
        "statement": "Generated code has not been executed by this command. No resource was created and nothing was published.",
    });
    std::fs::write(
        out.join("manifest.json"),
        serde_json::to_string_pretty(&manifest).unwrap(),
    )
    .map_err(|e| e.to_string())?;
    println!(
        "Generated {} files under {} (code for: {:?}); execution_ready={}",
        files.len() + 1,
        out.display(),
        generated,
        outcome.execution_ready
    );
    Ok(ExitCode::SUCCESS)
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

pub fn consume(path: &Path, request: &Path, records: Option<&Path>) -> Result<ExitCode, String> {
    let (l, _) = loaded!(path, OutFormat::Json);
    let bytes = std::fs::read(request).map_err(|e| e.to_string())?;
    let doc = featlas_core::parse::parse_bytes(&bytes, featlas_core::parse::Format::Json)
        .map_err(|d| format!("{:?}", d.iter().map(|x| x.to_string()).collect::<Vec<_>>()))?;
    let req: ConsumerRequest =
        serde_json::from_value(doc.value).map_err(|e| format!("consumer request: {e}"))?;
    let result = match records {
        Some(r) => {
            let raw = std::fs::read(r).map_err(|e| e.to_string())?;
            let parsed = featlas_core::parse::parse_bytes(
                &wrap_array(&raw)?,
                featlas_core::parse::Format::Json,
            )
            .map_err(|d| format!("{:?}", d.iter().map(|x| x.to_string()).collect::<Vec<_>>()))?;
            let rows: Vec<Map<String, Value>> =
                serde_json::from_value(parsed.value["items"].clone()).map_err(|e| e.to_string())?;
            serve(&l.project, &HostRecords { rows }, &req, 100_000)
        }
        None => serve(&l.project, &LocalStores::new(now_utc), &req, 100_000),
    };
    match result {
        Ok(resp) => {
            print_json(&json!({"schema": CLI_SCHEMA, "command": "consume", "response": resp}));
            Ok(ExitCode::SUCCESS)
        }
        Err(e) => {
            print_json(
                &json!({"schema": CLI_SCHEMA, "command": "consume", "error": {"code": e.code, "rule": e.rule, "message": e.message}}),
            );
            Ok(ExitCode::from(1))
        }
    }
}

pub fn migration_plan(
    path: &Path,
    from: &str,
    to: &str,
    format: OutFormat,
) -> Result<ExitCode, String> {
    let (l, _) = loaded!(path, format);
    let plan = featlas_core::migration::migration_plan(
        &l.project,
        &l.file_sha256,
        from,
        to,
        &Registry::default(),
    )?;
    if format == OutFormat::Json {
        print_json(&serde_json::to_value(&plan).unwrap());
    } else {
        println!("Migration plan {from} -> {to}");
        for b in &plan.blockers {
            println!("  BLOCKER: {b}");
        }
        for r in &plan.semantic_review {
            println!("  REVIEW: {r}");
        }
        println!("{}", serde_json::to_string_pretty(&plan.mapping).unwrap());
    }
    Ok(ExitCode::SUCCESS)
}

pub fn label(path: &Path, consumer: &str, input: &Path) -> Result<ExitCode, String> {
    let (l, _) = loaded!(path, OutFormat::Json);
    let bytes = std::fs::read(input).map_err(|e| e.to_string())?;
    let doc = featlas_core::parse::parse_bytes(&bytes, featlas_core::parse::Format::Json)
        .map_err(|d| format!("{:?}", d.iter().map(|x| x.to_string()).collect::<Vec<_>>()))?;
    let li: featlas_core::label::LabelInput =
        serde_json::from_value(doc.value).map_err(|e| e.to_string())?;
    match featlas_core::label::build_labels(&l.project, consumer, &li, &limits()) {
        Ok(ds) => {
            print_json(&json!({"schema": CLI_SCHEMA, "command": "label", "dataset": ds}));
            Ok(ExitCode::SUCCESS)
        }
        Err(e) => {
            print_json(
                &json!({"schema": CLI_SCHEMA, "command": "label", "error": {"code": e.code, "rule": e.rule, "message": e.message}}),
            );
            Ok(ExitCode::from(1))
        }
    }
}
