use crate::render::{self, e, verdict, Grid};
use crate::OutFormat;
use featlas_adapters::Registry;
use featlas_core::canon::logical_identities;
use featlas_core::capability::{
    capability_report, check_report as check_report_core, plan_deployment, AssessmentContext,
};
use featlas_core::diag::Diagnostic;
use featlas_core::eval::Limits;
use featlas_core::fixtures::{run_suites, CaseStatus};
use featlas_core::load::{load_path, sha256_hex, LoadReport, LoadedProject};
use featlas_core::model::*;
use featlas_core::time::Instant;
use serde_json::{json, Value};
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
    render::diagnostics(&render::console(), diags);
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
                let c = render::console();
                render::diagnostics(&c, &report.diagnostics);
                render::line(
                    &c,
                    &format!(
                        "{}: {} ({})",
                        verdict(false),
                        e(&path.display().to_string()),
                        e(&stage_summary(&report))
                    ),
                );
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

fn fmt_of(path: &Path) -> featlas_core::parse::Format {
    featlas_core::parse::Format::from_path(&path.to_string_lossy())
}

fn read_json(path: &Path) -> Result<Value, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    featlas_api::parse_json(&text).map_err(|e| e.to_string())
}

/// Print an API result envelope; errors become `{"error": ...}` with exit code 1.
fn emit(command: &str, r: featlas_api::ApiResult) -> Result<ExitCode, String> {
    match r {
        Ok(mut v) => {
            v["command"] = json!(command);
            print_json(&v);
            Ok(ExitCode::SUCCESS)
        }
        Err(e) => {
            print_json(&json!({"schema": CLI_SCHEMA, "command": command, "error": e.to_json()}));
            Ok(ExitCode::from(1))
        }
    }
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
        let c = render::console();
        render::diagnostics(&c, &report.diagnostics);
        render::line(
            &c,
            &format!(
                "{}: {} ({}); [dim]planning, data and runtime levels not evaluated by validate[/]",
                verdict(passed),
                e(&path.display().to_string()),
                e(&stage_summary(&report))
            ),
        );
    }
    Ok(if passed {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    })
}

pub fn hash(path: &Path) -> Result<ExitCode, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
    emit("hash", featlas_api::hash(&bytes, fmt_of(path)))
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
        let c = render::console();
        let (head, rest) = lines.split_first().expect("explanation has a heading");
        render::line(&c, &format!("[bold cyan]{}[/]", e(head)));
        for l in rest {
            match l.trim_start().split_once(": ") {
                Some((k, v)) if !k.contains(' ') || k == "stage order" => {
                    render::line(&c, &format!("  [bold]{}[/]: {}", e(k), e(v)))
                }
                _ => render::line(&c, &e(l)),
            }
        }
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
        let con = render::console();
        render::diagnostics(&con, &outcome.diagnostics);
        if !outcome.checks.is_empty() {
            let mut g = Grid::new(
                &format!("Capability checks: {deployment}"),
                &["adapter", "role", "capability", "status", "evidence"],
            );
            for c in &outcome.checks {
                let status = c.status.as_str();
                let style = match status {
                    "supported" => "green",
                    "unsupported" => "red",
                    _ => "yellow",
                };
                g.row(&[
                    e(&c.adapter),
                    e(c.role.as_str()),
                    e(&c.capability),
                    format!("[{style}]{}[/]", e(status)),
                    e(c.evidence_scope.as_str()),
                ]);
            }
            g.print(&con);
        }
        render::line(
            &con,
            &format!(
                "plannable={} execution_ready={}",
                outcome.plannable, outcome.execution_ready
            ),
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
    let bytes = std::fs::read(project).map_err(|e| format!("{}: {e}", project.display()))?;
    let r = read_json(report)?;
    match featlas_api::check_report(&r, &bytes, fmt_of(project)) {
        Ok(v) => {
            let ok = v["consistent"] == json!(true);
            print_json(&v);
            Ok(if ok {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            })
        }
        Err(e) => emit("check-report", Err(e)),
    }
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
        let con = render::console();
        let mut g = Grid::new(
            &format!("Acceptance cases: {}", path.display()),
            &["status", "suite/case", "feature", "reason"],
        );
        for c in &summary.cases {
            let st = match c.status {
                CaseStatus::Passed => "[green]PASS[/]",
                CaseStatus::Failed => "[bold red]FAIL[/]",
                CaseStatus::NotExecuted => "[yellow]NOT EXECUTED[/]",
            };
            g.row(&[
                st.to_string(),
                e(&format!("{}/{}", c.suite, c.case)),
                e(&format!("{}@{}", c.feature, c.feature_version)),
                e(&c.reason),
            ]);
        }
        g.print(&con);
        render::line(
            &con,
            &format!(
                "executed={} passed={} failed={} not_executed={} [dim](not-executed cases are not passes)[/]",
                summary.executed, summary.passed, summary.failed, summary.not_executed
            ),
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
        let con = render::console();
        let st = match r.status {
            CaseStatus::Passed => "[bold green]Passed[/]",
            CaseStatus::Failed => "[bold red]Failed[/]",
            CaseStatus::NotExecuted => "[bold yellow]NotExecuted[/]",
        };
        render::line(&con, &format!("{st}: {}", e(&r.reason)));
        if !r.trace.is_empty() {
            let mut g = Grid::new(
                &format!("Trace: {case_id}"),
                &["stage", "feature", "detail"],
            );
            for t in &r.trace {
                g.row(&[e(&t.stage.to_string()), e(&t.feature), e(&t.detail)]);
            }
            g.print(&con);
        }
        render::json(
            &con,
            &json!({"rows": r.actual_rows, "diagnostic": r.actual_diagnostic}),
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
    let bytes = std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let statuses = read_json(status)?;
    emit(
        "coverage",
        featlas_api::coverage(
            &bytes,
            fmt_of(path),
            deployment,
            feature_set,
            as_of,
            &statuses,
        ),
    )
}

pub fn generate(path: &Path, deployment: &str, out: &Path) -> Result<ExitCode, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let gen = match featlas_api::generate(&bytes, fmt_of(path), deployment, None) {
        Ok(g) => g,
        Err(e) => {
            let con = render::console();
            if let Some(diags) = e.detail.as_array() {
                for d in diags {
                    let sev = d["severity"].as_str().unwrap_or("");
                    let style = if sev == "error" { "bold red" } else { "yellow" };
                    render::line(
                        &con,
                        &format!(
                            "[{style}]{}[/] {} \\[{}]: {}",
                            render::e(sev),
                            render::e(d["code"].as_str().unwrap_or("")),
                            render::e(d["rule"].as_str().unwrap_or("")),
                            render::e(d["message"].as_str().unwrap_or(""))
                        ),
                    );
                }
            }
            render::line(
                &con,
                &format!("[bold red]Not generated[/]: {}", render::e(&e.to_string())),
            );
            return Ok(ExitCode::from(1));
        }
    };
    let files = gen["files"].as_object().cloned().unwrap_or_default();
    for (rel, content) in &files {
        let target = out.join(rel);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        std::fs::write(&target, content.as_str().unwrap_or(""))
            .map_err(|e| format!("{}: {e}", target.display()))?;
    }
    let mut manifest = gen["manifest"].clone();
    manifest["generator"]["cli"] = json!(env!("CARGO_PKG_VERSION"));
    manifest["generator"]["executable_sha256"] =
        json!(featlas_adapters::local::executable_sha256());
    std::fs::write(
        out.join("manifest.json"),
        serde_json::to_string_pretty(&manifest).unwrap(),
    )
    .map_err(|e| e.to_string())?;
    render::line(
        &render::console(),
        &format!(
            "[green]Generated[/] {} files under {}; execution_ready={}",
            files.len() + 1,
            e(&out.display().to_string()),
            manifest["execution_ready"]
        ),
    );
    Ok(ExitCode::SUCCESS)
}

pub fn consume(path: &Path, request: &Path, records: Option<&Path>) -> Result<ExitCode, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let req = read_json(request)?;
    let recs = records.map(read_json).transpose()?;
    emit(
        "consume",
        featlas_api::consume(&bytes, fmt_of(path), &req, recs.as_ref()),
    )
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
        let con = render::console();
        render::line(
            &con,
            &format!("[bold cyan]Migration plan[/] {} -> {}", e(from), e(to)),
        );
        for b in &plan.blockers {
            render::line(
                &con,
                &format!("  [bold red]BLOCKER[/]: {}", e(&b.to_string())),
            );
        }
        for r in &plan.semantic_review {
            render::line(&con, &format!("  [yellow]REVIEW[/]: {}", e(&r.to_string())));
        }
        render::json(&con, &serde_json::to_value(&plan.mapping).unwrap());
    }
    Ok(ExitCode::SUCCESS)
}

pub fn label(path: &Path, consumer: &str, input: &Path) -> Result<ExitCode, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let li = read_json(input)?;
    emit(
        "label",
        featlas_api::labels(&bytes, fmt_of(path), consumer, &li),
    )
}
