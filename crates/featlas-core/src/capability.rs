//! Adapter registry interface, target planning preflight and capability reports.
//!
//! The core resolves adapters only by the exact `(implementation, version)`
//! pair declared in a deployment; it never dispatches on vendor names. An
//! adapter publishes its exact-version configuration schema and its own
//! per-role capability assessments. The core validates the configuration
//! envelope against that schema without interpreting vendor settings.
//!
//! Reports keep four things apart: support status, evidence scope, exact
//! adapter identity and execution readiness (SEM-PORT-006). A requirement
//! appearing in configuration never proves support.

use crate::diag::{Diagnostic, Stage};
use crate::model::*;
use crate::plan::{build_plan, Plan, Requirement};
use serde::Serialize;
use serde_json::{Map, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    SupportedNative,
    SupportedEmulated,
    Unsupported,
    Unverified,
}

impl Status {
    pub fn as_str(self) -> &'static str {
        match self {
            Status::SupportedNative => "supported_native",
            Status::SupportedEmulated => "supported_emulated",
            Status::Unsupported => "unsupported",
            Status::Unverified => "unverified",
        }
    }
    pub fn supported(self) -> bool {
        matches!(self, Status::SupportedNative | Status::SupportedEmulated)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceScope {
    Declaration,
    Static,
    LocalReference,
    LocalMock,
    LiveTarget,
}

impl EvidenceScope {
    pub fn as_str(self) -> &'static str {
        match self {
            EvidenceScope::Declaration => "declaration",
            EvidenceScope::Static => "static",
            EvidenceScope::LocalReference => "local_reference",
            EvidenceScope::LocalMock => "local_mock",
            EvidenceScope::LiveTarget => "live_target",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Assessment {
    pub status: Status,
    pub scope: EvidenceScope,
    pub evidence: Vec<ReportEvidence>,
    pub limitations: Vec<String>,
}

impl Assessment {
    pub fn unsupported(reason: impl Into<String>) -> Self {
        Assessment {
            status: Status::Unsupported,
            scope: EvidenceScope::Static,
            evidence: vec![],
            limitations: vec![reason.into()],
        }
    }
    pub fn unverified(scope: EvidenceScope, reason: impl Into<String>) -> Self {
        Assessment {
            status: Status::Unverified,
            scope,
            evidence: vec![],
            limitations: vec![reason.into()],
        }
    }
}

/// Context an adapter may use for its assessment (e.g. digests of local fixture runs).
#[derive(Debug, Clone, Default)]
pub struct AssessmentContext {
    /// Local reference evidence produced in this invocation (uri, sha256, description).
    pub local_evidence: Vec<ReportEvidence>,
    /// Live-target evidence the operator supplies (never fetched by the core).
    pub live_evidence: Vec<(String, Role, String, ReportEvidence)>,
}

/// An exact adapter implementation version.
pub trait AdapterDescriptor: Send + Sync {
    fn implementation(&self) -> &str;
    fn version(&self) -> &str;
    fn roles(&self) -> &[Role];
    /// JSON Schema (Draft 2020-12) for this exact version's configuration.
    fn configuration_schema(&self) -> Value;
    /// True when executing this adapter leaves the local process (cloud/external target).
    fn external(&self) -> bool;
    fn assess(&self, role: Role, capability: &str, ctx: &AssessmentContext) -> Assessment;
    fn implementation_sha256(&self) -> Option<String>;
    fn runtime_versions(&self) -> Map<String, Value>;
    /// Adapter-specific physical preflight of locations/mappings (e.g. namespace shape, identifier rules).
    fn check_bindings(&self, _p: &Project, _d: &Deployment, _adapter_id: &str) -> Vec<Diagnostic> {
        Vec::new()
    }
}

pub trait AdapterRegistry {
    fn get(&self, implementation: &str, version: &str) -> Option<&dyn AdapterDescriptor>;
    fn list(&self) -> Vec<(String, String)>;
}

fn pdiag(code: &str, rule: &str, path: String, msg: impl Into<String>) -> Diagnostic {
    Diagnostic::new(code, Stage::Planning, rule, path, msg)
}

#[derive(Debug, Clone, Serialize)]
pub struct CheckOut {
    pub adapter: String,
    pub role: Role,
    pub capability: String,
    pub status: Status,
    pub evidence_scope: EvidenceScope,
    pub evidence: Vec<ReportEvidence>,
    pub limitations: Vec<String>,
    pub reasons: Vec<String>,
    pub declared: bool,
    pub derived: bool,
    pub operations: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PlanningOutcome {
    pub plan: Option<Plan>,
    pub checks: Vec<CheckOut>,
    pub diagnostics: Vec<Diagnostic>,
    /// No unsupported requirement and all adapters resolved: code may be generated.
    pub plannable: bool,
    /// Every requirement supported with evidence adequate for the execution scope.
    pub execution_ready: bool,
    pub execution_scope: &'static str,
}

/// Resolve adapters, validate their configuration, derive requirements and assess them.
pub fn plan_deployment(
    p: &Project,
    file_sha256: &str,
    deployment_id: &str,
    registry: &dyn AdapterRegistry,
    ctx: &AssessmentContext,
) -> PlanningOutcome {
    let mut diagnostics = Vec::new();
    let Some(di) = p.deployments.iter().position(|d| d.id == deployment_id) else {
        return PlanningOutcome {
            plan: None,
            checks: vec![],
            diagnostics: vec![pdiag(
                "UNKNOWN_DEPLOYMENT",
                "SEM-ID-002",
                "/deployments".into(),
                format!("No deployment {deployment_id}"),
            )],
            plannable: false,
            execution_ready: false,
            execution_scope: "local_reference",
        };
    };
    let d = &p.deployments[di];
    let mut external = false;
    for (k, a) in d.adapters.iter().enumerate() {
        let path = format!("/deployments/{di}/adapters/{k}");
        let Some(desc) = registry.get(&a.implementation, &a.version) else {
            diagnostics.push(pdiag(
                "UNRESOLVED_ADAPTER",
                "SEM-PORT-002",
                path,
                format!("No installed adapter implements {}@{}; registered: {:?}. Example/synthetic adapter pins are not installed packages.", a.implementation, a.version, registry.list()),
            ));
            continue;
        };
        external |= desc.external();
        for r in &a.roles {
            if !desc.roles().contains(r) {
                diagnostics.push(pdiag(
                    "ADAPTER_ROLE_UNAVAILABLE",
                    "SEM-PORT-002",
                    format!("{path}/roles"),
                    format!(
                        "{}@{} does not provide role {}",
                        a.implementation,
                        a.version,
                        r.as_str()
                    ),
                ));
            }
        }
        let schema = desc.configuration_schema();
        match jsonschema::options()
            .with_draft(jsonschema::Draft::Draft202012)
            .build(&schema)
        {
            Err(e) => diagnostics.push(pdiag(
                "ADAPTER_CONFIG_SCHEMA_INVALID",
                "SEM-PORT-002",
                path.clone(),
                format!("Adapter configuration schema is invalid: {e}"),
            )),
            Ok(v) => {
                let instance = Value::Object(a.configuration.clone());
                for e in v.iter_errors(&instance) {
                    diagnostics.push(pdiag(
                        "ADAPTER_CONFIG_INVALID",
                        "SEM-PORT-002",
                        format!("{path}/configuration{}", e.instance_path()),
                        format!("{}@{} configuration: {e}", a.implementation, a.version),
                    ));
                }
            }
        }
        diagnostics.extend(desc.check_bindings(p, d, &a.id));
    }
    let plan = build_plan(p, file_sha256, deployment_id);
    let requirements: Vec<Requirement> = plan
        .as_ref()
        .map(|x| x.requirements.clone())
        .unwrap_or_default();
    let mut checks = Vec::new();
    for r in &requirements {
        if r.derived && !r.declared {
            diagnostics.push(
                pdiag("CAPABILITY_REQUIREMENT_DERIVED", "SEM-PORT-006", format!("/deployments/{di}/runtime/required_capabilities"), format!("Planner derived {}/{}/{} from the actual operations; it is required even though not declared", r.adapter, r.role.as_str(), r.capability))
                    .warning(),
            );
        }
        let assessment = d
            .adapter(&r.adapter)
            .and_then(|a| registry.get(&a.implementation, &a.version))
            .map(|desc| desc.assess(r.role, &r.capability, ctx))
            .unwrap_or_else(|| Assessment::unverified(EvidenceScope::Declaration, "Adapter implementation is not installed; nothing about this capability is established"));
        if assessment.status == Status::Unsupported {
            // Label-dataset construction and scheduling gate only their own operation; every
            // other unsupported requirement blocks planning of the deployment.
            let scoped = !r.operations.is_empty()
                && r.operations
                    .iter()
                    .all(|o| o == "label_dataset" || o == "schedule");
            let d = pdiag(
                "UNSUPPORTED_CAPABILITY",
                "SEM-PORT-006",
                format!("/deployments/{di}"),
                format!(
                    "{}/{} cannot satisfy {} (gates: {}): {}",
                    r.adapter,
                    r.role.as_str(),
                    r.capability,
                    r.operations.join(", "),
                    assessment.limitations.join("; ")
                ),
            );
            diagnostics.push(if scoped { d.warning() } else { d });
        }
        checks.push(CheckOut {
            adapter: r.adapter.clone(),
            role: r.role,
            capability: r.capability.clone(),
            status: assessment.status,
            evidence_scope: assessment.scope,
            evidence: assessment.evidence,
            limitations: assessment.limitations,
            reasons: r.reasons.clone(),
            declared: r.declared,
            derived: r.derived,
            operations: r.operations.clone(),
        });
    }
    let plannable = !diagnostics.iter().any(|x| x.is_error()) && plan.is_some();
    let scope_ok = |c: &CheckOut| {
        if external {
            c.evidence_scope == EvidenceScope::LiveTarget
        } else {
            c.evidence_scope != EvidenceScope::Declaration
        }
    };
    let execution_ready = plannable
        && checks
            .iter()
            .all(|c| c.status.supported() && scope_ok(c) && !c.evidence.is_empty());
    PlanningOutcome {
        plan,
        checks,
        diagnostics,
        plannable,
        execution_ready,
        execution_scope: if external {
            "external_target"
        } else {
            "local_reference"
        },
    }
}

/// Produce a schema-valid AdapterCapabilityReport for the exact project bytes.
pub fn capability_report(
    p: &Project,
    file_sha256: &str,
    deployment_id: &str,
    registry: &dyn AdapterRegistry,
    outcome: &PlanningOutcome,
    assessed_at: &str,
) -> CapabilityReport {
    let d = p.deployment(deployment_id).expect("deployment exists");
    let adapters = d
        .adapters
        .iter()
        .map(|a| {
            let desc = registry.get(&a.implementation, &a.version);
            AdapterAssessment {
                id: a.id.clone(),
                implementation: a.implementation.clone(),
                version: a.version.clone(),
                implementation_sha256: desc.and_then(|x| x.implementation_sha256()),
                runtime_versions: desc.map(|x| x.runtime_versions()).unwrap_or_default(),
            }
        })
        .collect();
    let checks = outcome
        .checks
        .iter()
        .map(|c| CapabilityCheck {
            adapter: c.adapter.clone(),
            role: c.role,
            capability: c.capability.clone(),
            status: c.status.as_str().into(),
            evidence_scope: c.evidence_scope.as_str().into(),
            evidence: c.evidence.clone(),
            limitations: c.limitations.clone(),
        })
        .collect();
    let mut limitations = vec![
        "Requirements include planner-derived operations as well as declared ones; declared support strings are never treated as evidence.".to_string(),
        "Evidence references are recorded, not fetched or authenticated by this report generator.".to_string(),
    ];
    if !outcome.execution_ready {
        limitations.push("Execution is not ready: at least one requirement is unsupported, unverified, or lacks evidence adequate for the execution scope.".into());
    }
    limitations.extend(
        outcome
            .diagnostics
            .iter()
            .filter(|x| x.is_error())
            .map(|x| format!("{}: {}", x.code, x.message)),
    );
    CapabilityReport {
        spec_version: "0.2.0".into(),
        kind: "AdapterCapabilityReport".into(),
        project_id: p.metadata.id.clone(),
        project_version: p.metadata.version.clone(),
        project_file_sha256: file_sha256.into(),
        deployment: DeploymentIdentity {
            id: d.id.clone(),
            version: d.version.clone(),
        },
        assessed_at: assessed_at.into(),
        execution_scope: outcome.execution_scope.into(),
        adapters,
        checks,
        execution_ready: outcome.execution_ready,
        limitations,
    }
}

const PLACEHOLDERS: &[&str] = &[
    "latest",
    "unknown",
    "unverified",
    "unobserved",
    "not_observed",
    "not observed",
    "not installed",
    "none",
    "n/a",
    "auto",
    "placeholder",
    "example",
];

/// Consistency check of a report against exact project bytes (a port of the
/// kit's bounded checker). It does not authenticate evidence.
pub fn check_report(report_value: &Value, p: &Project, file_sha256: &str) -> Vec<Diagnostic> {
    let mut out =
        crate::schema::validate_against("capability-report.schema.json", report_value, None);
    if !out.is_empty() {
        return out;
    }
    let report: CapabilityReport = match serde_json::from_value(report_value.clone()) {
        Ok(r) => r,
        Err(e) => {
            return vec![pdiag(
                "REPORT_DOCUMENT_INVALID",
                "SEM-PORT-006",
                "/".into(),
                e.to_string(),
            )]
        }
    };
    let mut add = |code: &str, path: &str, msg: String| {
        out.push(pdiag(code, "SEM-PORT-006", path.into(), msg))
    };
    if report.project_file_sha256 != file_sha256 {
        add(
            "REPORT_PROJECT_HASH_MISMATCH",
            "/project_file_sha256",
            "Report does not identify these exact project bytes".into(),
        );
    }
    if (report.project_id.as_str(), report.project_version.as_str())
        != (p.metadata.id.as_str(), p.metadata.version.as_str())
    {
        add(
            "REPORT_PROJECT_IDENTITY_MISMATCH",
            "/project_id",
            "Report project ID/version differs".into(),
        );
    }
    let Some(d) = p
        .deployments
        .iter()
        .find(|d| d.id == report.deployment.id && d.version == report.deployment.version)
    else {
        add(
            "REPORT_DEPLOYMENT_MISMATCH",
            "/deployment",
            "Report must select exactly one deployment ID/version".into(),
        );
        return out;
    };
    let mut seen = std::collections::BTreeSet::new();
    for (i, a) in report.adapters.iter().enumerate() {
        if !seen.insert(a.id.clone()) {
            add(
                "REPORT_DUPLICATE_ADAPTER",
                &format!("/adapters/{i}/id"),
                "Adapter assessed more than once".into(),
            );
        }
        if let Some(e) = d.adapter(&a.id) {
            if (e.implementation.as_str(), e.version.as_str())
                != (a.implementation.as_str(), a.version.as_str())
            {
                add(
                    "REPORT_ADAPTER_IDENTITY_MISMATCH",
                    &format!("/adapters/{i}"),
                    "Adapter implementation/version differs from the deployment pin".into(),
                );
            }
        }
    }
    let expected: std::collections::BTreeSet<String> =
        d.adapters.iter().map(|a| a.id.clone()).collect();
    if seen != expected {
        add(
            "REPORT_ADAPTER_ROSTER_MISMATCH",
            "/adapters",
            format!("Adapter roster differs: expected {expected:?}, got {seen:?}"),
        );
    }
    let mut keys = std::collections::BTreeSet::new();
    for (i, c) in report.checks.iter().enumerate() {
        let path = format!("/checks/{i}");
        if !keys.insert((c.adapter.clone(), c.role, c.capability.clone())) {
            add(
                "REPORT_DUPLICATE_CHECK",
                &path,
                "Adapter/role/capability assessed more than once".into(),
            );
        }
        match d.adapter(&c.adapter) {
            None => add(
                "REPORT_CHECK_ADAPTER_UNKNOWN",
                &path,
                "Check names no deployment adapter".into(),
            ),
            Some(a) if !a.roles.contains(&c.role) => add(
                "REPORT_CHECK_ROLE_MISMATCH",
                &path,
                "Check role is not bound to this adapter".into(),
            ),
            _ => {}
        }
        if c.status == "supported_native" || c.status == "supported_emulated" {
            if c.evidence.is_empty() {
                add(
                    "REPORT_SUPPORT_EVIDENCE_MISSING",
                    &path,
                    "Supported claims require digest-pinned evidence".into(),
                );
            }
            if report
                .adapters
                .iter()
                .find(|a| a.id == c.adapter)
                .and_then(|a| a.implementation_sha256.clone())
                .is_none()
            {
                add(
                    "REPORT_IMPLEMENTATION_DIGEST_MISSING",
                    &path,
                    "Supported claims require the exact implementation digest".into(),
                );
            }
            if c.evidence_scope == "declaration" {
                add(
                    "REPORT_DECLARATION_CANNOT_SUPPORT",
                    &path,
                    "Declarations do not establish support".into(),
                );
            }
        }
    }
    for r in crate::plan::derive_requirements(p, d) {
        if !keys.contains(&(r.adapter.clone(), r.role, r.capability.clone())) {
            add(
                "REPORT_REQUIRED_CAPABILITY_MISSING",
                "/checks",
                format!(
                    "Missing assessment for {}/{}/{} ({})",
                    r.adapter,
                    r.role.as_str(),
                    r.capability,
                    if r.declared {
                        "declared"
                    } else {
                        "planner-derived"
                    }
                ),
            );
        }
    }
    if report.execution_ready {
        if report
            .checks
            .iter()
            .any(|c| c.status != "supported_native" && c.status != "supported_emulated")
        {
            add(
                "REPORT_NOT_EXECUTION_READY",
                "/execution_ready",
                "Every check must be supported".into(),
            );
        }
        if report.execution_scope == "external_target" {
            for (i, c) in report.checks.iter().enumerate() {
                if c.evidence_scope != "live_target" {
                    add(
                        "REPORT_LIVE_EVIDENCE_REQUIRED",
                        &format!("/checks/{i}/evidence_scope"),
                        "External readiness requires live-target evidence".into(),
                    );
                }
            }
            for (i, a) in report.adapters.iter().enumerate() {
                let ok = !a.runtime_versions.is_empty()
                    && a.runtime_versions.iter().all(|(k, v)| {
                        let v = v.as_str().unwrap_or("").trim();
                        !k.trim().is_empty()
                            && !v.is_empty()
                            && !PLACEHOLDERS.contains(&v.to_ascii_lowercase().as_str())
                            && !v.starts_with(['<', '>', '=', '~', '^'])
                            && !v.contains('*')
                    });
                if !ok {
                    add(
                        "REPORT_RUNTIME_VERSIONS_REQUIRED",
                        &format!("/adapters/{i}/runtime_versions"),
                        "External readiness requires observed exact version pins".into(),
                    );
                }
            }
        }
    }
    out
}
