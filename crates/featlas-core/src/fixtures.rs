//! Acceptance-suite runner over the exact local evaluator.
//!
//! Golden expectations are read from the project and never regenerated.
//! `adapter_required` suites and cases needing capabilities this evaluator
//! lacks are reported `not_executed` with a reason; they never count as passes.

use crate::eval::{evaluate, Limits, OutputRow, Request, TraceStep};
use crate::model::*;
use crate::time::Instant;
use crate::value::{equal_with_tolerance, V};
use serde::Serialize;
use serde_json::Value as Json;

/// Capabilities the local reference evaluator actually implements.
pub const LOCAL_CAPABILITIES: &[&str] = &[
    "core.aggregate",
    "core.recency",
    "core.lookup",
    "core.expression",
    "core.revision_reconstruction",
    "core.as_of_join",
    "core.relationship_join",
    "core.source_as_known",
    "core.serving_replay",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CaseStatus {
    Passed,
    Failed,
    NotExecuted,
}

#[derive(Debug, Clone, Serialize)]
pub struct CaseResult {
    pub suite: String,
    pub case: String,
    pub feature: String,
    pub feature_version: String,
    pub status: CaseStatus,
    pub reason: String,
    pub expected: Json,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual_rows: Option<Vec<OutputRow>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual_diagnostic: Option<Json>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub trace: Vec<TraceStep>,
    /// Warning-severity quality findings of the evaluation (values unchanged).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub quality_warnings: Vec<crate::eval::QualityFinding>,
}

pub fn run_case(
    p: &Project,
    suite: &AcceptanceSuite,
    case: &AcceptanceCase,
    limits: &Limits,
    with_trace: bool,
) -> CaseResult {
    let mut result = CaseResult {
        suite: suite.id.clone(),
        case: case.id.clone(),
        feature: case.feature.id.clone(),
        feature_version: case.feature.version.clone(),
        status: CaseStatus::NotExecuted,
        reason: String::new(),
        expected: serde_json::to_value(&case.expected).unwrap_or(Json::Null),
        actual_rows: None,
        actual_diagnostic: None,
        trace: Vec::new(),
        quality_warnings: Vec::new(),
    };
    if suite.runner == Runner::AdapterRequired {
        result.reason =
            "Suite runner is adapter_required; the local reference evaluator does not execute it"
                .into();
        return result;
    }
    let missing: Vec<&String> = case
        .context
        .capabilities
        .iter()
        .filter(|c| !LOCAL_CAPABILITIES.contains(&c.as_str()))
        .collect();
    if !missing.is_empty() {
        result.reason = format!("Case requires capabilities not implemented locally: {missing:?}");
        return result;
    }
    let (as_of, requested_at) = match (
        Instant::parse(&case.as_of),
        case.context
            .requested_at
            .as_deref()
            .map(Instant::parse)
            .transpose(),
    ) {
        (Ok(a), Ok(r)) => (a, r),
        _ => {
            result.status = CaseStatus::Failed;
            result.reason = "Invalid case timestamps".into();
            return result;
        }
    };
    let req = Request {
        as_of,
        source_rows: &case.source_rows,
        source_status: &case.context.source_status,
        expected_population: Some(&case.population_rows),
        mode: case.context.mode,
        requested_at,
        published_values: case.context.published_values.as_deref(),
        consumer: case.context.consumer.as_deref(),
        capabilities: &case.context.capabilities,
    };
    let outcome = evaluate(p, &case.feature.id, &req, limits);
    let feature = p.feature(&case.feature.id);
    match (outcome, &case.expected) {
        (Ok(ev), Expectation::Rows { rows }) => {
            if with_trace {
                result.trace = ev.trace.clone();
            }
            let cmp = feature
                .map(|f| compare_rows(f, &ev.rows, rows))
                .unwrap_or(Err("Unknown feature".into()));
            result.quality_warnings = ev.quality_warnings.clone();
            result.actual_rows = Some(ev.rows);
            match cmp {
                Ok(()) => result.status = CaseStatus::Passed,
                Err(m) => {
                    result.status = CaseStatus::Failed;
                    result.reason = m;
                }
            }
        }
        (Ok(ev), Expectation::Diagnostic { diagnostic }) => {
            result.status = CaseStatus::Failed;
            result.reason = format!(
                "Expected diagnostic {} but evaluation produced rows",
                diagnostic.code
            );
            result.quality_warnings = ev.quality_warnings.clone();
            result.actual_rows = Some(ev.rows);
        }
        (Err(e), Expectation::Diagnostic { diagnostic }) => {
            result.actual_diagnostic =
                Some(serde_json::json!({"code": e.code, "rule": e.rule, "message": e.message}));
            let msg_ok = diagnostic
                .message_contains
                .as_ref()
                .is_none_or(|m| e.message.contains(m.as_str()));
            if e.code == diagnostic.code && msg_ok {
                result.status = CaseStatus::Passed;
            } else {
                result.status = CaseStatus::Failed;
                result.reason = format!(
                    "Expected diagnostic {}, got {}: {}",
                    diagnostic.code, e.code, e.message
                );
            }
        }
        (Err(e), Expectation::Rows { .. }) => {
            result.status = CaseStatus::Failed;
            result.reason = format!("Unexpected diagnostic {}: {}", e.code, e.message);
            result.actual_diagnostic =
                Some(serde_json::json!({"code": e.code, "rule": e.rule, "message": e.message}));
        }
    }
    result
}

/// Compare complete keys, states and values (with the feature's tolerance).
pub fn compare_rows(
    f: &Feature,
    actual: &[OutputRow],
    expected: &[ExpectedRow],
) -> Result<(), String> {
    if actual.len() != expected.len() {
        return Err(format!(
            "Expected {} rows, got {}",
            expected.len(),
            actual.len()
        ));
    }
    let entity_key =
        |m: &serde_json::Map<String, Json>| crate::canon::canonical_json(&Json::Object(m.clone()));
    let mut a: Vec<&OutputRow> = actual.iter().collect();
    let mut e: Vec<&ExpectedRow> = expected.iter().collect();
    a.sort_by_key(|r| entity_key(&r.entity_keys));
    e.sort_by_key(|r| entity_key(&r.entity_keys));
    let (abs, rel) = f
        .output
        .numeric_tolerance
        .as_ref()
        .map(|t| (t.absolute, t.relative))
        .unwrap_or((0.0, 0.0));
    for (got, want) in a.iter().zip(e.iter()) {
        if entity_key(&got.entity_keys) != entity_key(&want.entity_keys) {
            return Err(format!(
                "Entity keys differ: expected {:?}, got {:?}",
                want.entity_keys, got.entity_keys
            ));
        }
        if got.state != want.state {
            return Err(format!(
                "State differs for {:?}: expected {}, got {}",
                want.entity_keys, want.state, got.state
            ));
        }
        let ty = if V::from_json(&want.value, f.output.ty).is_ok() {
            f.output.ty
        } else {
            LogicalType::String
        };
        if !equal_with_tolerance(&got.value, &want.value, ty, abs, rel) {
            return Err(format!(
                "Value differs for {:?}: expected {}, got {}",
                want.entity_keys, want.value, got.value
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize)]
pub struct SuiteSummary {
    pub executed: usize,
    pub passed: usize,
    pub failed: usize,
    pub not_executed: usize,
    pub cases: Vec<CaseResult>,
}

pub fn run_suites(
    p: &Project,
    suite_filter: Option<&str>,
    limits: &Limits,
    with_trace: bool,
) -> SuiteSummary {
    let mut cases = Vec::new();
    for s in &p.acceptance_suites {
        if suite_filter.is_some_and(|f| f != s.id) {
            continue;
        }
        for c in &s.cases {
            cases.push(run_case(p, s, c, limits, with_trace));
        }
    }
    let count = |st: CaseStatus| cases.iter().filter(|c| c.status == st).count();
    SuiteSummary {
        executed: count(CaseStatus::Passed) + count(CaseStatus::Failed),
        passed: count(CaseStatus::Passed),
        failed: count(CaseStatus::Failed),
        not_executed: count(CaseStatus::NotExecuted),
        cases,
    }
}
