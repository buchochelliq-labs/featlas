//! `featlas._native`: in-process Python bindings for the featlas core.
//!
//! Every function takes the exact project document bytes (hashed as given), a
//! format (`"yaml"`/`"json"`) and JSON text for structured inputs, and returns
//! JSON text produced by `featlas-api` — the same structures the CLI and MCP
//! server return. Failures raise `featlas._native.FeatlasError(code, message,
//! detail_json)`. The GIL is released while the core runs. Nothing here reads
//! files, fetches URIs, deploys or publishes.

use featlas_api as api;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use serde_json::Value;

pyo3::create_exception!(
    _native,
    FeatlasError,
    PyException,
    "A featlas core failure: args are (code, message, detail_json)."
);

fn raise(e: api::ApiError) -> PyErr {
    FeatlasError::new_err((e.code.clone(), e.message.clone(), e.detail.to_string()))
}

fn fmt(format: &str) -> PyResult<featlas_core::parse::Format> {
    api::parse_format(format).map_err(raise)
}

fn json_arg(text: &str) -> PyResult<Value> {
    api::parse_json(text).map_err(raise)
}

fn out(r: api::ApiResult) -> PyResult<String> {
    r.map(|v| v.to_string()).map_err(raise)
}

/// Parse, schema-validate and reference/type-check; never raises for invalid documents.
#[pyfunction]
fn validate(py: Python<'_>, document: &[u8], format: &str) -> PyResult<String> {
    let f = fmt(format)?;
    Ok(py.detach(|| api::validate(document, f)).to_string())
}

#[pyfunction]
fn hash(py: Python<'_>, document: &[u8], format: &str) -> PyResult<String> {
    let f = fmt(format)?;
    out(py.detach(|| api::hash(document, f)))
}

#[pyfunction]
fn explain(py: Python<'_>, document: &[u8], format: &str, feature: &str) -> PyResult<String> {
    let f = fmt(format)?;
    out(py.detach(|| api::explain(document, f, feature)))
}

#[pyfunction]
fn plan(py: Python<'_>, document: &[u8], format: &str, deployment: &str) -> PyResult<String> {
    let f = fmt(format)?;
    out(py.detach(|| api::plan(document, f, deployment)))
}

#[pyfunction]
#[pyo3(signature = (document, format, deployment, assessed_at=None))]
fn capabilities(
    py: Python<'_>,
    document: &[u8],
    format: &str,
    deployment: &str,
    assessed_at: Option<&str>,
) -> PyResult<String> {
    let f = fmt(format)?;
    out(py.detach(|| api::capabilities(document, f, deployment, assessed_at, Vec::new())))
}

#[pyfunction]
fn check_report(
    py: Python<'_>,
    report_json: &str,
    document: &[u8],
    format: &str,
) -> PyResult<String> {
    let f = fmt(format)?;
    let report = json_arg(report_json)?;
    out(py.detach(|| api::check_report(&report, document, f)))
}

#[pyfunction]
#[pyo3(signature = (document, format, suite=None))]
fn run_fixtures(
    py: Python<'_>,
    document: &[u8],
    format: &str,
    suite: Option<&str>,
) -> PyResult<String> {
    let f = fmt(format)?;
    out(py.detach(|| api::run_fixtures(document, f, suite)))
}

#[pyfunction]
fn evaluate_case(py: Python<'_>, document: &[u8], format: &str, case_id: &str) -> PyResult<String> {
    let f = fmt(format)?;
    out(py.detach(|| api::evaluate_case(document, f, case_id)))
}

#[pyfunction]
fn coverage(
    py: Python<'_>,
    document: &[u8],
    format: &str,
    deployment: &str,
    feature_set: &str,
    as_of: &str,
    statuses_json: &str,
) -> PyResult<String> {
    let f = fmt(format)?;
    let statuses = json_arg(statuses_json)?;
    out(py.detach(|| api::coverage(document, f, deployment, feature_set, as_of, &statuses)))
}

/// Generate plan, capability report and adapter code in memory (no files written).
#[pyfunction]
#[pyo3(signature = (document, format, deployment, generated_at=None))]
fn generate(
    py: Python<'_>,
    document: &[u8],
    format: &str,
    deployment: &str,
    generated_at: Option<&str>,
) -> PyResult<String> {
    let f = fmt(format)?;
    out(py.detach(|| api::generate(document, f, deployment, generated_at)))
}

#[pyfunction]
#[pyo3(signature = (document, format, request_json, records_json=None))]
fn consume(
    py: Python<'_>,
    document: &[u8],
    format: &str,
    request_json: &str,
    records_json: Option<&str>,
) -> PyResult<String> {
    let f = fmt(format)?;
    let request = json_arg(request_json)?;
    let records = records_json.map(json_arg).transpose()?;
    out(py.detach(|| api::consume(document, f, &request, records.as_ref())))
}

#[pyfunction]
fn migration_plan(
    py: Python<'_>,
    document: &[u8],
    format: &str,
    from_deployment: &str,
    to_deployment: &str,
) -> PyResult<String> {
    let f = fmt(format)?;
    out(py.detach(|| api::migration_plan(document, f, from_deployment, to_deployment)))
}

#[pyfunction]
fn labels(
    py: Python<'_>,
    document: &[u8],
    format: &str,
    consumer: &str,
    input_json: &str,
) -> PyResult<String> {
    let f = fmt(format)?;
    let input = json_arg(input_json)?;
    out(py.detach(|| api::labels(document, f, consumer, &input)))
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("FeatlasError", m.py().get_type::<FeatlasError>())?;
    m.add("API_SCHEMA", api::API_SCHEMA)?;
    m.add("CORE_VERSION", featlas_core::plan::CORE_VERSION)?;
    m.add("SPEC_VERSION", "0.2.0")?;
    for f in [
        wrap_pyfunction!(validate, m)?,
        wrap_pyfunction!(hash, m)?,
        wrap_pyfunction!(explain, m)?,
        wrap_pyfunction!(plan, m)?,
        wrap_pyfunction!(capabilities, m)?,
        wrap_pyfunction!(check_report, m)?,
        wrap_pyfunction!(run_fixtures, m)?,
        wrap_pyfunction!(evaluate_case, m)?,
        wrap_pyfunction!(coverage, m)?,
        wrap_pyfunction!(generate, m)?,
        wrap_pyfunction!(consume, m)?,
        wrap_pyfunction!(migration_plan, m)?,
        wrap_pyfunction!(labels, m)?,
    ] {
        m.add_function(f)?;
    }
    Ok(())
}
