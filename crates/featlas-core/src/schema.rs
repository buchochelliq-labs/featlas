//! Locally resolved Draft 2020-12 schema registry (structure stage).
//!
//! All twelve shipped schemas are embedded at build time. References resolve
//! only against these resources; the retriever refuses every URI, so no schema
//! is ever fetched from the network or the filesystem at runtime.

use crate::diag::{Diagnostic, Stage};
use crate::parse::ParsedDocument;
use jsonschema::{Draft, Registry, Retrieve, Uri};
use serde_json::Value;
use std::sync::OnceLock;

pub const SCHEMA_BASE: &str = "https://example.org/feature-engineering-spec/0.2.0/";

macro_rules! schema_file {
    ($name:literal) => {
        (
            $name,
            include_str!(concat!(
                "../../../spec/feature-engineering-spec-v0.2.0/schemas/",
                $name
            )),
        )
    };
}

/// (file name, contents) of every shipped schema.
pub const SCHEMAS: &[(&str, &str)] = &[
    schema_file!("acceptance-suite.schema.json"),
    schema_file!("adapter-instance.schema.json"),
    schema_file!("capability-report.schema.json"),
    schema_file!("consumer.schema.json"),
    schema_file!("deployment.schema.json"),
    schema_file!("entity.schema.json"),
    schema_file!("expression.schema.json"),
    schema_file!("feature-set.schema.json"),
    schema_file!("feature.schema.json"),
    schema_file!("project.schema.json"),
    schema_file!("relationship.schema.json"),
    schema_file!("source.schema.json"),
];

struct RefuseAll;

impl Retrieve for RefuseAll {
    fn retrieve(
        &self,
        uri: &Uri<String>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        Err(format!(
            "schema retrieval is disabled; unresolved reference {}",
            uri.as_str()
        )
        .into())
    }
}

fn parsed_schemas() -> &'static Vec<(String, Value)> {
    static CELL: OnceLock<Vec<(String, Value)>> = OnceLock::new();
    CELL.get_or_init(|| {
        SCHEMAS
            .iter()
            .map(|(name, text)| {
                (
                    name.to_string(),
                    serde_json::from_str(text).expect("embedded schema is JSON"),
                )
            })
            .collect()
    })
}

pub fn schema_value(name: &str) -> &'static Value {
    &parsed_schemas()
        .iter()
        .find(|(n, _)| n == name)
        .expect("known schema")
        .1
}

/// Check every embedded schema against the Draft 2020-12 metaschema and for unique `$id`s.
pub fn check_schemas() -> Result<usize, String> {
    let mut ids = std::collections::BTreeSet::new();
    for (name, schema) in parsed_schemas() {
        jsonschema::meta::validate(schema).map_err(|e| format!("{name}: {e}"))?;
        let id = schema
            .get("$id")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("{name} has no $id"))?;
        if !ids.insert(id.to_string()) {
            return Err(format!("duplicate $id {id}"));
        }
        if id != format!("{SCHEMA_BASE}{name}") {
            return Err(format!("{name}: $id {id} does not match its file name"));
        }
    }
    Ok(parsed_schemas().len())
}

/// Validate an instance against a named embedded schema.
pub fn validate_against(
    name: &str,
    instance: &Value,
    doc: Option<&ParsedDocument>,
) -> Vec<Diagnostic> {
    let mut registry = Registry::new();
    for (n, schema) in parsed_schemas() {
        registry = match registry.add(format!("{SCHEMA_BASE}{n}"), schema.clone()) {
            Ok(r) => r,
            Err(e) => return vec![internal(format!("schema registry: {e}"))],
        };
    }
    let registry = match registry.retriever(RefuseAll).prepare() {
        Ok(r) => r,
        Err(e) => return vec![internal(format!("schema registry: {e}"))],
    };
    let validator = match jsonschema::options()
        .with_draft(Draft::Draft202012)
        .with_registry(&registry)
        .with_retriever(RefuseAll)
        .should_validate_formats(false)
        .build(schema_value(name))
    {
        Ok(v) => v,
        Err(e) => return vec![internal(format!("schema build: {e}"))],
    };
    let mut out: Vec<Diagnostic> = validator
        .iter_errors(instance)
        .map(|e| {
            let path = e.instance_path().to_string();
            let location = doc.and_then(|d| d.locate(&path));
            Diagnostic::new(
                "SCHEMA_INVALID",
                Stage::Schema,
                "SEM-CONF-001",
                path,
                format!("{e} (schema {})", e.schema_path()),
            )
            .at(location)
        })
        .collect();
    out.sort_by(|a, b| {
        (a.path.clone(), a.message.clone()).cmp(&(b.path.clone(), b.message.clone()))
    });
    out.dedup_by(|a, b| a.path == b.path && a.message == b.message);
    out
}

pub fn validate_project(doc: &ParsedDocument) -> Vec<Diagnostic> {
    validate_against("project.schema.json", &doc.value, Some(doc))
}

fn internal(message: String) -> Diagnostic {
    Diagnostic::new(
        "SCHEMA_INTERNAL",
        Stage::Schema,
        "SEM-CONF-002",
        "",
        message,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_schemas_are_valid_and_local() {
        assert_eq!(check_schemas().unwrap(), 12);
    }
}
