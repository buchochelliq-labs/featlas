//! Canonical JSON and identity hashing (SEM-ID-004, SEM-PORT-008).
//!
//! `featlas-canon/1` canonical JSON:
//! * objects: keys sorted by Unicode code point, no whitespace;
//! * arrays: order preserved (ordering is meaningful in this language);
//! * strings: JSON escapes for `"`, `\\`, and U+0000–U+001F only (`\b \f \n \r \t`
//!   short forms, otherwise `\u00XX` lowercase); all other characters literal UTF-8;
//! * integers: shortest decimal; numbers (binary64): Rust shortest round-trip
//!   form, always containing `.` or `e` so `1` (integer) and `1.0` (number)
//!   remain distinct; `-0.0` is written `-0.0`.
//!
//! Four identity scopes are kept separate:
//! 1. logical semantic fingerprints (entity/source/relationship/feature/set/consumer),
//!    which exclude documentation fields and every deployment binding;
//! 2. deployment identity, which includes bindings and adapter pins;
//! 3. implementation identity (core version, adapter implementation/version);
//! 4. run/publication identity (assigned at execution time).
//!
//! The project-file SHA-256 hashes exact bytes and is binding-sensitive; it is
//! never a logical fingerprint.

use crate::model::Project;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub const CANON_VERSION: &str = "featlas-canon/1";

/// Keys removed at every depth by the semantic projection: documentation only.
const DOC_KEYS: &[&str] = &["description", "extensions"];
/// Keys removed from the top level of a logical object: discovery/ownership metadata.
const TOP_DOC_KEYS: &[&str] = &["name", "owner", "owners", "tags", "status", "acceptance"];

pub fn canonical_json(v: &Value) -> String {
    let mut out = String::new();
    write(v, &mut out);
    out
}

fn write(v: &Value, out: &mut String) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                out.push_str(&i.to_string());
            } else if let Some(u) = n.as_u64() {
                out.push_str(&u.to_string());
            } else {
                let f = n.as_f64().expect("finite number");
                let s = format!("{f:?}");
                out.push_str(&s);
            }
        }
        Value::String(s) => write_str(s, out),
        Value::Array(a) => {
            out.push('[');
            for (i, x) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write(x, out);
            }
            out.push(']');
        }
        Value::Object(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort_by(|a, b| a.chars().cmp(b.chars()));
            out.push('{');
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_str(k, out);
                out.push(':');
                write(&m[*k], out);
            }
            out.push('}');
        }
    }
}

fn write_str(s: &str, out: &mut String) {
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

pub fn sha256(text: &str) -> String {
    hex::encode(Sha256::digest(text.as_bytes()))
}

pub fn digest(v: &Value) -> String {
    sha256(&canonical_json(v))
}

fn strip_docs(v: &Value) -> Value {
    match v {
        Value::Object(m) => Value::Object(
            m.iter()
                .filter(|(k, _)| !DOC_KEYS.contains(&k.as_str()))
                .map(|(k, v)| (k.clone(), strip_docs(v)))
                .collect(),
        ),
        Value::Array(a) => Value::Array(a.iter().map(strip_docs).collect()),
        other => other.clone(),
    }
}

/// Semantic projection of one logical object.
pub fn project_object<T: serde::Serialize>(obj: &T) -> Value {
    let v = serde_json::to_value(obj).expect("serializable model");
    match strip_docs(&v) {
        Value::Object(m) => Value::Object(
            m.into_iter()
                .filter(|(k, _)| !TOP_DOC_KEYS.contains(&k.as_str()))
                .collect(),
        ),
        other => other,
    }
}

fn envelope(kind: &str, body: Map<String, Value>) -> Value {
    let mut m = Map::new();
    m.insert("canon".into(), Value::String(CANON_VERSION.into()));
    m.insert("kind".into(), Value::String(kind.into()));
    for (k, v) in body {
        m.insert(k, v);
    }
    Value::Object(m)
}

/// Logical fingerprints for every feature, feature set and consumer.
#[derive(Debug, Clone, serde::Serialize, PartialEq)]
pub struct LogicalIdentities {
    pub canon: String,
    pub features: BTreeMap<String, String>,
    pub feature_sets: BTreeMap<String, String>,
    pub consumers: BTreeMap<String, String>,
    pub entities: BTreeMap<String, String>,
    pub sources: BTreeMap<String, String>,
    pub relationships: BTreeMap<String, String>,
    /// Digest over all logical collections (excludes deployments and acceptance suites).
    pub logical_project: String,
}

pub fn feature_fingerprint(
    p: &Project,
    id: &str,
    memo: &mut BTreeMap<String, String>,
) -> Option<String> {
    if let Some(h) = memo.get(id) {
        return Some(h.clone());
    }
    let f = p.feature(id)?;
    let mut body = Map::new();
    body.insert("feature".into(), project_object(f));
    if let Some(e) = p.entity(&f.entity) {
        body.insert("entity".into(), project_object(e));
        if let Some(s) = p.source(&e.population.source) {
            body.insert("population_source".into(), project_object(s));
        }
    }
    let mut sources = Map::new();
    for i in &f.inputs {
        if let Some(s) = p.source(&i.source) {
            sources.insert(format!("{}@{}", s.id, s.version), project_object(s));
        }
    }
    body.insert("sources".into(), Value::Object(sources));
    let mut rels = Map::new();
    if let Some((_, joins, _, _)) = f.transform.row_shape() {
        for j in joins {
            if let Some(r) = p.relationship(&j.relationship) {
                rels.insert(format!("{}@{}", r.id, r.version), project_object(r));
            }
        }
    }
    body.insert("relationships".into(), Value::Object(rels));
    // Insert a placeholder to break cycles; cycles are rejected by validation anyway.
    memo.insert(id.to_string(), "cycle".into());
    let mut deps = Map::new();
    for d in &f.dependencies {
        let h = feature_fingerprint(p, &d.feature, memo).unwrap_or_else(|| "unresolved".into());
        deps.insert(format!("{}@{}", d.feature, d.version), Value::String(h));
    }
    body.insert("dependencies".into(), Value::Object(deps));
    let h = digest(&envelope("feature", body));
    memo.insert(id.to_string(), h.clone());
    Some(h)
}

pub fn logical_identities(p: &Project) -> LogicalIdentities {
    let mut memo = BTreeMap::new();
    let features: BTreeMap<String, String> = p
        .features
        .iter()
        .filter_map(|f| {
            feature_fingerprint(p, &f.id, &mut memo).map(|h| (format!("{}@{}", f.id, f.version), h))
        })
        .collect();
    let mut feature_sets = BTreeMap::new();
    for s in &p.feature_sets {
        let mut body = Map::new();
        body.insert("feature_set".into(), project_object(s));
        let members: Map<String, Value> = s
            .features
            .iter()
            .map(|m| {
                (
                    format!("{}@{}", m.id, m.version),
                    Value::String(
                        features
                            .get(&format!("{}@{}", m.id, m.version))
                            .cloned()
                            .unwrap_or_default(),
                    ),
                )
            })
            .collect();
        body.insert("members".into(), Value::Object(members));
        feature_sets.insert(
            format!("{}@{}", s.id, s.version),
            digest(&envelope("feature_set", body)),
        );
    }
    let mut consumers = BTreeMap::new();
    for c in &p.consumers {
        let mut body = Map::new();
        body.insert("consumer".into(), project_object(c));
        let key = format!("{}@{}", c.feature_set.feature_set, c.feature_set.version);
        body.insert(
            "feature_set".into(),
            Value::String(feature_sets.get(&key).cloned().unwrap_or_default()),
        );
        consumers.insert(
            format!("{}@{}", c.id, c.version),
            digest(&envelope("consumer", body)),
        );
    }
    let one = |kind: &str, v: Value| {
        let mut body = Map::new();
        body.insert(kind.into(), v);
        digest(&envelope(kind, body))
    };
    let entities = p
        .entities
        .iter()
        .map(|e| {
            (
                format!("{}@{}", e.id, e.version),
                one("entity", project_object(e)),
            )
        })
        .collect();
    let sources = p
        .sources
        .iter()
        .map(|s| {
            (
                format!("{}@{}", s.id, s.version),
                one("source", project_object(s)),
            )
        })
        .collect();
    let relationships = p
        .relationships
        .iter()
        .map(|r| {
            (
                format!("{}@{}", r.id, r.version),
                one("relationship", project_object(r)),
            )
        })
        .collect();
    let mut body = Map::new();
    body.insert("spec_version".into(), Value::String(p.spec_version.clone()));
    for (k, v) in [
        ("entities", serde_json::to_value(&entities).unwrap()),
        ("sources", serde_json::to_value(&sources).unwrap()),
        (
            "relationships",
            serde_json::to_value(&relationships).unwrap(),
        ),
        ("features", serde_json::to_value(&features).unwrap()),
        ("feature_sets", serde_json::to_value(&feature_sets).unwrap()),
        ("consumers", serde_json::to_value(&consumers).unwrap()),
    ] {
        body.insert(k.into(), v);
    }
    let logical_project = digest(&envelope("logical_project", body));
    LogicalIdentities {
        canon: CANON_VERSION.into(),
        features,
        feature_sets,
        consumers,
        entities,
        sources,
        relationships,
        logical_project,
    }
}

/// Deployment identity: binding-sensitive, documentation excluded.
pub fn deployment_identity(p: &Project, deployment_id: &str) -> Option<String> {
    let d = p.deployment(deployment_id)?;
    let ids = logical_identities(p);
    let mut body = Map::new();
    body.insert("deployment".into(), project_object(d));
    let sets: Map<String, Value> = d
        .output_targets
        .iter()
        .map(|t| {
            let k = format!("{}@{}", t.feature_set, t.version);
            (
                k.clone(),
                Value::String(ids.feature_sets.get(&k).cloned().unwrap_or_default()),
            )
        })
        .collect();
    body.insert("feature_sets".into(), Value::Object(sets));
    Some(digest(&envelope("deployment", body)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn canonical_form_is_frozen() {
        let v = json!({"b": [1, 1.0, -0.0, 0.1, 1e300], "a": "x\"\n\u{1}é", "c": {"z": null, "y": true}});
        assert_eq!(
            canonical_json(&v),
            r#"{"a":"x\"\n\u0001é","b":[1,1.0,-0.0,0.1,1e300],"c":{"y":true,"z":null}}"#
        );
        assert_eq!(
            sha256(""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }
}
