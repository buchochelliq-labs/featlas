//! Target-neutral consumer API and publication semantics.
//!
//! Applications address logical consumer IDs/versions, complete entity keys
//! and declared time inputs. The core resolves the deployment's consumer
//! binding to an output target, asks a storage adapter (through the
//! [`PublicationStore`] trait) for physical records, maps them back to logical
//! fields generically, and applies selection, freshness, null and atomic-set
//! rules itself. Callers never see table names or SDK objects; adapters never
//! decide which value is selected.

use crate::diag::EvalError;
use crate::model::*;
use crate::time::Instant;
use crate::value::{key_string, V};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as Json};
use std::collections::BTreeMap;

pub const OUTPUT_STATES: &[&str] = &["ok", "empty", "insufficient_history", "source_unavailable", "not_published"];

fn err(code: &str, rule: &str, msg: impl Into<String>) -> EvalError {
    EvalError::new(code, rule, msg)
}

/// One logical publication of one feature value for one entity.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PublicationRecord {
    pub feature: String,
    pub version: String,
    #[serde(skip)]
    pub entity_key: Vec<V>,
    pub entity_keys: Map<String, Json>,
    #[serde(serialize_with = "ser_instant")]
    pub as_of: Instant,
    /// Observed visibility instant on the declared surface; `None` while evidence is pending.
    #[serde(serialize_with = "ser_opt_instant")]
    pub published_at: Option<Instant>,
    pub sequence: i64,
    pub publication_id: String,
    pub materialization_id: Option<String>,
    #[serde(skip)]
    pub value: V,
    #[serde(rename = "value")]
    pub value_json: Json,
    pub state: String,
}

fn ser_instant<S: serde::Serializer>(t: &Instant, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&t.to_string())
}
fn ser_opt_instant<S: serde::Serializer>(t: &Option<Instant>, s: S) -> Result<S::Ok, S::Error> {
    match t {
        Some(t) => s.serialize_str(&t.to_string()),
        None => s.serialize_none(),
    }
}

fn parse_ts(v: &str, what: &str) -> Result<Instant, EvalError> {
    Instant::parse(v).map_err(|m| err("INVALID_TIMESTAMP", "SEM-TIME-003", format!("{what}: {m}")))
}

impl PublicationRecord {
    pub fn from_fixture(pv: &PublishedValue, entity: &Entity, f: &Feature) -> Result<Self, EvalError> {
        let mut key = Vec::new();
        for k in &entity.keys {
            let raw = pv.entity_keys.get(&k.name).ok_or_else(|| err("PUBLICATION_KEYS", "SEM-ENT-002", format!("Publication lacks entity key {}", k.name)))?;
            key.push(V::from_json(raw, k.ty).map_err(|m| err("PUBLICATION_KEYS", "SEM-ENT-002", m))?);
        }
        if pv.entity_keys.len() != entity.keys.len() {
            return Err(err("PUBLICATION_KEYS", "SEM-ENT-002", "Publication entity keys must be exactly the entity keys"));
        }
        let value = V::from_json(&pv.value, f.output.ty).map_err(|m| err("FIXTURE_TYPE_MISMATCH", "SEM-OUT-001", m))?;
        Ok(PublicationRecord {
            feature: pv.feature.clone(),
            version: pv.version.clone(),
            entity_key: key,
            entity_keys: pv.entity_keys.clone(),
            as_of: parse_ts(&pv.as_of, "as_of")?,
            published_at: Some(parse_ts(&pv.published_at, "published_at")?),
            sequence: pv.sequence,
            publication_id: pv.run_id.clone(),
            materialization_id: None,
            value_json: value.to_json(),
            value,
            state: pv.state.clone(),
        })
    }
}

/// Select under historical-serving replay: eligible = as_of ≤ T and observed
/// published_at ≤ requested_at; greatest (as_of, published_at, sequence);
/// duplicate ordering keys fail; latest null supersedes; staleness after selection.
pub fn select_replay<'r>(records: &'r [PublicationRecord], key: &[V], as_of: Instant, requested_at: Instant, max_staleness: i64) -> Result<Option<&'r PublicationRecord>, EvalError> {
    select(records, key, as_of, Some(requested_at), max_staleness)
}

/// Shared selection. `requested_at = None` is the source-as-known read of the
/// latest evidence-complete publication; replay passes `Some`.
pub fn select<'r>(records: &'r [PublicationRecord], key: &[V], as_of: Instant, requested_at: Option<Instant>, max_staleness: i64) -> Result<Option<&'r PublicationRecord>, EvalError> {
    let k = key_string(key);
    let eligible: Vec<&PublicationRecord> = records
        .iter()
        .filter(|r| key_string(&r.entity_key) == k && r.as_of <= as_of)
        .filter(|r| match (r.published_at, requested_at) {
            (None, _) => false, // Evidence pending: never served or replayed.
            (Some(p), Some(req)) => p <= req,
            (Some(_), None) => true,
        })
        .collect();
    let mut keys = BTreeMap::new();
    for r in &eligible {
        let ord = (r.as_of, r.published_at, r.sequence);
        if keys.insert(ord, *r).is_some() {
            return Err(err("PUBLICATION_TIE", "SEM-REPLAY-002", "Two eligible publications share (as_of, published_at, sequence); no arbitrary choice is made"));
        }
    }
    let Some((_, chosen)) = keys.into_iter().next_back() else { return Ok(None) };
    let age = (as_of.micros - chosen.as_of.micros) / 1_000_000;
    let exact_age_exceeds = as_of.micros - chosen.as_of.micros > max_staleness.saturating_mul(1_000_000);
    if exact_age_exceeds {
        return Err(err("STALE_FEATURE", "SEM-FRESH-002", format!("Selected publication is {age}s older than the request as_of; maximum {max_staleness}s; no fallback is applied")));
    }
    Ok(Some(chosen))
}

/// A resolved physical output target for one consumer.
#[derive(Debug, Clone, Serialize)]
pub struct ResolvedTarget<'p> {
    pub deployment: &'p Deployment,
    pub adapter: &'p AdapterInstance,
    pub target: &'p OutputTarget,
    pub consumer: &'p Consumer,
    pub feature_set: &'p FeatureSet,
    pub entity: &'p Entity,
}

/// Physical key filter handed to stores: physical column → JSON values (one per requested entity).
#[derive(Debug, Clone, Serialize)]
pub struct PhysicalReadRequest {
    pub location: Location,
    pub key_columns: Vec<String>,
    pub keys: Vec<Vec<Json>>,
    pub as_of_column: String,
    pub max_as_of: String,
}

/// Storage-role interface: physical I/O only. Implemented in adapter crates.
pub trait PublicationStore {
    fn read(&self, adapter: &AdapterInstance, request: &PhysicalReadRequest) -> Result<Vec<Map<String, Json>>, EvalError>;
    fn write(&self, adapter: &AdapterInstance, location: &Location, rows: Vec<Map<String, Json>>) -> Result<StoreCommit, EvalError>;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StoreCommit {
    /// Store-observed visibility instant of the atomic commit.
    pub committed_at: String,
    pub commit_id: String,
    pub rows: usize,
}

/// Resolve stores by exact implementation identity (no vendor-name dispatch).
pub trait StoreRegistry {
    fn store(&self, implementation: &str, version: &str) -> Option<&dyn PublicationStore>;
}

pub fn resolve<'p>(p: &'p Project, deployment_id: &str, consumer_id: &str, consumer_version: &str) -> Result<ResolvedTarget<'p>, EvalError> {
    let d = p.deployment(deployment_id).ok_or_else(|| err("UNKNOWN_DEPLOYMENT", "SEM-ID-002", deployment_id.to_string()))?;
    let consumer = p.consumer(consumer_id).ok_or_else(|| err("UNKNOWN_CONSUMER", "SEM-ID-002", consumer_id.to_string()))?;
    if consumer.version != consumer_version {
        return Err(err("VERSION_MISMATCH", "SEM-CONS-001", format!("Requested {consumer_id}@{consumer_version}; project has {}", consumer.version)));
    }
    let b = d
        .consumer_bindings
        .iter()
        .find(|b| b.consumer == consumer_id && b.version == consumer_version)
        .ok_or_else(|| err("CONSUMER_UNBOUND", "SEM-PORT-005", format!("Deployment {deployment_id} has no binding for {consumer_id}@{consumer_version}")))?;
    if b.role != d.publication.surface.role() {
        return Err(err("CONSUMER_SURFACE_MISMATCH", "SEM-PORT-005", "Consumer binding does not use the deployment's guaranteed publication surface"));
    }
    let adapter = d.adapter(&b.adapter).ok_or_else(|| err("UNKNOWN_ADAPTER", "SEM-PORT-002", b.adapter.clone()))?;
    let target = d
        .output_targets
        .iter()
        .find(|t| t.feature_set == consumer.feature_set.feature_set && t.version == consumer.feature_set.version && t.adapter == b.adapter && t.role == b.role)
        .ok_or_else(|| err("CONSUMER_TARGET_MISSING", "SEM-PORT-005", "No matching output target for the consumer binding"))?;
    let feature_set = p.feature_set(&target.feature_set).ok_or_else(|| err("UNKNOWN_FEATURE_SET", "SEM-ID-002", target.feature_set.clone()))?;
    let entity = p.entity(&feature_set.entity).ok_or_else(|| err("UNKNOWN_ENTITY", "SEM-ID-002", feature_set.entity.clone()))?;
    Ok(ResolvedTarget { deployment: d, adapter, target, consumer, feature_set, entity })
}

fn phys(map: &ColumnMap, logical: &str) -> Result<String, EvalError> {
    map.get(logical).and_then(Json::as_str).map(str::to_string).ok_or_else(|| err("OUTPUT_ENTITY_MAPPING_INCOMPLETE", "SEM-PORT-004", format!("No physical field for {logical}")))
}

/// Logical publication metadata attached to a snapshot write.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PublicationMetadata {
    pub as_of: String,
    /// Original observed visibility instant; `None` until evidence is recorded.
    pub published_at: Option<String>,
    pub publication_sequence: i64,
    pub publication_id: String,
    pub materialization_id: String,
}

/// Map a logical feature-set snapshot to physical rows (lossless, rename only).
pub fn to_physical(rt: &ResolvedTargetRef, rows: &[LogicalSnapshotRow], meta: &PublicationMetadata) -> Result<Vec<Map<String, Json>>, EvalError> {
    let t = rt.target;
    let mut out = Vec::new();
    for r in rows {
        let mut m = Map::new();
        for k in &rt.entity.keys {
            let v = r.entity_keys.get(&k.name).ok_or_else(|| err("OUTPUT_ENTITY_MAPPING_INCOMPLETE", "SEM-PORT-004", k.name.clone()))?;
            m.insert(phys(&t.entity_key_mapping, &k.name)?, v.clone());
        }
        for fm in &t.feature_mapping {
            let (value, state) = r.values.get(&fm.feature).ok_or_else(|| err("OUTPUT_FEATURE_MAPPING_INCOMPLETE", "SEM-PORT-004", fm.feature.clone()))?;
            if !OUTPUT_STATES.contains(&state.as_str()) {
                return Err(err("INVALID_STATE", "SEM-MISS-001", state.clone()));
            }
            m.insert(fm.column.clone(), value.clone());
            m.insert(fm.state_column.clone(), Json::String(state.clone()));
        }
        let mc = &t.metadata_columns;
        m.insert(mc.as_of.clone(), Json::String(meta.as_of.clone()));
        m.insert(mc.published_at.clone(), meta.published_at.clone().map(Json::String).unwrap_or(Json::Null));
        m.insert(mc.publication_sequence.clone(), Json::from(meta.publication_sequence));
        m.insert(mc.publication_id.clone(), Json::String(meta.publication_id.clone()));
        m.insert(mc.materialization_id.clone(), Json::String(meta.materialization_id.clone()));
        out.push(m);
    }
    Ok(out)
}

/// The parts of a resolved target needed for mapping in either direction.
pub struct ResolvedTargetRef<'p> {
    pub target: &'p OutputTarget,
    pub entity: &'p Entity,
    pub features: Vec<&'p Feature>,
}

impl<'p> ResolvedTargetRef<'p> {
    pub fn new(p: &'p Project, target: &'p OutputTarget) -> Result<Self, EvalError> {
        let fs = p.feature_set(&target.feature_set).ok_or_else(|| err("UNKNOWN_FEATURE_SET", "SEM-ID-002", target.feature_set.clone()))?;
        let entity = p.entity(&fs.entity).ok_or_else(|| err("UNKNOWN_ENTITY", "SEM-ID-002", fs.entity.clone()))?;
        let features = fs.features.iter().map(|m| p.feature(&m.id).ok_or_else(|| err("UNKNOWN_FEATURE", "SEM-ID-002", m.id.clone()))).collect::<Result<_, _>>()?;
        Ok(ResolvedTargetRef { target, entity, features })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LogicalSnapshotRow {
    pub entity_keys: Map<String, Json>,
    /// feature id → (value, state)
    pub values: BTreeMap<String, (Json, String)>,
}

/// Map physical rows back to logical publication records (one per member feature).
pub fn from_physical(rt: &ResolvedTargetRef, rows: &[Map<String, Json>]) -> Result<Vec<PublicationRecord>, EvalError> {
    let t = rt.target;
    let mc = &t.metadata_columns;
    let mut out = Vec::new();
    for (i, row) in rows.iter().enumerate() {
        let get = |col: &str| row.get(col).ok_or_else(|| err("PHYSICAL_FIELD_MISSING", "SEM-PORT-004", format!("record {i} lacks physical field {col}")));
        let mut key = Vec::new();
        let mut entity_keys = Map::new();
        for k in &rt.entity.keys {
            let raw = get(&phys(&t.entity_key_mapping, &k.name)?)?;
            let v = V::from_json(raw, k.ty).map_err(|m| err("PHYSICAL_TYPE_MISMATCH", "SEM-PORT-004", m))?;
            if v.is_null() {
                return Err(err("PHYSICAL_TYPE_MISMATCH", "SEM-PORT-004", format!("record {i} has a null entity key {}", k.name)));
            }
            entity_keys.insert(k.name.clone(), raw.clone());
            key.push(v);
        }
        let as_of = parse_ts(get(&mc.as_of)?.as_str().ok_or_else(|| err("PHYSICAL_TYPE_MISMATCH", "SEM-PORT-009", "as_of must be a timestamp string"))?, "as_of")?;
        let published_at = match get(&mc.published_at)? {
            Json::Null => None,
            Json::String(s) => Some(parse_ts(s, "published_at")?),
            _ => return Err(err("PHYSICAL_TYPE_MISMATCH", "SEM-PORT-009", "published_at must be a timestamp string or null")),
        };
        let sequence = get(&mc.publication_sequence)?.as_i64().ok_or_else(|| err("PHYSICAL_TYPE_MISMATCH", "SEM-PORT-009", "publication_sequence must be an integer"))?;
        let publication_id = get(&mc.publication_id)?.as_str().ok_or_else(|| err("PHYSICAL_TYPE_MISMATCH", "SEM-PORT-009", "publication_id must be a string"))?.to_string();
        let materialization_id = get(&mc.materialization_id)?.as_str().map(str::to_string);
        for fm in &t.feature_mapping {
            let f = rt.features.iter().find(|f| f.id == fm.feature).ok_or_else(|| err("UNKNOWN_FEATURE", "SEM-ID-002", fm.feature.clone()))?;
            let raw = get(&fm.column)?;
            let value = V::from_json(raw, f.output.ty).map_err(|m| err("PHYSICAL_TYPE_MISMATCH", "SEM-PORT-004", format!("{}: {m}", fm.column)))?;
            let state = get(&fm.state_column)?.as_str().filter(|s| OUTPUT_STATES.contains(s)).ok_or_else(|| err("PHYSICAL_TYPE_MISMATCH", "SEM-MISS-001", format!("{} must hold a declared output state", fm.state_column)))?.to_string();
            if value.is_null() && !f.output.nullable {
                return Err(err("PHYSICAL_TYPE_MISMATCH", "SEM-OUT-001", format!("{} is null for a non-null output", fm.column)));
            }
            out.push(PublicationRecord {
                feature: f.id.clone(),
                version: fm.version.clone(),
                entity_key: key.clone(),
                entity_keys: entity_keys.clone(),
                as_of,
                published_at,
                sequence,
                publication_id: publication_id.clone(),
                materialization_id: materialization_id.clone(),
                value_json: value.to_json(),
                value,
                state,
            });
        }
    }
    Ok(out)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerRequest {
    pub api_version: String,
    pub deployment: String,
    pub consumer: String,
    pub consumer_version: String,
    /// Each row holds exactly the consumer's entity input fields and time inputs.
    pub rows: Vec<Map<String, Json>>,
}

pub const CONSUMER_API_VERSION: &str = "featlas.consumer/v1";

#[derive(Debug, Clone, Serialize)]
pub struct FeatureValueOut {
    pub feature: String,
    pub version: String,
    pub value: Json,
    pub state: String,
    pub provenance: Option<Provenance>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Provenance {
    pub publication_id: String,
    pub materialization_id: Option<String>,
    pub feature_as_of: String,
    pub published_at: Option<String>,
    pub publication_sequence: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResponseRow {
    pub request_index: usize,
    pub entity_keys: Map<String, Json>,
    pub as_of: String,
    pub requested_at: String,
    pub values: Vec<FeatureValueOut>,
    pub diagnostics: Vec<crate::diag::Diagnostic>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConsumerResponse {
    pub api_version: String,
    pub consumer: String,
    pub consumer_version: String,
    pub feature_set: String,
    pub feature_set_version: String,
    pub selection_mode: SelectionMode,
    pub surface: Surface,
    pub deployment: String,
    pub deployment_version: String,
    pub adapter_implementation: String,
    pub adapter_version: String,
    pub rows: Vec<ResponseRow>,
}

struct ParsedRequestRow {
    key: Vec<V>,
    entity_keys: Map<String, Json>,
    as_of: Instant,
    requested_at: Instant,
}

fn parse_request_row(c: &Consumer, e: &Entity, row: &Map<String, Json>) -> Result<ParsedRequestRow, EvalError> {
    let mut allowed: Vec<&str> = c.entity_inputs.iter().map(|m| m.input_field.as_str()).collect();
    allowed.push(&c.time_inputs.as_of);
    allowed.push(&c.time_inputs.requested_at);
    if let Some(d) = &c.time_inputs.dataset_cutoff {
        allowed.push(d);
    }
    if let Some(extra) = row.keys().find(|k| !allowed.contains(&k.as_str())) {
        return Err(err("REQUEST_FIELD_UNKNOWN", "SEM-CONS-001", format!("Request field {extra} is not declared by the consumer")));
    }
    let mut key = Vec::new();
    let mut entity_keys = Map::new();
    for k in &e.keys {
        let m = c.entity_inputs.iter().find(|m| m.entity_key == k.name).ok_or_else(|| err("ENTITY_MAPPING_INCOMPLETE", "SEM-CONS-001", k.name.clone()))?;
        let raw = row.get(&m.input_field).ok_or_else(|| err("REQUEST_KEY_MISSING", "SEM-CONS-001", format!("Request lacks {} (entity key {}); complete keys including tenant keys are required", m.input_field, k.name)))?;
        let v = V::from_json(raw, k.ty).map_err(|msg| err("REQUEST_KEY_TYPE", "SEM-CONS-001", msg))?;
        if v.is_null() {
            return Err(err("REQUEST_KEY_MISSING", "SEM-CONS-001", format!("{} is null", m.input_field)));
        }
        entity_keys.insert(k.name.clone(), raw.clone());
        key.push(v);
    }
    let ts = |field: &str| -> Result<Instant, EvalError> {
        let raw = row.get(field).and_then(Json::as_str).ok_or_else(|| err("REQUEST_TIME_MISSING", "SEM-CONS-001", format!("Request lacks time input {field}")))?;
        parse_ts(raw, field)
    };
    Ok(ParsedRequestRow { key, entity_keys, as_of: ts(&c.time_inputs.as_of)?, requested_at: ts(&c.time_inputs.requested_at)? })
}

/// Serve a consumer request through the deployment's binding and a store.
pub fn serve(p: &Project, stores: &dyn StoreRegistry, req: &ConsumerRequest, max_rows: usize) -> Result<ConsumerResponse, EvalError> {
    if req.api_version != CONSUMER_API_VERSION {
        return Err(err("API_VERSION_UNSUPPORTED", "SEM-CONS-001", format!("Expected {CONSUMER_API_VERSION}")));
    }
    if req.rows.len() > max_rows {
        return Err(err("RESOURCE_LIMIT_EXCEEDED", "SEM-BUDGET-001", format!("{} request rows exceed the limit {max_rows}", req.rows.len())));
    }
    let rt = resolve(p, &req.deployment, &req.consumer, &req.consumer_version)?;
    let store = stores
        .store(&rt.adapter.implementation, &rt.adapter.version)
        .ok_or_else(|| err("UNRESOLVED_ADAPTER", "SEM-PORT-002", format!("No store implementation registered for {}@{}", rt.adapter.implementation, rt.adapter.version)))?;
    let mapping = ResolvedTargetRef::new(p, rt.target)?;
    let mut parsed = Vec::new();
    for row in &req.rows {
        parsed.push(parse_request_row(rt.consumer, rt.entity, row));
    }
    let ok_rows: Vec<&ParsedRequestRow> = parsed.iter().filter_map(|r| r.as_ref().ok()).collect();
    let key_columns: Vec<String> = rt.entity.keys.iter().map(|k| phys(&rt.target.entity_key_mapping, &k.name)).collect::<Result<_, _>>()?;
    let max_as_of = ok_rows.iter().map(|r| r.as_of).max().unwrap_or(Instant { micros: 0 });
    let read = PhysicalReadRequest {
        location: rt.target.location.clone(),
        key_columns,
        keys: ok_rows.iter().map(|r| r.entity_keys.values().cloned().collect()).collect(),
        as_of_column: rt.target.metadata_columns.as_of.clone(),
        max_as_of: max_as_of.to_string(),
    };
    let physical = if ok_rows.is_empty() { Vec::new() } else { store.read(rt.adapter, &read)? };
    let records = from_physical(&mapping, &physical)?;
    let c = rt.consumer;
    let mut rows = Vec::new();
    for (i, pr) in parsed.into_iter().enumerate() {
        let pr = match pr {
            Ok(x) => x,
            Err(e) => {
                rows.push(ResponseRow { request_index: i, entity_keys: Map::new(), as_of: String::new(), requested_at: String::new(), values: vec![], diagnostics: vec![e.to_diagnostic(&format!("/rows/{i}"))] });
                continue;
            }
        };
        let mut values = Vec::new();
        let mut diagnostics = Vec::new();
        let mut publication_ids = std::collections::BTreeSet::new();
        for m in &rt.feature_set.features {
            let recs: Vec<PublicationRecord> = records.iter().filter(|r| r.feature == m.id && r.version == m.version).cloned().collect();
            let requested = match c.selection.mode {
                SelectionMode::HistoricalServingReplay => Some(pr.requested_at),
                SelectionMode::SourceAsKnown => None,
            };
            match select(&recs, &pr.key, pr.as_of, requested, c.max_staleness_seconds) {
                Err(e) => diagnostics.push(e.to_diagnostic(&format!("/rows/{i}/{}", m.id))),
                Ok(None) => {
                    if c.missing_value_policy == MissingValuePolicy::Reject {
                        diagnostics.push(err("MISSING_FEATURE_VALUE", "SEM-FRESH-002", format!("{} has no eligible publication", m.id)).to_diagnostic(&format!("/rows/{i}/{}", m.id)));
                    }
                    values.push(FeatureValueOut { feature: m.id.clone(), version: m.version.clone(), value: Json::Null, state: "not_published".into(), provenance: None });
                }
                Ok(Some(r)) => {
                    if r.value.is_null() && c.missing_value_policy == MissingValuePolicy::Reject {
                        diagnostics.push(err("MISSING_FEATURE_VALUE", "SEM-FRESH-002", format!("{}: latest selected value is null ({}); no fallback", m.id, r.state)).to_diagnostic(&format!("/rows/{i}/{}", m.id)));
                    }
                    publication_ids.insert(r.publication_id.clone());
                    values.push(FeatureValueOut {
                        feature: m.id.clone(),
                        version: m.version.clone(),
                        value: r.value_json.clone(),
                        state: r.state.clone(),
                        provenance: Some(Provenance {
                            publication_id: r.publication_id.clone(),
                            materialization_id: r.materialization_id.clone(),
                            feature_as_of: r.as_of.to_string(),
                            published_at: r.published_at.map(|t| t.to_string()),
                            publication_sequence: r.sequence,
                        }),
                    });
                }
            }
        }
        if publication_ids.len() > 1 {
            diagnostics.push(err("ATOMIC_SET_INCONSISTENT", "SEM-REPLAY-002", "Selected member values come from different feature-set publications; atomic set consistency is not established").to_diagnostic(&format!("/rows/{i}")));
        }
        if diagnostics.iter().any(|d| d.is_error()) {
            values.clear();
        }
        rows.push(ResponseRow { request_index: i, entity_keys: pr.entity_keys, as_of: pr.as_of.to_string(), requested_at: pr.requested_at.to_string(), values, diagnostics });
    }
    Ok(ConsumerResponse {
        api_version: CONSUMER_API_VERSION.into(),
        consumer: c.id.clone(),
        consumer_version: c.version.clone(),
        feature_set: rt.feature_set.id.clone(),
        feature_set_version: rt.feature_set.version.clone(),
        selection_mode: c.selection.mode,
        surface: rt.deployment.publication.surface,
        deployment: rt.deployment.id.clone(),
        deployment_version: rt.deployment.version.clone(),
        adapter_implementation: rt.adapter.implementation.clone(),
        adapter_version: rt.adapter.version.clone(),
        rows,
    })
}
