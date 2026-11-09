//! Local adapters: the reference evaluator and an in-memory alternate store.
//!
//! * `dev.featlas:local_reference@0.1.0` — compute (the core's exact
//!   evaluator), catalog (read-only acceptance-fixture sources) and an
//!   offline store writing JSON Lines files with atomic rename.
//! * `dev.featlas:memory_store@0.1.0` — an offline-store **test double**
//!   holding records in process memory. It exists to prove storage-role
//!   separation and rebinding; it is not a production store.

use featlas_core::capability::{AdapterDescriptor, Assessment, AssessmentContext, EvidenceScope, Status};
use featlas_core::consumer::{PhysicalReadRequest, PublicationStore, StoreCommit};
use featlas_core::diag::{Diagnostic, EvalError, Stage};
use featlas_core::fixtures::LOCAL_CAPABILITIES;
use featlas_core::model::*;
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::io::Write;
use std::sync::Mutex;

pub const LOCAL_IMPLEMENTATION: &str = "dev.featlas:local_reference";
pub const MEMORY_IMPLEMENTATION: &str = "dev.featlas:memory_store";
pub const VERSION: &str = "0.1.0";

pub fn executable_sha256() -> Option<String> {
    use sha2::{Digest, Sha256};
    let path = std::env::current_exe().ok()?;
    let bytes = std::fs::read(path).ok()?;
    Some(hex::encode(Sha256::digest(bytes)))
}

fn empty_config_schema(id: &str) -> Value {
    json!({"$schema": "https://json-schema.org/draft/2020-12/schema", "$id": id, "type": "object", "additionalProperties": false, "properties": {}})
}

fn runtime() -> Map<String, Value> {
    let mut m = Map::new();
    m.insert("featlas-core".into(), Value::String(featlas_core::plan::CORE_VERSION.into()));
    m.insert("target".into(), Value::String(format!("{}-{}", std::env::consts::ARCH, std::env::consts::OS)));
    m
}

pub struct LocalReference;

impl AdapterDescriptor for LocalReference {
    fn implementation(&self) -> &str {
        LOCAL_IMPLEMENTATION
    }
    fn version(&self) -> &str {
        VERSION
    }
    fn roles(&self) -> &[Role] {
        &[Role::Compute, Role::Catalog, Role::OfflineStore]
    }
    fn configuration_schema(&self) -> Value {
        empty_config_schema("urn:featlas:adapter:dev.featlas:local_reference:0.1.0:configuration")
    }
    fn external(&self) -> bool {
        false
    }
    fn assess(&self, role: Role, capability: &str, ctx: &AssessmentContext) -> Assessment {
        let evidence = ctx.local_evidence.clone();
        let supported = |status: Status, why: &str| {
            if evidence.is_empty() {
                Assessment::unverified(EvidenceScope::LocalReference, format!("{why}; no local acceptance run evidence was supplied with this assessment"))
            } else {
                Assessment { status, scope: EvidenceScope::LocalReference, evidence: evidence.clone(), limitations: vec![format!("{why}. Local reference evidence only; not a production or external-target claim.")] }
            }
        };
        match (role, capability) {
            (Role::Compute, c) if LOCAL_CAPABILITIES.contains(&c) => supported(Status::SupportedNative, "Executed by the canonical exact evaluator over bounded in-memory inputs"),
            (Role::Catalog, "core.source_read") => supported(Status::SupportedNative, "Reads read-only acceptance-fixture sources only"),
            (Role::OfflineStore, "core.atomic_publication") => supported(Status::SupportedEmulated, "JSON Lines snapshot file replaced by atomic rename; single process, no concurrent writers"),
            (Role::OfflineStore, "core.serving_replay") => supported(Status::SupportedNative, "Replay selection is performed by the core over stored publication evidence"),
            (_, c) if c.starts_with("learned.") || c.starts_with("custom.") => Assessment::unsupported("The local reference never loads artifacts or executes custom code"),
            (_, "core.label_construction") => Assessment::unsupported("Label dataset construction is exposed as a core function, not as a deployment execution path"),
            (r, c) => Assessment::unsupported(format!("{c} is not provided for role {}", r.as_str())),
        }
    }
    fn implementation_sha256(&self) -> Option<String> {
        executable_sha256()
    }
    fn runtime_versions(&self) -> Map<String, Value> {
        runtime()
    }
    fn check_bindings(&self, p: &Project, d: &Deployment, adapter_id: &str) -> Vec<Diagnostic> {
        let di = p.deployments.iter().position(|x| x.id == d.id).unwrap_or(0);
        let mut out = Vec::new();
        for (k, b) in d.source_bindings.iter().enumerate() {
            if b.adapter == adapter_id && !matches!(b.location, Location::Fixture { .. }) {
                out.push(Diagnostic::new("LOCAL_SOURCE_UNSUPPORTED", Stage::Planning, "SEM-PORT-003", format!("/deployments/{di}/source_bindings/{k}/location"), "The local reference catalog reads acceptance-fixture sources only"));
            }
        }
        for (k, t) in d.output_targets.iter().enumerate() {
            if t.adapter == adapter_id {
                match &t.location {
                    Location::File { format, .. } if format == "jsonl" => {}
                    other => out.push(Diagnostic::new(
                        "LOCAL_OUTPUT_UNSUPPORTED",
                        Stage::Planning,
                        "SEM-PORT-004",
                        format!("/deployments/{di}/output_targets/{k}/location"),
                        format!("The local reference store writes JSON Lines files only (format jsonl); {} is unsupported", other.address()),
                    )),
                }
            }
        }
        out
    }
}

pub struct MemoryStoreAdapter;

impl AdapterDescriptor for MemoryStoreAdapter {
    fn implementation(&self) -> &str {
        MEMORY_IMPLEMENTATION
    }
    fn version(&self) -> &str {
        VERSION
    }
    fn roles(&self) -> &[Role] {
        &[Role::OfflineStore]
    }
    fn configuration_schema(&self) -> Value {
        empty_config_schema("urn:featlas:adapter:dev.featlas:memory_store:0.1.0:configuration")
    }
    fn external(&self) -> bool {
        false
    }
    fn assess(&self, role: Role, capability: &str, ctx: &AssessmentContext) -> Assessment {
        match (role, capability) {
            (Role::OfflineStore, "core.atomic_publication" | "core.serving_replay") if !ctx.local_evidence.is_empty() => Assessment {
                status: Status::SupportedEmulated,
                scope: EvidenceScope::LocalMock,
                evidence: ctx.local_evidence.clone(),
                limitations: vec!["In-process test double: proves mapping/rebinding boundaries only; not a production store or a migration".into()],
            },
            (Role::OfflineStore, "core.atomic_publication" | "core.serving_replay") => Assessment::unverified(EvidenceScope::LocalMock, "In-process test double; no rebinding evidence supplied"),
            (r, c) => Assessment::unsupported(format!("{c} is not provided for role {}", r.as_str())),
        }
    }
    fn implementation_sha256(&self) -> Option<String> {
        executable_sha256()
    }
    fn runtime_versions(&self) -> Map<String, Value> {
        runtime()
    }
    fn check_bindings(&self, p: &Project, d: &Deployment, adapter_id: &str) -> Vec<Diagnostic> {
        let di = p.deployments.iter().position(|x| x.id == d.id).unwrap_or(0);
        d.output_targets
            .iter()
            .enumerate()
            .filter(|(_, t)| t.adapter == adapter_id && !matches!(t.location, Location::Resource { .. }))
            .map(|(k, _)| Diagnostic::new("MEMORY_STORE_LOCATION", Stage::Planning, "SEM-PORT-004", format!("/deployments/{di}/output_targets/{k}/location"), "The memory store addresses records by opaque resource URI"))
            .collect()
    }
}

/// In-process store used by both local adapters in tests and the CLI.
pub struct InMemoryStore {
    data: Mutex<HashMap<String, Vec<Map<String, Value>>>>,
    clock: Box<dyn Fn() -> String + Send + Sync>,
    counter: Mutex<u64>,
}

impl InMemoryStore {
    pub fn new(clock: Box<dyn Fn() -> String + Send + Sync>) -> Self {
        InMemoryStore { data: Mutex::new(HashMap::new()), clock, counter: Mutex::new(0) }
    }
    pub fn dump(&self, location: &Location) -> Vec<Map<String, Value>> {
        self.data.lock().unwrap().get(&location.address()).cloned().unwrap_or_default()
    }
    /// Record observed visibility evidence for one publication (never fabricated).
    pub fn record_published_at(&self, location: &Location, publication_column: &str, publication_id: &str, published_column: &str, instant: &str) {
        if let Some(rows) = self.data.lock().unwrap().get_mut(&location.address()) {
            for r in rows.iter_mut() {
                if r.get(publication_column).and_then(Value::as_str) == Some(publication_id) && r.get(published_column).is_none_or(Value::is_null) {
                    r.insert(published_column.into(), Value::String(instant.into()));
                }
            }
        }
    }
    /// Import physical rows verbatim (migration of an observed ledger).
    pub fn import(&self, location: &Location, rows: Vec<Map<String, Value>>) {
        self.data.lock().unwrap().entry(location.address()).or_default().extend(rows);
    }
}

fn matches_keys(row: &Map<String, Value>, req: &PhysicalReadRequest) -> bool {
    req.keys.iter().any(|k| req.key_columns.iter().zip(k).all(|(c, v)| row.get(c) == Some(v)))
}

impl PublicationStore for InMemoryStore {
    fn read(&self, _adapter: &AdapterInstance, req: &PhysicalReadRequest) -> Result<Vec<Map<String, Value>>, EvalError> {
        let max = featlas_core::time::Instant::parse(&req.max_as_of).map_err(|m| EvalError::new("INVALID_TIMESTAMP", "SEM-TIME-003", m))?;
        Ok(self
            .dump(&req.location)
            .into_iter()
            .filter(|r| matches_keys(r, req))
            .filter(|r| r.get(&req.as_of_column).and_then(Value::as_str).and_then(|s| featlas_core::time::Instant::parse(s).ok()).is_some_and(|t| t <= max))
            .collect())
    }
    fn write(&self, _adapter: &AdapterInstance, location: &Location, rows: Vec<Map<String, Value>>) -> Result<StoreCommit, EvalError> {
        let n = rows.len();
        let mut counter = self.counter.lock().unwrap();
        *counter += 1;
        // One lock acquisition makes the whole snapshot visible at once.
        self.data.lock().unwrap().entry(location.address()).or_default().extend(rows);
        Ok(StoreCommit { committed_at: (self.clock)(), commit_id: format!("memory-commit-{}", *counter), rows: n })
    }
}

/// JSON Lines file store with atomic replace for the local reference adapter.
pub struct JsonlFileStore {
    pub clock: Box<dyn Fn() -> String + Send + Sync>,
}

impl JsonlFileStore {
    fn path(location: &Location) -> Result<std::path::PathBuf, EvalError> {
        match location {
            Location::File { path, format } if format == "jsonl" => Ok(path.into()),
            _ => Err(EvalError::new("LOCAL_OUTPUT_UNSUPPORTED", "SEM-PORT-004", "JSON Lines file locations only")),
        }
    }
    fn load(path: &std::path::Path) -> Result<Vec<Map<String, Value>>, EvalError> {
        if !path.exists() {
            return Ok(vec![]);
        }
        let text = std::fs::read_to_string(path).map_err(|e| EvalError::new("STORE_IO", "SEM-DEPLOY-003", e.to_string()))?;
        text.lines().filter(|l| !l.trim().is_empty()).map(|l| serde_json::from_str(l).map_err(|e| EvalError::new("STORE_CORRUPT", "SEM-DEPLOY-003", e.to_string()))).collect()
    }
}

impl PublicationStore for JsonlFileStore {
    fn read(&self, _adapter: &AdapterInstance, req: &PhysicalReadRequest) -> Result<Vec<Map<String, Value>>, EvalError> {
        Ok(Self::load(&Self::path(&req.location)?)?.into_iter().filter(|r| matches_keys(r, req)).collect())
    }
    fn write(&self, _adapter: &AdapterInstance, location: &Location, rows: Vec<Map<String, Value>>) -> Result<StoreCommit, EvalError> {
        let path = Self::path(location)?;
        let mut existing = Self::load(&path)?;
        let n = rows.len();
        existing.extend(rows);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| EvalError::new("STORE_IO", "SEM-DEPLOY-003", e.to_string()))?;
        }
        let tmp = path.with_extension("jsonl.tmp");
        {
            let mut f = std::fs::File::create(&tmp).map_err(|e| EvalError::new("STORE_IO", "SEM-DEPLOY-003", e.to_string()))?;
            for r in &existing {
                writeln!(f, "{}", serde_json::to_string(r).unwrap()).map_err(|e| EvalError::new("STORE_IO", "SEM-DEPLOY-003", e.to_string()))?;
            }
            f.sync_all().map_err(|e| EvalError::new("STORE_IO", "SEM-DEPLOY-003", e.to_string()))?;
        }
        std::fs::rename(&tmp, &path).map_err(|e| EvalError::new("STORE_IO", "SEM-DEPLOY-003", e.to_string()))?;
        Ok(StoreCommit { committed_at: (self.clock)(), commit_id: format!("rename:{}", path.display()), rows: n })
    }
}
