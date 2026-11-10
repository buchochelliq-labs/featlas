//! `dev.featlas:feast@0.1.0` — Feast (open-source feature store) as an offline and online store.
//!
//! Feast stores and retrieves feature values; it does not compute them. A Feast deployment
//! computes snapshots with another compute adapter (the Spark SQL lowering of
//! `dev.featlas:databricks`, run on Apache Spark or Databricks) and the Python host
//! `featlas_feast` publishes them:
//!
//! * offline_store: one parquet file per publication in the target directory, renamed into
//!   place (a reader sees all of a publication or none of it); `published_at` is the instant the
//!   host first reads the publication back through Feast, kept in `_publications.jsonl` beside the
//!   data. Feast's point-in-time join (latest `as_of` <= request time, within the feature view
//!   TTL = the consumer's `max_staleness_seconds`) serves source-as-known reads. Feast cannot
//!   filter by publication visibility, so historical serving replay is not provided.
//! * online_store: `materialize` into the configured online store (SQLite locally); visibility
//!   is observed with `get_online_features` on the published keys. Feast writes entity rows in
//!   batches, so a reader can see a partly updated snapshot: atomic online publication is
//!   reported unverified with that limitation.

use featlas_core::capability::{AdapterDescriptor, Assessment, AssessmentContext, EvidenceScope, Status};
use featlas_core::diag::{Diagnostic, Stage};
use featlas_core::model::*;
use serde_json::{json, Map, Value};

pub const IMPLEMENTATION: &str = "dev.featlas:feast";
pub const VERSION: &str = "0.1.0";

pub struct Feast;

impl AdapterDescriptor for Feast {
    fn implementation(&self) -> &str {
        IMPLEMENTATION
    }
    fn version(&self) -> &str {
        VERSION
    }
    fn roles(&self) -> &[Role] {
        &[Role::OfflineStore, Role::OnlineStore]
    }
    fn configuration_schema(&self) -> Value {
        json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "urn:featlas:adapter:dev.featlas:feast:0.1.0:configuration",
            "type": "object", "additionalProperties": false, "required": ["project"],
            "properties": {
                "project": {"type": "string", "pattern": "^[a-z][a-z0-9_]{0,62}$", "description": "Feast project name."},
                "online_store": {"enum": ["sqlite"], "description": "Online store type the host configures (SQLite for local use)."}
            }
        })
    }
    fn external(&self) -> bool {
        false
    }
    fn assess(&self, role: Role, capability: &str, ctx: &AssessmentContext) -> Assessment {
        let ev = ctx.local_evidence.clone();
        let local = |status: Status, why: &str| {
            if ev.is_empty() {
                Assessment::unverified(EvidenceScope::LocalReference, format!("{why}; no local run evidence was supplied with this assessment"))
            } else {
                Assessment { status, scope: EvidenceScope::LocalReference, evidence: ev.clone(), limitations: vec![format!("{why}. Local Feast evidence (file offline store, SQLite online store); not a production deployment.")] }
            }
        };
        match (role, capability) {
            (Role::OfflineStore, "core.atomic_publication") => local(Status::SupportedNative, "One parquet file per publication renamed into the feature view's source directory; published_at observed by reading the publication back through Feast"),
            (Role::OnlineStore, "core.online_publication") => local(Status::SupportedNative, "materialize into the online store; published_at observed with get_online_features on the published keys"),
            (Role::OnlineStore, "core.atomic_publication") => Assessment::unverified(EvidenceScope::LocalReference, "Feast writes online entity rows in batches: during materialization a reader can see part of a snapshot"),
            (_, "core.serving_replay") => Assessment::unsupported("Feast's point-in-time join selects by feature timestamp and TTL; it cannot restrict to publications visible at a past request time"),
            (r, c) => Assessment::unsupported(format!("{c} is not provided for role {}", r.as_str())),
        }
    }
    fn implementation_sha256(&self) -> Option<String> {
        crate::local::executable_sha256()
    }
    fn runtime_versions(&self) -> Map<String, Value> {
        Map::new()
    }
    fn check_bindings(&self, p: &Project, d: &Deployment, adapter_id: &str) -> Vec<Diagnostic> {
        let di = p.deployments.iter().position(|x| x.id == d.id).unwrap_or(0);
        d.output_targets
            .iter()
            .enumerate()
            .filter(|(_, t)| t.adapter == adapter_id && !matches!(&t.location, Location::File { format, .. } if format == "parquet"))
            .map(|(k, _)| Diagnostic::new("FEAST_LOCATION", Stage::Planning, "SEM-PORT-004", format!("/deployments/{di}/output_targets/{k}/location"), "Feast targets are parquet directories (location kind file, format parquet)"))
            .collect()
    }
}
