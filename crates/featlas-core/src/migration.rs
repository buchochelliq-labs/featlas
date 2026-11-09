//! Store-migration planning between two deployments of one project
//! (SEM-PORT-008/009/010, docs/MIGRATION.md).
//!
//! The plan is an artifact for review. It performs no transfer, backfill or
//! cutover and never claims that one happened.

use crate::canon::{deployment_identity, logical_identities};
use crate::capability::{plan_deployment, AdapterRegistry, AssessmentContext};
use crate::model::*;
use crate::validate::feature_sources;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Serialize)]
pub struct FieldChange {
    pub logical: String,
    pub kind: &'static str,
    pub from: String,
    pub to: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct MigrationPlan {
    pub plan_version: &'static str,
    pub identity: Value,
    pub consumers: Vec<Value>,
    pub capabilities: Value,
    pub data: Vec<Value>,
    pub mapping: Vec<Value>,
    pub publication_history: Value,
    pub execution: Value,
    pub verification: Value,
    pub cutover: Value,
    pub semantic_review: Vec<String>,
    pub blockers: Vec<String>,
    pub limitations: Vec<String>,
}

fn target_map(t: &OutputTarget, entity: &Entity) -> Vec<(String, &'static str, String)> {
    let mut v = Vec::new();
    for k in &entity.keys {
        v.push((k.name.clone(), "entity_key", t.entity_key_mapping.get(&k.name).and_then(Value::as_str).unwrap_or("").to_string()));
    }
    for f in &t.feature_mapping {
        v.push((format!("{}@{}", f.feature, f.version), "value", f.column.clone()));
        v.push((format!("{}@{}", f.feature, f.version), "state", f.state_column.clone()));
    }
    let m = &t.metadata_columns;
    for (name, col) in [("as_of", &m.as_of), ("published_at", &m.published_at), ("publication_sequence", &m.publication_sequence), ("publication_id", &m.publication_id), ("materialization_id", &m.materialization_id)] {
        v.push((name.to_string(), "publication_metadata", col.clone()));
    }
    v
}

pub fn migration_plan(p: &Project, file_sha256: &str, from: &str, to: &str, registry: &dyn AdapterRegistry) -> Result<MigrationPlan, String> {
    let a = p.deployment(from).ok_or(format!("Unknown deployment {from}"))?;
    let b = p.deployment(to).ok_or(format!("Unknown deployment {to}"))?;
    let ids = logical_identities(p);
    let mut blockers = Vec::new();
    let mut review = Vec::new();
    let pin = |d: &Deployment| d.adapters.iter().map(|x| json!({"id": x.id, "implementation": x.implementation, "version": x.version, "roles": x.roles})).collect::<Vec<_>>();
    let identity = json!({
        "project": {"id": p.metadata.id, "version": p.metadata.version, "file_sha256": file_sha256, "logical_digest": ids.logical_project},
        "source": {"deployment": a.id, "version": a.version, "identity": deployment_identity(p, from), "adapters": pin(a)},
        "destination": {"deployment": b.id, "version": b.version, "identity": deployment_identity(p, to), "adapters": pin(b)},
        "logical_pins_preserved": "Both deployments belong to one project; logical entity/source/relationship/feature/set/consumer identities and fingerprints are shared by construction.",
        "feature_set_fingerprints": ids.feature_sets,
    });
    // Consumers.
    let mut consumers = Vec::new();
    for cb in &a.consumer_bindings {
        let dest = b.consumer_bindings.iter().find(|x| x.consumer == cb.consumer && x.version == cb.version);
        let c = p.consumer(&cb.consumer);
        if dest.is_none() {
            blockers.push(format!("Consumer {}@{} is bound in {from} but not in {to}", cb.consumer, cb.version));
        }
        consumers.push(json!({
            "consumer": cb.consumer, "version": cb.version,
            "selection_mode": c.map(|c| c.selection.mode),
            "max_staleness_seconds": c.map(|c| c.max_staleness_seconds),
            "missing_value_policy": c.map(|c| c.missing_value_policy),
            "current_route": {"adapter": cb.adapter, "role": cb.role, "surface": a.publication.surface},
            "destination_route": dest.map(|d| json!({"adapter": d.adapter, "role": d.role, "surface": b.publication.surface})),
        }));
    }
    if a.publication.surface != b.publication.surface {
        review.push(format!("Publication surface changes from {} to {}; consumers need separate acceptance on the new surface", a.publication.surface.as_str(), b.publication.surface.as_str()));
    }
    // Capabilities of the destination.
    let ctx = AssessmentContext::default();
    let dest = plan_deployment(p, file_sha256, to, registry, &ctx);
    let src = plan_deployment(p, file_sha256, from, registry, &ctx);
    for c in &dest.checks {
        if !c.status.supported() {
            blockers.push(format!("Destination {}/{}/{} is {} ({})", c.adapter, c.role.as_str(), c.capability, c.status.as_str(), c.evidence_scope.as_str()));
        }
    }
    for d in dest.diagnostics.iter().filter(|d| d.is_error()) {
        blockers.push(format!("{}: {}", d.code, d.message));
    }
    let capabilities = json!({
        "source": src.checks.iter().map(|c| json!({"adapter": c.adapter, "role": c.role, "capability": c.capability, "status": c.status, "evidence_scope": c.evidence_scope})).collect::<Vec<_>>(),
        "destination": dest.checks.iter().map(|c| json!({"adapter": c.adapter, "role": c.role, "capability": c.capability, "status": c.status, "evidence_scope": c.evidence_scope, "limitations": c.limitations})).collect::<Vec<_>>(),
        "destination_execution_ready": dest.execution_ready,
    });
    // Data.
    let mut sources = BTreeSet::new();
    for t in &b.output_targets {
        if let Some(fs) = p.feature_set(&t.feature_set) {
            for m in &fs.features {
                sources.extend(feature_sources(p, &m.id));
            }
        }
    }
    let data = sources
        .iter()
        .filter_map(|s| p.source(s))
        .map(|s| {
            let ba = a.source_bindings.iter().find(|x| x.source == s.id);
            let bb = b.source_bindings.iter().find(|x| x.source == s.id);
            json!({
                "source": s.id, "version": s.version,
                "earliest_reproducible_at": s.retention.earliest_reproducible_at,
                "revision_history_seconds": s.retention.revision_history_seconds,
                "raw_seconds": s.retention.raw_seconds,
                "source_location": ba.map(|x| x.location.address()), "destination_location": bb.map(|x| x.location.address()),
                "binding_changed": ba.map(|x| (&x.location, &x.column_mapping, &x.adapter)) != bb.map(|x| (&x.location, &x.column_mapping, &x.adapter)),
                "obligation": "Snapshot identity, retained revisions/deletions and coverage evidence must be pinned before any transfer or backfill; a binding change cannot recover overwritten revisions.",
            })
        })
        .collect();
    // Mapping in both directions.
    let mut mapping = Vec::new();
    for tb in &b.output_targets {
        let Some(fs) = p.feature_set(&tb.feature_set) else { continue };
        let Some(e) = p.entity(&fs.entity) else { continue };
        let ta = a.output_targets.iter().find(|x| x.feature_set == tb.feature_set && x.version == tb.version && x.role == tb.role);
        let new = target_map(tb, e);
        let old = ta.map(|t| target_map(t, e)).unwrap_or_default();
        let changes: Vec<FieldChange> = new
            .iter()
            .filter_map(|(l, kind, to)| {
                let from = old.iter().find(|(ol, ok, _)| ol == l && ok == kind).map(|x| x.2.clone()).unwrap_or_default();
                (from != *to).then(|| FieldChange { logical: l.clone(), kind, from, to: to.clone() })
            })
            .collect();
        if ta.is_none() {
            blockers.push(format!("{}@{} has no {} target in {from} to migrate from", tb.feature_set, tb.version, tb.role.as_str()));
        }
        mapping.push(json!({
            "feature_set": tb.feature_set, "version": tb.version, "role": tb.role,
            "source_resource": ta.map(|t| json!({"adapter": t.adapter, "location": t.location})),
            "destination_resource": {"adapter": tb.adapter, "location": tb.location},
            "fields": new.iter().map(|(l, k, to)| json!({"logical": l, "kind": k, "source_field": old.iter().find(|(ol, ok, _)| ol == l && ok == k).map(|x| x.2.clone()), "destination_field": to})).collect::<Vec<_>>(),
            "renamed": changes,
            "lossless_rules": "Rename only: entity keys, values, states and all five publication metadata fields map to distinct fields in both directions; precision (microsecond UTC, BIGINT, binary64) and explicit null/state encodings must be verified on the destination backend.",
        }));
    }
    if b.output_retention_seconds < a.output_retention_seconds {
        review.push(format!("Destination output retention {}s is shorter than the source's {}s; replay over the difference becomes unsupported", b.output_retention_seconds, a.output_retention_seconds));
    }
    if a.publication.atomicity != b.publication.atomicity || a.publication.visibility != b.publication.visibility {
        review.push("Publication atomicity/visibility semantics differ".into());
    }
    let publication_history = json!({
        "origin": {"deployment": a.id, "version": a.version, "surface": a.publication.surface},
        "preserve": ["published_at", "publication_sequence", "publication_id"],
        "assign_new": ["materialization_id"],
        "record_separately": "Destination import/visibility observations (commit instants) go to the migration manifest; they never overwrite original published_at.",
        "replay_continuity": "Historical-serving replay after cutover is UNVERIFIED until the copied ledger is shown complete for the retained interval and continuity of the logical service and surface is established.",
        "backfill_rule": "A fresh backfill yields recomputed values with new publication identities; it never manufactures a historical served-value ledger.",
    });
    let execution = json!({
        "transfer_namespace": "A separate destination namespace/resource for transferred and backfilled data until cutover",
        "destination_backfill": b.backfill,
        "idempotency_key": ["publication_id", "materialization_id", "entity keys", "as_of"],
        "budget": b.budget,
        "concurrency": b.budget.max_concurrent_runs,
        "failure_recovery": "Re-running a transfer with the same materialization_id must not duplicate rows; interrupted imports are discarded or resumed by idempotency key before verification.",
    });
    let suites: Vec<Value> = p
        .acceptance_suites
        .iter()
        .filter(|s| s.cases.iter().any(|c| b.output_targets.iter().filter_map(|t| p.feature_set(&t.feature_set)).any(|fs| fs.features.contains(&c.feature))))
        .map(|s| json!({"suite": s.id, "version": s.version, "runner": s.runner, "cases": s.cases.len()}))
        .collect();
    let verification = json!({
        "golden_suites": suites,
        "compare": ["complete entity keys", "row counts", "values (declared tolerances only)", "states", "as_of/published_at/sequence/publication_id", "coverage", "lineage"],
        "shadow": "Dual-read real consumer requests through both routes via the target-neutral consumer API and compare responses including provenance.",
        "fault_injection": ["interrupted write", "retry with same materialization_id", "duplicate submission", "concurrent run", "late correction", "retention expiry"],
    });
    let cutover = json!({
        "preconditions": ["All destination capabilities supported with live evidence", "Golden and historical comparisons reviewed", "Consumer dual-read parity met", "Authorization for routing change recorded"],
        "steps": ["Freeze writes or establish a synchronization point", "Final incremental transfer", "Switch consumer bindings to the destination deployment", "Observe end-to-end reads"],
        "rollback": ["Consumer parity or freshness regressions", "Missing publication evidence", "Unexpected states/nulls"],
        "rollback_action": format!("Restore consumer bindings to {from}; the source resources are retained unchanged"),
        "retirement": format!("Retire source resources only after output retention ({}s) and the recovery window elapse", a.output_retention_seconds),
    });
    let limitations = vec![
        "This is a plan. No data was transferred, backfilled, compared on live stores or cut over.".to_string(),
        "A local alternate-store round trip establishes boundary separation only, not a migration (SEM-PORT-010).".to_string(),
    ];
    Ok(MigrationPlan { plan_version: "featlas.migration-plan/v1", identity, consumers, capabilities, data, mapping, publication_history, execution, verification, cutover, semantic_review: review, blockers, limitations })
}
