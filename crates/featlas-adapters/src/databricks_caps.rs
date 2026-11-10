//! Capability assessment for `dev.featlas:databricks`, driven by the adapter lock.
//!
//! The lock (`databricks_lock.json`) is written only from observed live runs. A run vouches for
//! a (role, capability) only while the adapter's lowering source and the Python host runner are
//! byte-identical to the files it tested (`sources_sha256`); otherwise the capability falls back
//! to `unverified` with a limitation naming the run that would need repeating. Assessment logic
//! lives here, outside `databricks.rs`, so changing it does not invalidate the lowering pin.

use featlas_core::capability::{Assessment, AssessmentContext, EvidenceScope, Status};
use featlas_core::model::{ReportEvidence, Role};
use serde::Deserialize;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::OnceLock;

use crate::databricks::IMPLEMENTATION;

const LOCK: &str = include_str!("databricks_lock.json");

/// The files a live run vouches for, as compiled into this binary.
const SOURCES: &[(&str, &[u8])] = &[
    (
        "crates/featlas-adapters/src/databricks.rs",
        include_bytes!("databricks.rs"),
    ),
    (
        "python/featlas_databricks/runner.py",
        include_bytes!("../../../python/featlas_databricks/runner.py"),
    ),
];

/// Capabilities for which this version contains a lowering/statement generator.
pub const COMPUTE_LOWERED: &[&str] = &[
    "core.aggregate",
    "core.recency",
    "core.lookup",
    "core.expression",
    "core.revision_reconstruction",
    "core.as_of_join",
    "core.relationship_join",
    "core.source_as_known",
    "core.label_construction",
];

#[derive(Debug, Deserialize)]
pub struct Lock {
    pub adapter: String,
    pub adapter_version: String,
    pub verified_runs: Vec<Run>,
}

#[derive(Debug, Deserialize)]
pub struct Run {
    pub run_id: String,
    pub run_url: String,
    pub artifact_sha256: String,
    pub tested_commit: String,
    pub target: BTreeMap<String, Value>,
    pub client: BTreeMap<String, String>,
    pub sources_sha256: BTreeMap<String, String>,
    pub evidence: BTreeMap<String, EvidenceFile>,
    pub verified: Vec<Verified>,
}

#[derive(Debug, Deserialize)]
pub struct EvidenceFile {
    pub path: String,
    pub sha256: String,
    pub summary: String,
}

#[derive(Debug, Deserialize)]
pub struct Verified {
    pub role: Role,
    pub capability: String,
    pub evidence: String,
}

pub fn lock() -> &'static Lock {
    static L: OnceLock<Lock> = OnceLock::new();
    L.get_or_init(|| serde_json::from_str(LOCK).expect("databricks_lock.json is valid"))
}

/// SHA-256 of each pinned source file as compiled into this binary.
pub fn current_sources() -> BTreeMap<String, String> {
    SOURCES
        .iter()
        .map(|(p, b)| (p.to_string(), hex::encode(Sha256::digest(b))))
        .collect()
}

impl Run {
    /// Whether this run tested exactly the compiled sources.
    pub fn matches_current(&self) -> bool {
        let cur = current_sources();
        cur.len() == self.sources_sha256.len()
            && cur
                .iter()
                .all(|(p, h)| self.sources_sha256.get(p) == Some(h))
    }
    fn target_label(&self) -> String {
        let s = |k: &str| self.target.get(k).and_then(Value::as_str).unwrap_or("?");
        format!(
            "{} ({}, {} compute), Databricks Runtime {}, Spark {}",
            s("product"),
            s("cloud"),
            s("compute"),
            s("dbr_version"),
            s("spark_version")
        )
    }
}

/// Digest identifying the compiled implementation: SHA-256 over `path:sha256` lines of every
/// vouched-for source, in path order.
pub fn implementation_sha256() -> String {
    let lines: String = current_sources().iter().map(|(p, h)| format!("{p}:{h}\n")).collect();
    hex::encode(Sha256::digest(lines.as_bytes()))
}

/// The newest run that tested the compiled sources, if any.
pub fn current_run() -> Option<&'static Run> {
    let l = lock();
    if l.adapter != IMPLEMENTATION || l.adapter_version != crate::databricks::VERSION {
        return None;
    }
    l.verified_runs.iter().rev().find(|r| r.matches_current())
}

pub fn runtime_versions() -> Map<String, Value> {
    let mut m = Map::new();
    if let Some(r) = current_run() {
        for (k, v) in &r.target {
            // The report schema requires string values.
            let v = v.as_str().map(str::to_string).unwrap_or_else(|| v.to_string());
            m.insert(format!("target.{k}"), Value::String(v));
        }
        for (k, v) in &r.client {
            m.insert(format!("client.{k}"), Value::String(v.clone()));
        }
        m.insert("lock.run_id".into(), Value::String(r.run_id.clone()));
    }
    m
}

fn live(role: Role, capability: &str) -> Option<Assessment> {
    let l = lock();
    let verifying = |r: &&Run| {
        r.verified
            .iter()
            .any(|v| v.role == role && v.capability == capability)
    };
    if let Some(r) = current_run().filter(verifying) {
        let v = r
            .verified
            .iter()
            .find(|v| v.role == role && v.capability == capability)?;
        let ev = r.evidence.get(&v.evidence)?;
        return Some(Assessment {
            status: Status::SupportedNative,
            scope: EvidenceScope::LiveTarget,
            evidence: vec![ReportEvidence {
                uri: format!("{}#{}", r.run_url, ev.path),
                sha256: ev.sha256.clone(),
                description: format!(
                    "Live run {} on {} (tested commit {}, artifact sha256 {}): {}",
                    r.run_id,
                    r.target_label(),
                    &r.tested_commit[..12.min(r.tested_commit.len())],
                    r.artifact_sha256,
                    ev.summary
                ),
            }],
            limitations: vec![format!(
                "Observed only on {}; Azure Databricks and classic clusters were not observed. Evidence is pinned to adapter source sha256 {} and host runner sha256 {}.",
                r.target_label(),
                r.sources_sha256.get(SOURCES[0].0).map(String::as_str).unwrap_or("?"),
                r.sources_sha256.get(SOURCES[1].0).map(String::as_str).unwrap_or("?"),
            )],
        });
    }
    // A run verified it, but for other source bytes: say exactly what would need repeating.
    l.verified_runs.iter().rev().find(verifying).map(|r| {
        Assessment::unverified(
            EvidenceScope::Static,
            format!(
                "Live run {} verified this on {}, but the adapter lowering or host runner has changed since (tested sources: {:?}; compiled: {:?}). Re-run .github/workflows/databricks-live.yml and pin the new run in databricks_lock.json.",
                r.run_id,
                r.target_label(),
                r.sources_sha256,
                current_sources()
            ),
        )
    })
}

pub fn assess(role: Role, capability: &str, ctx: &AssessmentContext) -> Assessment {
    if let Some((_, _, _, ev)) = ctx
        .live_evidence
        .iter()
        .find(|(a, r, c, _)| a == IMPLEMENTATION && *r == role && c == capability)
    {
        // Operator-supplied evidence is recorded, but only the reviewed lock can make it supported.
        return Assessment {
            status: Status::Unverified,
            scope: EvidenceScope::LiveTarget,
            evidence: vec![ev.clone()],
            limitations: vec!["Live evidence reference supplied by the operator; it stays unverified until a reviewed run is pinned in databricks_lock.json".into()],
        };
    }
    if let Some(a) = live(role, capability) {
        return a;
    }
    let none_live = "No live Databricks run in databricks_lock.json covers this capability for the compiled adapter";
    match (role, capability) {
        (Role::Compute, c) if COMPUTE_LOWERED.contains(&c) => Assessment::unverified(EvidenceScope::Static, format!("Spark SQL lowering is generated and differential-tested on local Apache Spark. {none_live}.")),
        (Role::Catalog, "core.source_read") => Assessment::unverified(EvidenceScope::Static, format!("Unity Catalog reads are generated as SELECT statements. {none_live}.")),
        (Role::OfflineStore, "core.atomic_publication") => Assessment::unverified(EvidenceScope::Static, format!("Single-statement Delta MERGE per feature-set snapshot plus change-data-feed commit evidence is generated. {none_live}.")),
        (Role::OfflineStore, "core.serving_replay") => Assessment::unverified(EvidenceScope::Static, format!("Replay reads require persisted published_at evidence. {none_live}.")),
        (Role::Orchestrator, "core.scheduling") => Assessment::unverified(EvidenceScope::Static, format!("A paused Databricks Asset Bundle job (databricks/job.yml: UTC Quartz schedule on the deployment grid, slot-derived idempotent publication identity, retries, max_concurrent_runs and timeout from budget/SLO) is generated; deploying and running it has not been observed. {none_live}.")),
        (Role::OnlineStore, _) => Assessment::unsupported("This adapter version provides no online_store role"),
        (_, c) if c.starts_with("learned.") || c.starts_with("custom.") => Assessment::unsupported(format!("{c} has no attested implementation in this adapter; custom/learned execution is not supported")),
        (_, "core.online_publication") => Assessment::unsupported("Online publication is not implemented"),
        (r, c) => Assessment::unsupported(format!("{c} is not provided for role {}", r.as_str())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lock_is_consistent() {
        let l = lock();
        assert_eq!(l.adapter, IMPLEMENTATION);
        for r in &l.verified_runs {
            assert_eq!(
                r.sources_sha256.keys().collect::<Vec<_>>(),
                SOURCES.iter().map(|(p, _)| p).collect::<Vec<_>>(),
                "run {} pins exactly the vouched-for sources",
                r.run_id
            );
            for v in &r.verified {
                assert!(
                    r.evidence.contains_key(&v.evidence),
                    "{} names missing evidence {}",
                    v.capability,
                    v.evidence
                );
            }
        }
    }

    #[test]
    fn evidence_files_match_the_lock() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        for r in &lock().verified_runs {
            for ev in r.evidence.values() {
                let bytes = std::fs::read(root.join(&ev.path))
                    .unwrap_or_else(|e| panic!("{}: {e}", ev.path));
                assert_eq!(hex::encode(Sha256::digest(bytes)), ev.sha256, "{}", ev.path);
            }
        }
    }

    #[test]
    fn changed_sources_fall_back_to_unverified() {
        let a = assess(
            Role::Compute,
            "core.aggregate",
            &AssessmentContext::default(),
        );
        match current_run() {
            Some(_) => assert_eq!(a.status, Status::SupportedNative),
            None => {
                assert_eq!(a.status, Status::Unverified);
                assert!(
                    a.limitations[0].contains("has changed since"),
                    "{:?}",
                    a.limitations
                );
            }
        }
        // Never vouched for by any run.
        let s = assess(
            Role::Orchestrator,
            "core.scheduling",
            &AssessmentContext::default(),
        );
        assert_ne!(s.status, Status::SupportedNative);
    }
}
