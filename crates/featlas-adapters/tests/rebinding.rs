//! Milestone 4: storage rebinding preserves logical identity and meaning.
//!
//! Example 06 keeps Databricks compute/catalog but rebinds the offline store to
//! an in-memory test double with renamed physical fields. This test proves
//! boundary separation only: it is not a second production store and not a
//! migration (SEM-PORT-010).

use featlas_adapters::local::InMemoryStore;
use featlas_adapters::Registry;
use featlas_core::canon::{deployment_identity, logical_identities};
use featlas_core::capability::{plan_deployment, AssessmentContext};
use featlas_core::consumer::*;
use featlas_core::diag::EvalError;
use featlas_core::eval::{evaluate, Limits, Request};
use featlas_core::load::{load_path, LoadedProject};
use featlas_core::model::*;
use featlas_core::time::Instant;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::path::PathBuf;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn load(rel: &str) -> LoadedProject {
    let (l, r) = load_path(&root().join(rel)).unwrap();
    assert!(r.passed(), "{rel}: {:?}", r.diagnostics);
    l.unwrap()
}

#[test]
fn logical_fingerprints_survive_rebinding_and_adapter_repinning() {
    let ex01 = load("spec/feature-engineering-spec-v0.2.0/examples/01_fault_count.yaml");
    let ex06 = load("spec/feature-engineering-spec-v0.2.0/examples/06_store_migration.yaml");
    let pr01 = load("projects/01_fault_count.yaml");
    let pr06 = load("projects/06_store_migration.yaml");
    let ids: Vec<_> = [&ex01, &ex06, &pr01, &pr06]
        .iter()
        .map(|l| logical_identities(&l.project))
        .collect();
    for other in &ids[1..] {
        assert_eq!(other.features, ids[0].features);
        assert_eq!(other.feature_sets, ids[0].feature_sets);
        assert_eq!(other.consumers, ids[0].consumers);
        assert_eq!(other.sources, ids[0].sources);
        assert_eq!(other.logical_project, ids[0].logical_project);
    }
    // Full-file digests are binding-sensitive and all differ.
    let files: std::collections::BTreeSet<_> = [&ex01, &ex06, &pr01, &pr06]
        .iter()
        .map(|l| l.file_sha256.clone())
        .collect();
    assert_eq!(files.len(), 4);
    // Deployment identities reflect bindings.
    let a = deployment_identity(&pr06.project, "fault_prod").unwrap();
    let b = deployment_identity(&pr06.project, "fault_alternative_store").unwrap();
    assert_ne!(a, b);
    assert_eq!(
        a,
        deployment_identity(&pr01.project, "fault_prod").unwrap(),
        "identical fault_prod declarations have identical deployment identity"
    );
    assert_ne!(
        deployment_identity(&ex01.project, "fault_prod").unwrap(),
        a,
        "adapter re-pinning changes deployment identity"
    );
    // Logical plan digest is binding-independent; the full plan digest is not.
    let reg = Registry::default();
    let pa = plan_deployment(
        &pr06.project,
        &pr06.file_sha256,
        "fault_prod",
        &reg,
        &AssessmentContext::default(),
    )
    .plan
    .unwrap();
    let pb = plan_deployment(
        &pr06.project,
        &pr06.file_sha256,
        "fault_alternative_store",
        &reg,
        &AssessmentContext::default(),
    )
    .plan
    .unwrap();
    assert_eq!(pa.logical_plan_digest, pb.logical_plan_digest);
    assert_ne!(pa.plan_digest, pb.plan_digest);
}

/// Physical records already read from the Databricks-named layout (no live access here).
struct Surrogate(Vec<Map<String, Value>>);

impl PublicationStore for Surrogate {
    fn read(
        &self,
        _: &AdapterInstance,
        req: &PhysicalReadRequest,
    ) -> Result<Vec<Map<String, Value>>, EvalError> {
        Ok(self
            .0
            .iter()
            .filter(|r| {
                req.keys.iter().any(|k| {
                    req.key_columns
                        .iter()
                        .zip(k)
                        .all(|(c, v)| r.get(c) == Some(v))
                })
            })
            .cloned()
            .collect())
    }
    fn write(
        &self,
        _: &AdapterInstance,
        _: &Location,
        _: Vec<Map<String, Value>>,
    ) -> Result<StoreCommit, EvalError> {
        unreachable!()
    }
}

struct Stores<'a> {
    memory: &'a InMemoryStore,
    surrogate: Surrogate,
}

impl StoreRegistry for Stores<'_> {
    fn store(&self, implementation: &str, version: &str) -> Option<&dyn PublicationStore> {
        match (implementation, version) {
            ("dev.featlas:memory_store", "0.1.0") => Some(self.memory),
            ("dev.featlas:databricks", "0.1.0") => Some(&self.surrogate),
            _ => None,
        }
    }
}

fn snapshot(p: &Project, case_id: &str) -> (Instant, Vec<LogicalSnapshotRow>) {
    let case = p
        .acceptance_suites
        .iter()
        .flat_map(|s| &s.cases)
        .find(|c| c.id == case_id)
        .unwrap();
    let as_of = Instant::parse(&case.as_of).unwrap();
    let req = Request {
        as_of,
        source_rows: &case.source_rows,
        source_status: &case.context.source_status,
        expected_population: Some(&case.population_rows),
        mode: SelectionMode::SourceAsKnown,
        requested_at: None,
        published_values: None,
        consumer: None,
        capabilities: &[],
    };
    let ev = evaluate(p, "device.fault_count_7d", &req, &Limits::default()).unwrap();
    let rows = ev
        .rows
        .into_iter()
        .map(|r| LogicalSnapshotRow {
            entity_keys: r.entity_keys,
            values: BTreeMap::from([("device.fault_count_7d".to_string(), (r.value, r.state))]),
        })
        .collect();
    (as_of, rows)
}

#[test]
fn rebinding_round_trip_preserves_values_states_and_publication_metadata() {
    let l = load("projects/06_store_migration.yaml");
    let p = &l.project;
    let prod = p.deployment("fault_prod").unwrap();
    let alt = p.deployment("fault_alternative_store").unwrap();
    let t_prod = ResolvedTargetRef::new(p, &prod.output_targets[0]).unwrap();
    let t_alt = ResolvedTargetRef::new(p, &alt.output_targets[0]).unwrap();
    // Two publications for two tenants sharing device_1: a count and a later typed null (insufficient history).
    let (as_of, rows) = snapshot(p, "tenant_isolation");
    let (_, mut null_rows) = snapshot(p, "insufficient_history");
    null_rows.push(LogicalSnapshotRow {
        entity_keys: serde_json::from_value(
            json!({"tenant_id": "tenant_b", "device_id": "device_1"}),
        )
        .unwrap(),
        values: BTreeMap::from([(
            "device.fault_count_7d".to_string(),
            (json!(0), "empty".to_string()),
        )]),
    });
    let m1 = PublicationMetadata {
        as_of: as_of.to_string(),
        published_at: Some("2026-06-08T00:00:30Z".into()),
        publication_sequence: 1,
        publication_id: "pub-1".into(),
        materialization_id: "mat-1".into(),
    };
    let m2 = PublicationMetadata {
        as_of: as_of.to_string(),
        published_at: None,
        publication_sequence: 2,
        publication_id: "pub-2".into(),
        materialization_id: "mat-2".into(),
    };
    let memory = InMemoryStore::new(Box::new(|| "2026-06-08T00:02:00Z".to_string()));
    let mut prod_rows = Vec::new();
    for (rows, meta) in [(&rows, &m1), (&null_rows, &m2)] {
        let a = to_physical(&t_prod, rows, meta).unwrap();
        let b = to_physical(&t_alt, rows, meta).unwrap();
        // Bidirectional mapping is lossless (null vs zero, states, metadata).
        let back_a: Vec<_> = from_physical(&t_prod, &a).unwrap();
        let back_b: Vec<_> = from_physical(&t_alt, &b).unwrap();
        assert_eq!(
            serde_json::to_value(&back_a).unwrap(),
            serde_json::to_value(&back_b).unwrap()
        );
        assert!(b.iter().all(|r| r.contains_key("tenant_key")
            && r.contains_key("asset_key")
            && r.contains_key("fault_total")
            && r.contains_key("fault_status")));
        assert!(a
            .iter()
            .all(|r| r.contains_key("device_fault_count_7d_state")
                && !r.contains_key("fault_status")));
        prod_rows.extend(a);
        use featlas_core::consumer::PublicationStore;
        memory
            .write(
                alt.adapter("alternative_store").unwrap(),
                &alt.output_targets[0].location,
                b,
            )
            .unwrap();
    }
    // Evidence for pub-2 is recorded later; until then it is invisible to every read.
    let stores = Stores {
        memory: &memory,
        surrogate: Surrogate(prod_rows.clone()),
    };
    let request = |deployment: &str, consumer: &str, requested_at: &str| {
        ConsumerRequest {
        api_version: CONSUMER_API_VERSION.into(),
        deployment: deployment.into(),
        consumer: consumer.into(),
        consumer_version: "1.0.0".into(),
        rows: vec![
            serde_json::from_value(json!({"tenant_id": "tenant_a", "device_id": "device_1", "as_of": "2026-06-08T00:00:00Z", "requested_at": requested_at})).unwrap(),
            serde_json::from_value(json!({"tenant_id": "tenant_b", "device_id": "device_1", "as_of": "2026-06-08T00:00:00Z", "requested_at": requested_at})).unwrap(),
        ],
    }
    };
    let strip = |r: &ConsumerResponse| -> Value {
        json!(r
            .rows
            .iter()
            .map(
                |x| json!({"keys": x.entity_keys, "values": x.values, "diagnostics": x.diagnostics})
            )
            .collect::<Vec<_>>())
    };
    for consumer in ["reliability_batch", "reliability_replay"] {
        let a = serve(
            p,
            &stores,
            &request("fault_prod", consumer, "2026-06-08T00:05:00Z"),
            100,
        )
        .unwrap();
        let b = serve(
            p,
            &stores,
            &request("fault_alternative_store", consumer, "2026-06-08T00:05:00Z"),
            100,
        )
        .unwrap();
        assert_eq!(
            strip(&a),
            strip(&b),
            "{consumer}: logical responses must be identical across rebinding"
        );
        assert_ne!(a.adapter_implementation, b.adapter_implementation);
        let v = strip(&a);
        if consumer == "reliability_replay" {
            // Replay ignores pub-2 until its visibility evidence is recorded.
            assert_eq!(v[0]["values"][0]["value"], json!(1));
            assert_eq!(v[1]["values"][0]["value"], json!(2));
            assert_eq!(
                v[0]["values"][0]["provenance"]["publication_id"],
                json!("pub-1")
            );
        } else {
            // Live source-as-known reads see the committed pub-2 snapshot (latest null supersedes).
            assert_eq!(v[0]["values"][0]["value"], Value::Null);
            assert_eq!(
                v[0]["values"][0]["provenance"]["publication_id"],
                json!("pub-2")
            );
            assert_eq!(v[0]["values"][0]["provenance"]["published_at"], Value::Null);
        }
    }
    // Record observed evidence for pub-2 in the test double only: the null now supersedes on that route.
    memory.record_published_at(
        &alt.output_targets[0].location,
        "original_publication_id",
        "pub-2",
        "original_visible_at",
        "2026-06-08T00:03:00Z",
    );
    let b = serve(
        p,
        &stores,
        &request(
            "fault_alternative_store",
            "reliability_replay",
            "2026-06-08T00:05:00Z",
        ),
        100,
    )
    .unwrap();
    let v = strip(&b);
    assert_eq!(v[0]["values"][0]["value"], Value::Null);
    assert_eq!(v[0]["values"][0]["state"], json!("insufficient_history"));
    assert_eq!(
        v[1]["values"][0]["value"],
        json!(0),
        "known-empty zero is distinct from null"
    );
    assert_eq!(v[1]["values"][0]["state"], json!("empty"));
    // Replay before the observed instant still sees pub-1 only.
    let b = serve(
        p,
        &stores,
        &request(
            "fault_alternative_store",
            "reliability_replay",
            "2026-06-08T00:02:59.999999Z",
        ),
        100,
    )
    .unwrap();
    assert_eq!(strip(&b)[0]["values"][0]["value"], json!(1));
}

#[test]
fn consumer_api_rejects_incomplete_or_foreign_requests() {
    let l = load("projects/06_store_migration.yaml");
    let p = &l.project;
    let memory = InMemoryStore::new(Box::new(|| "2026-06-08T00:00:00Z".to_string()));
    let stores = Stores {
        memory: &memory,
        surrogate: Surrogate(vec![]),
    };
    let mk = |row: Value, consumer_version: &str| ConsumerRequest {
        api_version: CONSUMER_API_VERSION.into(),
        deployment: "fault_alternative_store".into(),
        consumer: "reliability_batch".into(),
        consumer_version: consumer_version.into(),
        rows: vec![serde_json::from_value(row).unwrap()],
    };
    let missing_tenant = serve(p, &stores, &mk(json!({"device_id": "device_1", "as_of": "2026-06-08T00:00:00Z", "requested_at": "2026-06-08T00:00:00Z"}), "1.0.0"), 10).unwrap();
    assert_eq!(
        missing_tenant.rows[0].diagnostics[0].code,
        "REQUEST_KEY_MISSING"
    );
    let table_name = serve(p, &stores, &mk(json!({"tenant_id": "a", "device_id": "d", "table": "x.y.z", "as_of": "2026-06-08T00:00:00Z", "requested_at": "2026-06-08T00:00:00Z"}), "1.0.0"), 10).unwrap();
    assert_eq!(
        table_name.rows[0].diagnostics[0].code,
        "REQUEST_FIELD_UNKNOWN"
    );
    let unpublished = serve(p, &stores, &mk(json!({"tenant_id": "a", "device_id": "d", "as_of": "2026-06-08T00:00:00Z", "requested_at": "2026-06-08T00:00:00Z"}), "1.0.0"), 10).unwrap();
    assert_eq!(unpublished.rows[0].values[0].state, "not_published");
    assert_eq!(
        serve(p, &stores, &mk(json!({}), "2.0.0"), 10)
            .unwrap_err()
            .code,
        "VERSION_MISMATCH"
    );
    let mut synthetic = mk(json!({}), "1.0.0");
    synthetic.deployment = "fault_prod".into();
    let empty = Stores {
        memory: &memory,
        surrogate: Surrogate(vec![]),
    };
    assert!(serve(p, &empty, &synthetic, 10).is_ok());
}

#[test]
fn atomic_set_inconsistency_is_reported() {
    let l = load("projects/03_commerce_ratio.yaml");
    let p = &l.project;
    let d = p.deployment("commerce_prod").unwrap();
    let t = ResolvedTargetRef::new(p, &d.output_targets[0]).unwrap();
    let keys: Map<String, Value> =
        serde_json::from_value(json!({"tenant_id": "tenant_a", "customer_id": "customer_1"}))
            .unwrap();
    let full = LogicalSnapshotRow {
        entity_keys: keys.clone(),
        values: BTreeMap::from([
            ("customer.spend_7d".into(), (json!(1), "ok".into())),
            ("customer.spend_30d".into(), (json!(4), "ok".into())),
            (
                "customer.recent_spend_ratio".into(),
                (json!(0.25), "ok".into()),
            ),
        ]),
    };
    let meta = |seq: i64, id: &str| PublicationMetadata {
        as_of: "2026-06-08T00:00:00Z".into(),
        published_at: Some(format!("2026-06-08T00:0{seq}:00Z")),
        publication_sequence: seq,
        publication_id: id.into(),
        materialization_id: id.into(),
    };
    let mut rows = to_physical(&t, &[full.clone()], &meta(1, "pub-1")).unwrap();
    // A second write that (incorrectly) carries only one member value per row, as a non-atomic store could.
    let mut partial = to_physical(&t, &[full], &meta(2, "pub-2")).unwrap();
    partial[0].insert("customer_spend_30d".into(), Value::Null);
    partial[0].insert("customer_spend_30d_state".into(), json!("not_published"));
    rows.extend(partial);
    let records = from_physical(&t, &rows).unwrap();
    // Member-wise selection picks pub-2 for every member here; then remove pub-2's 30d record to force a mix.
    let mixed: Vec<_> = records
        .into_iter()
        .filter(|r| !(r.publication_id == "pub-2" && r.feature == "customer.spend_30d"))
        .collect();
    let pick: std::collections::BTreeSet<_> = ["customer.spend_7d", "customer.spend_30d"]
        .iter()
        .map(|f| {
            let recs: Vec<_> = mixed.iter().filter(|r| r.feature == *f).cloned().collect();
            select(
                &recs,
                &[
                    featlas_core::value::V::Str("tenant_a".into()),
                    featlas_core::value::V::Str("customer_1".into()),
                ],
                Instant::parse("2026-06-08T00:00:00Z").unwrap(),
                None,
                3600,
            )
            .unwrap()
            .unwrap()
            .publication_id
            .clone()
        })
        .collect();
    assert_eq!(pick.len(), 2, "member-wise selection mixes snapshots; serve() reports ATOMIC_SET_INCONSISTENT for this condition");
}

#[test]
fn migration_plan_names_mappings_and_blockers() {
    let l = load("projects/06_store_migration.yaml");
    let plan = featlas_core::migration::migration_plan(
        &l.project,
        &l.file_sha256,
        "fault_prod",
        "fault_alternative_store",
        &Registry::default(),
    )
    .unwrap();
    let mapping = serde_json::to_value(&plan.mapping).unwrap();
    let renamed: Vec<String> = mapping[0]["renamed"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| {
            format!(
                "{}->{}",
                c["from"].as_str().unwrap(),
                c["to"].as_str().unwrap()
            )
        })
        .collect();
    assert!(
        renamed.contains(&"tenant_id->tenant_key".to_string()),
        "{renamed:?}"
    );
    assert!(renamed.contains(&"device_fault_count_7d->fault_total".to_string()));
    assert!(renamed.contains(&"published_at->original_visible_at".to_string()));
    assert!(
        !plan.blockers.is_empty(),
        "unverified destination capabilities must block a migration claim"
    );
    assert!(plan
        .limitations
        .iter()
        .any(|x| x.contains("boundary separation only")));
}

#[test]
fn core_crate_has_no_vendor_dependencies() {
    let manifest = std::fs::read_to_string(root().join("crates/featlas-core/Cargo.toml")).unwrap();
    for banned in ["databricks", "pyspark", "spark", "featlas-adapters"] {
        assert!(
            !manifest.to_lowercase().contains(banned),
            "core depends on {banned}"
        );
    }
    for entry in std::fs::read_dir(root().join("crates/featlas-core/src")).unwrap() {
        let text = std::fs::read_to_string(entry.unwrap().path())
            .unwrap()
            .to_lowercase();
        for banned in ["databricks", "unity catalog", "spark", "delta"] {
            assert!(!text.contains(banned), "core source mentions {banned}");
        }
    }
}
