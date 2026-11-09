//! Target-neutral execution plan.
//!
//! The plan exposes, per feature set and feature, the ordered stages an
//! implementation must preserve: source reconstruction (availability →
//! primary-key dedup → logical-key revision selection → tie rejection →
//! tombstones), population, event windows, availability-aware joins,
//! predicates, complete grouping, the exact transform, coverage, missingness,
//! output contract, lineage and publication intent. It references logical
//! sources and columns only; physical bindings are listed separately so the
//! logical part (and its digest) is identical across rebinding.

use crate::canon::{deployment_identity, digest, logical_identities};
use crate::model::*;
use crate::validate::{dependency_order, feature_sources, transform_type};
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

pub const PLAN_VERSION: &str = "featlas.plan/v1";
pub const CORE_NAME: &str = "featlas-core";
pub const CORE_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Clone, Serialize)]
pub struct SourceNode {
    pub id: String,
    pub version: String,
    pub fingerprint: String,
    pub record_kind: String,
    pub columns: Vec<ColumnNode>,
    pub event_time: String,
    pub available_time: String,
    pub primary_key: Vec<String>,
    pub duplicate_policy: DuplicatePolicy,
    pub logical_key: Vec<String>,
    pub revision_order: Vec<RevisionOrder>,
    pub tombstone: Option<(String, bool)>,
    pub earliest_reproducible_at: String,
    /// The fixed reconstruction order every lowering must preserve.
    pub stages: Vec<&'static str>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ColumnNode {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: LogicalType,
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "step", rename_all = "snake_case")]
pub enum Step {
    Reconstruct {
        alias: String,
        source: String,
        availability_upper: Boundary,
    },
    EventWindow {
        alias: String,
        column: String,
        lookback_seconds: i64,
        lower: Boundary,
        upper: Boundary,
    },
    Join {
        relationship: String,
        version: String,
        left_alias: String,
        right_alias: String,
        key_pairs: Vec<(String, String)>,
        cardinality: Cardinality,
        join_type: JoinType,
        unmatched: Unmatched,
        null_keys: NullKeys,
        temporal: RelationshipTemporal,
        left_event_time: String,
    },
    Filter {
        predicate: Expr,
    },
    Group {
        keys: Vec<GroupKey>,
    },
    Aggregate {
        operation: AggregateOp,
        value: Option<Expr>,
        distinct_by: Vec<Expr>,
        nulls: NullPolicy,
    },
    Recency {
        alias: String,
        column: String,
        rounding: String,
    },
    Lookup {
        value: Expr,
        order_by: Vec<OrderKey>,
    },
    Expression {
        expression: Expr,
        dependencies: Vec<String>,
    },
    RequiresCapability {
        kind: String,
        capabilities: Vec<String>,
        reason: String,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct CoverageNode {
    pub sources: Vec<String>,
    pub required_seconds: i64,
    pub earliest_as_of: String,
    /// `coverage_end > T` for an inclusive window upper bound, `>= T` for exclusive.
    pub end_rule: &'static str,
    pub status_envelope: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct FeaturePlan {
    pub id: String,
    pub version: String,
    pub fingerprint: String,
    pub kind: String,
    pub output: Output,
    pub static_type: Option<LogicalType>,
    pub dependencies: Vec<String>,
    pub steps: Vec<Step>,
    pub coverage: CoverageNode,
    pub missingness: Missingness,
    pub quality: FeatureQuality,
    pub lineage: FeatureLineage,
    pub publication_delay_seconds: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PopulationNode {
    pub entity: String,
    pub source: String,
    pub keys: Vec<EntityKey>,
    pub event_time_upper: Boundary,
    pub availability_upper: Boundary,
    pub deletion_policy: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct FeatureSetPlan {
    pub id: String,
    pub version: String,
    pub fingerprint: String,
    pub population: PopulationNode,
    /// Dependencies first; includes non-member dependencies needed to compute members.
    pub features: Vec<FeaturePlan>,
    pub members: Vec<String>,
    pub publication: PublicationIntent,
}

#[derive(Debug, Clone, Serialize)]
pub struct PublicationIntent {
    pub atomicity: String,
    pub visibility: String,
    pub surface: Surface,
    pub observed_timestamp_required: bool,
    pub metadata: Vec<&'static str>,
    pub target_role: Role,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Requirement {
    pub adapter: String,
    pub role: Role,
    pub capability: String,
    pub reasons: Vec<String>,
    pub declared: bool,
    pub derived: bool,
    /// Operations this requirement gates: `materialize` (compute/publish snapshots),
    /// `serve` (consumer reads/replay), `label_dataset`, `schedule`, or `declared`.
    pub operations: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Bindings {
    pub deployment: String,
    pub deployment_version: String,
    pub deployment_identity: String,
    pub adapters: Vec<AdapterInstance>,
    pub execution: Execution,
    pub sources: Vec<SourceBinding>,
    pub outputs: Vec<OutputTarget>,
    pub consumers: Vec<ConsumerBinding>,
    /// Logical request contracts of the bound consumers (entity inputs, time inputs, policies).
    pub consumer_contracts: Vec<Consumer>,
    pub schedule: Schedule,
    pub budget: Budget,
    pub backfill: Backfill,
    pub portability: Portability,
}

#[derive(Debug, Clone, Serialize)]
pub struct LogicalPlan {
    pub plan_version: &'static str,
    pub sources: BTreeMap<String, SourceNode>,
    pub feature_sets: Vec<FeatureSetPlan>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Plan {
    pub plan_version: &'static str,
    pub core: BTreeMap<&'static str, &'static str>,
    pub project: BTreeMap<String, String>,
    pub logical: LogicalPlan,
    pub logical_plan_digest: String,
    pub bindings: Bindings,
    pub requirements: Vec<Requirement>,
    pub nonportable: Vec<String>,
    pub plan_digest: String,
}

fn source_node(p: &Project, s: &Source, fp: &str) -> SourceNode {
    let rev = s.record_semantics.revision();
    let _ = p;
    SourceNode {
        id: s.id.clone(),
        version: s.version.clone(),
        fingerprint: fp.to_string(),
        record_kind: match s.record_semantics {
            RecordSemantics::AppendOnly { .. } => "append_only".into(),
            RecordSemantics::Versioned { .. } => "versioned".into(),
        },
        columns: s
            .columns
            .iter()
            .map(|c| ColumnNode {
                name: c.name.clone(),
                ty: c.ty,
                nullable: c.nullable,
            })
            .collect(),
        event_time: s.time.event_time.clone(),
        available_time: s.time.available_time.clone(),
        primary_key: s.primary_key.clone(),
        duplicate_policy: s.record_semantics.duplicate_policy(),
        logical_key: s.record_semantics.logical_key().to_vec(),
        revision_order: rev.map(|r| r.order_by.clone()).unwrap_or_default(),
        tombstone: rev.and_then(|r| r.tombstone_column.clone().zip(r.tombstone_value)),
        earliest_reproducible_at: s.retention.earliest_reproducible_at.clone(),
        stages: vec![
            "validate_rows",
            "availability_cutoff",
            "primary_key_duplicates",
            "logical_key_partition",
            "revision_select_latest",
            "reject_contradictory_ties",
            "apply_tombstones",
        ],
    }
}

fn feature_plan(p: &Project, f: &Feature, fp: &str) -> FeaturePlan {
    let mut steps = Vec::new();
    match &f.transform {
        Transform::Aggregate { .. } | Transform::Recency { .. } | Transform::Lookup { .. } => {
            let (input, joins, group_by, filter) = f.transform.row_shape().unwrap();
            let inp = f.input(input).unwrap();
            let src = p.source(&inp.source).unwrap();
            steps.push(Step::Reconstruct { alias: input.into(), source: src.id.clone(), availability_upper: f.time.availability.upper });
            let w = &f.time.window;
            steps.push(Step::EventWindow { alias: input.into(), column: src.time.event_time.clone(), lookback_seconds: w.lookback_seconds, lower: w.lower, upper: w.upper });
            for j in joins {
                let r = p.relationship(&j.relationship).unwrap();
                let right = f.input(&j.right_alias).unwrap();
                let left = f.input(&j.left_alias).unwrap();
                steps.push(Step::Reconstruct { alias: j.right_alias.clone(), source: right.source.clone(), availability_upper: f.time.availability.upper });
                steps.push(Step::Join {
                    relationship: r.id.clone(),
                    version: r.version.clone(),
                    left_alias: j.left_alias.clone(),
                    right_alias: j.right_alias.clone(),
                    key_pairs: r.from.columns.iter().cloned().zip(r.to.columns.iter().cloned()).collect(),
                    cardinality: r.cardinality,
                    join_type: r.join_type,
                    unmatched: r.unmatched,
                    null_keys: r.null_keys,
                    temporal: r.temporal.clone(),
                    left_event_time: p.source(&left.source).map(|s| s.time.event_time.clone()).unwrap_or_default(),
                });
            }
            if let Some(fl) = filter {
                steps.push(Step::Filter { predicate: fl.clone() });
            }
            steps.push(Step::Group { keys: group_by.to_vec() });
            match &f.transform {
                Transform::Aggregate { operation, value, distinct_by, nulls, .. } => steps.push(Step::Aggregate { operation: *operation, value: value.clone(), distinct_by: distinct_by.clone(), nulls: *nulls }),
                Transform::Recency { input, event_time, rounding, .. } => steps.push(Step::Recency { alias: input.clone(), column: event_time.clone(), rounding: rounding.clone() }),
                Transform::Lookup { value, order_by, .. } => steps.push(Step::Lookup { value: value.clone(), order_by: order_by.clone() }),
                _ => {}
            }
        }
        Transform::Expression { expression } => steps.push(Step::Expression { expression: expression.clone(), dependencies: f.dependencies.iter().map(|d| format!("{}@{}", d.feature, d.version)).collect() }),
        Transform::Learned { required_capabilities, .. } => steps.push(Step::RequiresCapability {
            kind: "learned".into(),
            capabilities: required_capabilities.clone(),
            reason: "Learned apply requires an immutable artifact, split provenance and an adapter that attests execution; never executed by the core".into(),
        }),
        Transform::Custom { required_capabilities, .. } => steps.push(Step::RequiresCapability {
            kind: "custom".into(),
            capabilities: required_capabilities.clone(),
            reason: "Custom code is never fetched or executed by the core; an adapter must attest a digest-pinned implementation".into(),
        }),
    }
    FeaturePlan {
        id: f.id.clone(),
        version: f.version.clone(),
        fingerprint: fp.to_string(),
        kind: f.transform.kind().into(),
        output: f.output.clone(),
        static_type: transform_type(p, f).and_then(|t| t.ty),
        dependencies: f.dependencies.iter().map(|d| format!("{}@{}", d.feature, d.version)).collect(),
        steps,
        coverage: CoverageNode {
            sources: feature_sources(p, &f.id).into_iter().collect(),
            required_seconds: f.history.required_seconds,
            earliest_as_of: f.history.earliest_as_of.clone(),
            end_rule: if f.time.window.upper == Boundary::Inclusive { "coverage_end > as_of" } else { "coverage_end >= as_of" },
            status_envelope: "[coverage_start, coverage_end) must contain [as_of - required_seconds, as_of]; outage => source_unavailable; otherwise insufficient_history",
        },
        missingness: f.missingness.clone(),
        quality: f.quality.clone(),
        lineage: f.lineage.clone(),
        publication_delay_seconds: f.time.publication.delay_seconds,
    }
}

/// Requirements derived from actual operations (SEM-PORT-006), merged with declared ones.
pub fn derive_requirements(p: &Project, d: &Deployment) -> Vec<Requirement> {
    let mut map: BTreeMap<
        (String, Role, String),
        (BTreeSet<String>, bool, bool, BTreeSet<&'static str>),
    > = BTreeMap::new();
    let mut add = |adapter: &str, role: Role, cap: &str, reason: String, declared: bool| {
        let op: &'static str = if declared {
            "declared"
        } else if cap == "core.label_construction" {
            "label_dataset"
        } else if cap == "core.serving_replay" {
            "serve"
        } else if cap == "core.scheduling" {
            "schedule"
        } else {
            "materialize"
        };
        let e = map
            .entry((adapter.to_string(), role, cap.to_string()))
            .or_insert((BTreeSet::new(), false, false, BTreeSet::new()));
        e.0.insert(reason);
        e.3.insert(op);
        if declared {
            e.1 = true;
        } else {
            e.2 = true;
        }
    };
    let compute = d.execution.compute.as_str();
    let mut features = Vec::new();
    for t in &d.output_targets {
        if let Some(fs) = p.feature_set(&t.feature_set) {
            features.extend(dependency_order(
                p,
                &fs.features.iter().map(|m| m.id.clone()).collect::<Vec<_>>(),
            ));
            add(&t.adapter, t.role, "core.atomic_publication", format!("Publish {}@{} as an atomic feature-set snapshot with observed visibility evidence", fs.id, fs.version), false);
            if t.role == Role::OnlineStore {
                add(
                    &t.adapter,
                    t.role,
                    "core.online_publication",
                    format!(
                        "Online publication of {}@{} with observed online visibility",
                        fs.id, fs.version
                    ),
                    false,
                );
            }
        }
    }
    features.sort();
    features.dedup();
    let mut sources = BTreeSet::new();
    for fid in &features {
        let Some(f) = p.feature(fid) else { continue };
        let why = |op: &str| format!("{op} required by {}@{}", f.id, f.version);
        match &f.transform {
            Transform::Aggregate { .. } => add(
                compute,
                Role::Compute,
                "core.aggregate",
                why("Exact aggregate"),
                false,
            ),
            Transform::Recency { .. } => add(
                compute,
                Role::Compute,
                "core.recency",
                why("Floor-rounded recency"),
                false,
            ),
            Transform::Lookup { .. } => add(
                compute,
                Role::Compute,
                "core.lookup",
                why("Deterministic lookup"),
                false,
            ),
            Transform::Expression { .. } => add(
                compute,
                Role::Compute,
                "core.expression",
                why("Typed derived expression"),
                false,
            ),
            Transform::Learned {
                required_capabilities,
                ..
            }
            | Transform::Custom {
                required_capabilities,
                ..
            } => {
                for c in required_capabilities {
                    add(
                        compute,
                        Role::Compute,
                        c,
                        why(&format!("{} transform", f.transform.kind())),
                        false,
                    );
                }
            }
        }
        if let Some((_, joins, _, filter)) = f.transform.row_shape() {
            add(
                compute,
                Role::Compute,
                "core.revision_reconstruction",
                why("Availability-first revision reconstruction"),
                false,
            );
            add(
                compute,
                Role::Compute,
                "core.source_as_known",
                why("Source-as-known recomputation"),
                false,
            );
            if filter.is_some() {
                add(
                    compute,
                    Role::Compute,
                    "core.expression",
                    why("Typed predicate"),
                    false,
                );
            }
            for j in joins {
                if let Some(r) = p.relationship(&j.relationship) {
                    match r.temporal {
                        RelationshipTemporal::AsOf { .. } => add(
                            compute,
                            Role::Compute,
                            "core.as_of_join",
                            why(&format!("Availability-aware valid-time join {}", r.id)),
                            false,
                        ),
                        RelationshipTemporal::None => add(
                            compute,
                            Role::Compute,
                            "core.relationship_join",
                            why(&format!("Cardinality-checked join {}", r.id)),
                            false,
                        ),
                    }
                }
            }
        }
        sources.extend(feature_sources(p, fid));
    }
    for sid in &sources {
        if let Some(b) = d.source_bindings.iter().find(|b| &b.source == sid) {
            add(&b.adapter, Role::Catalog, "core.source_read", format!("Read-only reconstructible history for {sid} through its complete column mapping"), false);
        }
    }
    for b in &d.consumer_bindings {
        if let Some(c) = p.consumer(&b.consumer) {
            if c.selection.mode == SelectionMode::HistoricalServingReplay {
                add(
                    &b.adapter,
                    b.role,
                    "core.serving_replay",
                    format!(
                        "Consumer {}@{} replays observed publications",
                        c.id, c.version
                    ),
                    false,
                );
            }
            if c.label.is_some() {
                add(
                    compute,
                    Role::Compute,
                    "core.label_construction",
                    format!(
                        "Consumer {}@{} builds forward-horizon labels",
                        c.id, c.version
                    ),
                    false,
                );
            }
        }
    }
    for n in &d.portability.native_bindings {
        for cap in &n.required_capabilities {
            add(
                &n.adapter,
                n.role,
                cap,
                format!(
                    "Native implementation of {}@{}",
                    n.feature.id, n.feature.version
                ),
                false,
            );
        }
    }
    if let (Some(o), Schedule::Interval { .. }) = (&d.execution.orchestrator, &d.schedule) {
        add(
            o,
            Role::Orchestrator,
            "core.scheduling",
            "Interval schedule with bounded, idempotent runs".into(),
            false,
        );
    }
    for r in &d.runtime.required_capabilities {
        add(&r.adapter, r.role, &r.capability, r.reason.clone(), true);
    }
    map.into_iter()
        .map(
            |((adapter, role, capability), (reasons, declared, derived, ops))| Requirement {
                adapter,
                role,
                capability,
                reasons: reasons.into_iter().collect(),
                declared,
                derived,
                operations: ops.into_iter().map(String::from).collect(),
            },
        )
        .collect()
}

pub fn build_plan(p: &Project, file_sha256: &str, deployment_id: &str) -> Option<Plan> {
    let d = p.deployment(deployment_id)?;
    let ids = logical_identities(p);
    let mut sources = BTreeMap::new();
    let mut feature_sets = Vec::new();
    for t in &d.output_targets {
        let Some(fs) = p.feature_set(&t.feature_set) else {
            continue;
        };
        if feature_sets.iter().any(|x: &FeatureSetPlan| x.id == fs.id) {
            continue;
        }
        let e = p.entity(&fs.entity)?;
        let order = dependency_order(
            p,
            &fs.features.iter().map(|m| m.id.clone()).collect::<Vec<_>>(),
        );
        let mut features = Vec::new();
        for fid in &order {
            let f = p.feature(fid)?;
            for sid in feature_sources(p, fid) {
                if let Some(s) = p.source(&sid) {
                    let key = format!("{}@{}", s.id, s.version);
                    sources.entry(s.id.clone()).or_insert_with(|| {
                        source_node(
                            p,
                            s,
                            ids.sources.get(&key).map(String::as_str).unwrap_or(""),
                        )
                    });
                }
            }
            features.push(feature_plan(
                p,
                f,
                ids.features
                    .get(&format!("{}@{}", f.id, f.version))
                    .map(String::as_str)
                    .unwrap_or(""),
            ));
        }
        feature_sets.push(FeatureSetPlan {
            id: fs.id.clone(),
            version: fs.version.clone(),
            fingerprint: ids
                .feature_sets
                .get(&format!("{}@{}", fs.id, fs.version))
                .cloned()
                .unwrap_or_default(),
            population: PopulationNode {
                entity: e.id.clone(),
                source: e.population.source.clone(),
                keys: e.keys.clone(),
                event_time_upper: e.population.event_time_upper,
                availability_upper: e.population.availability_upper,
                deletion_policy: e.population.deletion_policy.clone(),
            },
            features,
            members: fs
                .features
                .iter()
                .map(|m| format!("{}@{}", m.id, m.version))
                .collect(),
            publication: PublicationIntent {
                atomicity: d.publication.atomicity.clone(),
                visibility: d.publication.visibility.clone(),
                surface: d.publication.surface,
                observed_timestamp_required: d.publication.observed_timestamp_required,
                metadata: vec![
                    "as_of",
                    "published_at",
                    "publication_sequence",
                    "publication_id",
                    "materialization_id",
                ],
                target_role: d.publication.surface.role(),
            },
        });
    }
    let logical = LogicalPlan {
        plan_version: PLAN_VERSION,
        sources,
        feature_sets,
    };
    let logical_plan_digest = digest(&serde_json::to_value(&logical).ok()?);
    let bindings = Bindings {
        deployment: d.id.clone(),
        deployment_version: d.version.clone(),
        deployment_identity: deployment_identity(p, &d.id)?,
        adapters: d.adapters.clone(),
        execution: d.execution.clone(),
        sources: d.source_bindings.clone(),
        outputs: d.output_targets.clone(),
        consumers: d.consumer_bindings.clone(),
        consumer_contracts: d
            .consumer_bindings
            .iter()
            .filter_map(|b| p.consumer(&b.consumer).cloned())
            .collect(),
        schedule: d.schedule.clone(),
        budget: d.budget.clone(),
        backfill: d.backfill.clone(),
        portability: d.portability.clone(),
    };
    let requirements = derive_requirements(p, d);
    let nonportable = d
        .portability
        .native_bindings
        .iter()
        .map(|n| {
            format!(
                "{}@{} is implemented natively by {}/{} ({}); migration: {}",
                n.feature.id,
                n.feature.version,
                n.adapter,
                n.role.as_str(),
                n.implementation.sha256,
                n.migration_notes
            )
        })
        .collect();
    let mut project = BTreeMap::new();
    project.insert("id".into(), p.metadata.id.clone());
    project.insert("version".into(), p.metadata.version.clone());
    project.insert("spec_version".into(), p.spec_version.clone());
    project.insert("file_sha256".into(), file_sha256.to_string());
    project.insert("logical_digest".into(), ids.logical_project.clone());
    let core = BTreeMap::from([("name", CORE_NAME), ("version", CORE_VERSION)]);
    let plan_digest = digest(&json!({
        "logical_plan_digest": logical_plan_digest,
        "bindings": serde_json::to_value(&bindings).ok()?,
        "requirements": serde_json::to_value(&requirements).ok()?,
        "core": core,
    }));
    Some(Plan {
        plan_version: PLAN_VERSION,
        core,
        project,
        logical,
        logical_plan_digest,
        bindings,
        requirements,
        nonportable,
        plan_digest,
    })
}

pub fn plan_value(plan: &Plan) -> Value {
    serde_json::to_value(plan).unwrap_or(Value::Null)
}
