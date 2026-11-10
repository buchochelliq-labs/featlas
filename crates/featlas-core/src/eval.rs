//! Exact local evaluator for bounded fixtures and small local inputs.
//!
//! Stage order (enforced for every feature and every source):
//!  1. validate supplied rows and source status (invalid rows are rejected);
//!  2. apply the source-availability cutoff;
//!  3. resolve physical primary-key duplicates under the declared policy;
//!  4. partition by the complete logical key and select the latest eligible
//!     revision; contradictory ties fail; no fallback to older revisions;
//!  5. apply tombstones of the selected revision;
//!  6. reconstruct the eligible population (compared with fixture rows);
//!  7. event window → availability-aware valid-time joins → predicate →
//!     complete entity grouping (tenant keys included);
//!  8. exact transform and pinned same-entity dependencies;
//!  9. coverage/outage/empty/null/quality/output contracts;
//! 10. values + states + trace.
//!
//! Learned and custom transforms are never executed here.

use crate::diag::EvalError;
use crate::expr::{self, ValueEnv};
use crate::model::*;
use crate::time::{Instant, MICROS_PER_SECOND};
use crate::validate::{dependency_order, feature_sources, transform_type};
use crate::value::{compare, identical, key_string, V};
use serde::Serialize;
use serde_json::{Map, Value as Json};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};

pub type TRow = BTreeMap<String, V>;

#[derive(Debug, Clone)]
pub struct Limits {
    pub max_input_rows: usize,
    pub max_output_rows: usize,
    pub max_join_pairs: usize,
    pub deadline: Option<std::time::Instant>,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_input_rows: 1_000_000,
            max_output_rows: 1_000_000,
            max_join_pairs: 20_000_000,
            deadline: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct OutputRow {
    pub entity_keys: Map<String, Json>,
    pub value: Json,
    pub state: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TraceStep {
    pub feature: String,
    pub stage: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Evaluation {
    pub rows: Vec<OutputRow>,
    pub trace: Vec<TraceStep>,
}

/// Inputs to one evaluation, independent of fixture syntax.
pub struct Request<'a> {
    pub as_of: Instant,
    pub source_rows: &'a Map<String, Json>,
    pub source_status: &'a [SourceStatus],
    /// When present, reconstructed population keys must equal these rows.
    pub expected_population: Option<&'a [Row]>,
    pub mode: SelectionMode,
    pub requested_at: Option<Instant>,
    pub published_values: Option<&'a [PublishedValue]>,
    pub consumer: Option<&'a str>,
    pub capabilities: &'a [String],
}

fn err(code: &str, rule: &str, msg: impl Into<String>) -> EvalError {
    EvalError::new(code, rule, msg)
}

struct Evaluator<'a> {
    p: &'a Project,
    req: &'a Request<'a>,
    limits: &'a Limits,
    trace: Vec<TraceStep>,
    validated: BTreeMap<String, Vec<TRow>>,
    work: usize,
    reconstructed: HashMap<(String, Boundary), Vec<TRow>>,
    types: std::rc::Rc<TypeCache>,
}

#[derive(Clone)]
struct FeatureOutput {
    values: Vec<(V, String)>,
}

impl<'a> Evaluator<'a> {
    fn note(&mut self, feature: &str, stage: &str, detail: String) {
        self.trace.push(TraceStep {
            feature: feature.into(),
            stage: stage.into(),
            detail,
        });
    }

    fn tick(&mut self, n: usize) -> Result<(), EvalError> {
        self.work += n;
        if self.work > self.limits.max_join_pairs {
            return Err(err(
                "RESOURCE_LIMIT_EXCEEDED",
                "SEM-BUDGET-001",
                format!(
                    "Evaluation exceeded {} work units; no sampling or truncation is performed",
                    self.limits.max_join_pairs
                ),
            ));
        }
        if let Some(d) = self.limits.deadline {
            if std::time::Instant::now() > d {
                return Err(err(
                    "RESOURCE_LIMIT_EXCEEDED",
                    "SEM-BUDGET-001",
                    "Evaluation exceeded its time limit",
                ));
            }
        }
        Ok(())
    }

    fn source(&self, id: &str) -> Result<&'a Source, EvalError> {
        self.p
            .source(id)
            .ok_or_else(|| err("UNKNOWN_SOURCE", "SEM-ID-002", id.to_string()))
    }

    fn status(&self, id: &str) -> Result<&'a SourceStatus, EvalError> {
        self.req
            .source_status
            .iter()
            .find(|s| s.source() == id)
            .ok_or_else(|| {
                err(
                    "MISSING_SOURCE_STATUS",
                    "SEM-MISS-003",
                    format!(
                        "No source status evidence for {id}; absent rows never prove completeness"
                    ),
                )
            })
    }

    /// Stage 1: type-validate every supplied row of a source.
    fn validate_rows(&mut self, sid: &str) -> Result<(), EvalError> {
        if self.validated.contains_key(sid) {
            return Ok(());
        }
        let s = self.source(sid)?;
        let rows = self
            .req
            .source_rows
            .get(sid)
            .ok_or_else(|| {
                err(
                    "MISSING_SOURCE_ROWS",
                    "SEM-TEST-001",
                    format!("No rows (not even an explicit empty list) supplied for {sid}"),
                )
            })?
            .as_array()
            .ok_or_else(|| {
                err(
                    "INVALID_SOURCE_ROW",
                    "SEM-SRC-001",
                    format!("{sid} rows must be a list"),
                )
            })?;
        if rows.len() > self.limits.max_input_rows {
            return Err(err(
                "RESOURCE_LIMIT_EXCEEDED",
                "SEM-BUDGET-001",
                format!(
                    "{sid} has {} rows; limit {}",
                    rows.len(),
                    self.limits.max_input_rows
                ),
            ));
        }
        let mut out = Vec::with_capacity(rows.len());
        for (i, row) in rows.iter().enumerate() {
            let obj = row.as_object().ok_or_else(|| {
                err(
                    "INVALID_SOURCE_ROW",
                    "SEM-SRC-001",
                    format!("{sid}[{i}] is not an object"),
                )
            })?;
            if obj.len() != s.columns.len() || obj.keys().any(|k| s.column(k).is_none()) {
                return Err(err(
                    "INVALID_SOURCE_ROW",
                    "SEM-SRC-001",
                    format!("{sid}[{i}] columns differ from the source contract"),
                ));
            }
            let mut t = TRow::new();
            for c in &s.columns {
                let raw = obj.get(&c.name).ok_or_else(|| {
                    err(
                        "INVALID_SOURCE_ROW",
                        "SEM-SRC-001",
                        format!("{sid}[{i}] lacks {}", c.name),
                    )
                })?;
                let v = V::from_json(raw, c.ty).map_err(|m| {
                    err(
                        "INVALID_SOURCE_ROW",
                        "SEM-SRC-001",
                        format!("{sid}[{i}].{}: {m}", c.name),
                    )
                })?;
                if v.is_null() && !c.nullable {
                    return Err(err(
                        "INVALID_SOURCE_ROW",
                        "SEM-SRC-001",
                        format!(
                            "{sid}[{i}].{} is null but not nullable (invalid_row_policy: reject)",
                            c.name
                        ),
                    ));
                }
                t.insert(c.name.clone(), v);
            }
            out.push(t);
        }
        let n = out.len();
        self.validated.insert(sid.to_string(), out);
        self.note("", "validate_rows", format!("{sid}: {n} rows type-checked"));
        Ok(())
    }

    /// Stages 2–5 for one source at one cutoff.
    fn reconstruct(
        &mut self,
        feature: &str,
        sid: &str,
        upper: Boundary,
    ) -> Result<Vec<TRow>, EvalError> {
        if let Some(rows) = self.reconstructed.get(&(sid.to_string(), upper)) {
            return Ok(rows.clone());
        }
        let rows = self.reconstruct_uncached(feature, sid, upper)?;
        self.reconstructed
            .insert((sid.to_string(), upper), rows.clone());
        Ok(rows)
    }

    fn reconstruct_uncached(
        &mut self,
        feature: &str,
        sid: &str,
        upper: Boundary,
    ) -> Result<Vec<TRow>, EvalError> {
        self.validate_rows(sid)?;
        let s = self.source(sid)?;
        let cutoff = self.req.as_of;
        let all = self.validated[sid].clone();
        let total = all.len();
        let available: Vec<TRow> = all
            .into_iter()
            .filter(|r| match r.get(&s.time.available_time) {
                Some(V::Ts(t)) => {
                    if upper == Boundary::Inclusive {
                        *t <= cutoff
                    } else {
                        *t < cutoff
                    }
                }
                _ => false,
            })
            .collect();
        let n_avail = available.len();
        self.tick(n_avail)?;
        // Physical primary-key duplicates.
        let mut by_pk: BTreeMap<String, Vec<TRow>> = BTreeMap::new();
        for r in available {
            let k: Vec<V> = s.primary_key.iter().map(|c| r[c].clone()).collect();
            by_pk.entry(key_string(&k)).or_default().push(r);
        }
        let policy = s.record_semantics.duplicate_policy();
        let mut deduped = Vec::new();
        for (k, group) in by_pk {
            if group.len() > 1 {
                if policy == DuplicatePolicy::Reject {
                    return Err(err("DUPLICATE_CONFLICT", "SEM-REV-002", format!("{sid}: duplicate primary key {k:?} rejected by duplicate_policy: reject")));
                }
                if group.iter().skip(1).any(|r| !rows_identical(r, &group[0])) {
                    return Err(err("DUPLICATE_CONFLICT", "SEM-REV-002", format!("{sid}: rows sharing primary key disagree; collapse_identical never masks conflicts")));
                }
            }
            deduped.push(group.into_iter().next().unwrap());
        }
        // Logical key partition and revision selection.
        let mut by_lk: BTreeMap<String, Vec<TRow>> = BTreeMap::new();
        for r in deduped {
            let k: Vec<V> = s
                .record_semantics
                .logical_key()
                .iter()
                .map(|c| r[c].clone())
                .collect();
            by_lk.entry(key_string(&k)).or_default().push(r);
        }
        let mut selected = Vec::new();
        let mut tombstoned = 0usize;
        for (_, mut group) in by_lk {
            match &s.record_semantics {
                RecordSemantics::AppendOnly { .. } => {
                    if group.iter().skip(1).any(|r| !rows_identical(r, &group[0])) {
                        return Err(err(
                            "DUPLICATE_CONFLICT",
                            "SEM-REV-002",
                            format!(
                                "{sid}: an immutable logical event has conflicting physical rows"
                            ),
                        ));
                    }
                    selected.push(group.swap_remove(0));
                }
                RecordSemantics::Versioned { revision, .. } => {
                    let order = |a: &TRow, b: &TRow| -> Ordering {
                        for o in &revision.order_by {
                            let ord =
                                compare(&a[&o.column], &b[&o.column]).unwrap_or(Ordering::Equal);
                            if ord != Ordering::Equal {
                                return if o.direction == "desc" {
                                    ord.reverse()
                                } else {
                                    ord
                                };
                            }
                        }
                        Ordering::Equal
                    };
                    // Every eligible revision-key tie with differing payload is contradictory.
                    let mut seen: BTreeMap<String, &TRow> = BTreeMap::new();
                    for r in &group {
                        let rk: Vec<V> = revision
                            .order_by
                            .iter()
                            .map(|o| r[&o.column].clone())
                            .collect();
                        let key = key_string(&rk);
                        if let Some(prev) = seen.get(&key) {
                            if policy == DuplicatePolicy::Reject || !rows_identical(prev, r) {
                                return Err(err(
                                    "DUPLICATE_CONFLICT",
                                    "SEM-REV-001",
                                    format!(
                                        "{sid}: contradictory revisions share ordering key {key:?}"
                                    ),
                                ));
                            }
                        }
                        seen.insert(key, r);
                    }
                    group.sort_by(order);
                    let top = group.swap_remove(0);
                    if let (Some(col), Some(val)) =
                        (&revision.tombstone_column, revision.tombstone_value)
                    {
                        if top.get(col) == Some(&V::Bool(val)) {
                            tombstoned += 1;
                            continue;
                        }
                    }
                    selected.push(top);
                }
            }
        }
        self.note(feature, "reconstruct", format!("{sid}: {total} supplied → {n_avail} available ({:?} cutoff) → {} selected, {tombstoned} retracted", upper, selected.len()));
        Ok(selected)
    }

    /// Stage 6: population keys at T.
    fn population(&mut self, entity: &'a Entity) -> Result<Vec<Vec<V>>, EvalError> {
        let pop = &entity.population;
        let status = self.status(&pop.source)?;
        if matches!(status, SourceStatus::Unavailable { .. }) {
            return Err(err("SOURCE_UNAVAILABLE", "SEM-ENT-001", format!("Population source {} is unavailable; the eligible population cannot be reconstructed", pop.source)));
        }
        let rows = self.reconstruct("", &pop.source, pop.availability_upper)?;
        let s = self.source(&pop.source)?;
        let t = self.req.as_of;
        let mut keys = Vec::new();
        let mut seen = BTreeSet::new();
        for r in rows {
            let ok = match &r[&s.time.event_time] {
                V::Ts(e) => {
                    if pop.event_time_upper == Boundary::Inclusive {
                        *e <= t
                    } else {
                        *e < t
                    }
                }
                _ => false,
            };
            if !ok {
                continue;
            }
            let k: Vec<V> = entity
                .keys
                .iter()
                .map(|k| r[&k.source_column].clone())
                .collect();
            if !seen.insert(key_string(&k)) {
                return Err(err(
                    "DUPLICATE_POPULATION",
                    "SEM-ENT-001",
                    "Population source produces duplicate entity keys at T",
                ));
            }
            keys.push(k);
        }
        if let Some(expected) = self.req.expected_population {
            let mut want = BTreeSet::new();
            for row in expected {
                let mut k = Vec::new();
                for ek in &entity.keys {
                    let raw = row.get(&ek.name).ok_or_else(|| {
                        err(
                            "POPULATION_MISMATCH",
                            "SEM-ENT-001",
                            format!("Fixture population row lacks {}", ek.name),
                        )
                    })?;
                    k.push(
                        V::from_json(raw, ek.ty)
                            .map_err(|m| err("POPULATION_MISMATCH", "SEM-ENT-001", m))?,
                    );
                }
                want.insert(key_string(&k));
            }
            if want != seen || want.len() != expected.len() {
                return Err(err("POPULATION_MISMATCH", "SEM-ENT-001", "Fixture population_rows disagree with the population reconstructed from source history"));
            }
        }
        keys.sort_by_key(|k| key_string(k));
        self.note(
            "",
            "population",
            format!("{}: {} eligible entities", entity.id, keys.len()),
        );
        Ok(keys)
    }

    fn coverage(&self, f: &Feature) -> Result<Option<&'static str>, EvalError> {
        coverage_state(self.p, f, self.req.as_of, self.req.source_status)
    }

    fn feature_by_id(&self, id: &str) -> Result<&'a Feature, EvalError> {
        self.p
            .feature(id)
            .ok_or_else(|| err("UNKNOWN_FEATURE", "SEM-ID-002", id.to_string()))
    }

    fn compute(
        &mut self,
        f: &'a Feature,
        population: &[Vec<V>],
        deps: &HashMap<String, FeatureOutput>,
        entity: &Entity,
    ) -> Result<FeatureOutput, EvalError> {
        if matches!(
            f.transform,
            Transform::Learned { .. } | Transform::Custom { .. }
        ) {
            return Err(err(
                "UNSUPPORTED_CAPABILITY",
                "SEM-CUSTOM-002",
                format!("{} is a {} transform; the local evaluator never loads artifacts or executes custom code", f.id, f.transform.kind()),
            ));
        }
        let coverage = self.coverage(f)?;
        self.note(
            &f.id,
            "coverage",
            format!(
                "{}",
                coverage.unwrap_or("complete for the required interval")
            ),
        );
        let mut values: Vec<(V, String)> = Vec::with_capacity(population.len());
        let static_ty = transform_type(self.p, f).and_then(|t| t.ty);
        if coverage == Some("source_unavailable") {
            for _ in population {
                values.push(missing(f, "source_unavailable")?);
            }
            return Ok(FeatureOutput { values });
        }
        match &f.transform {
            Transform::Expression { expression } => {
                for (i, _) in population.iter().enumerate() {
                    if let Some(state) = coverage {
                        values.push(missing(f, state)?);
                        continue;
                    }
                    // A dependency's outage/history reason propagates rather than becoming a plain null.
                    let mut inherited = None;
                    for d in &f.dependencies {
                        let st = &deps[&d.feature].values[i].1;
                        if st == "source_unavailable" || st == "insufficient_history" {
                            inherited = Some(if st == "source_unavailable" {
                                "source_unavailable"
                            } else {
                                "insufficient_history"
                            });
                            if st == "source_unavailable" {
                                break;
                            }
                        }
                    }
                    if let Some(state) = inherited {
                        values.push(missing(f, state)?);
                        continue;
                    }
                    let env = DepEnv { deps, index: i };
                    let v = expr::eval(
                        expression,
                        &Typed {
                            inner: &env,
                            p: self.p,
                            types: self.types.clone(),
                            f,
                        },
                    )?;
                    values.push((expr::conform(v, static_ty), "ok".into()));
                }
            }
            _ if coverage.is_some() => {
                // Coverage not established: sources are still validated and reconstructed (data
                // contract violations surface), but windows/joins/predicates/transforms are skipped.
                let state = coverage.unwrap();
                for inp in &f.inputs {
                    self.reconstruct(&f.id, &inp.source, f.time.availability.upper)?;
                }
                for _ in population {
                    values.push(missing(f, state)?);
                }
            }
            _ => {
                let groups = self.row_environments(f, entity)?;
                self.reduce_assertions(f, &groups, population)?;
                for key in population {
                    if let Some(state) = coverage {
                        values.push(missing(f, state)?);
                        continue;
                    }
                    let rows = groups
                        .get(&key_string(key))
                        .map(Vec::as_slice)
                        .unwrap_or(&[]);
                    values.push(self.reduce(f, rows, static_ty)?);
                }
            }
        }
        Ok(FeatureOutput { values })
    }

    /// Stage 7: window, joins, predicate and grouping for a row-level transform.
    fn row_environments(
        &mut self,
        f: &'a Feature,
        entity: &Entity,
    ) -> Result<HashMap<String, Vec<Env>>, EvalError> {
        let (input, joins, group_by, filter) = f.transform.row_shape().unwrap();
        let primary = f
            .input(input)
            .ok_or_else(|| err("UNKNOWN_INPUT", "SEM-REL-001", input.to_string()))?;
        let ps = self.source(&primary.source)?;
        let avail = f.time.availability.upper;
        let rows = self.reconstruct(&f.id, &ps.id, avail)?;
        let w = &f.time.window;
        let t = self.req.as_of;
        let start = t
            .checked_sub_seconds(w.lookback_seconds)
            .ok_or_else(|| err("NUMERIC_OVERFLOW", "SEM-TIME-001", "Window underflows"))?;
        let before = rows.len();
        let windowed: Vec<TRow> = rows
            .into_iter()
            .filter(|r| match &r[&ps.time.event_time] {
                V::Ts(e) => {
                    (if w.lower == Boundary::Inclusive {
                        *e >= start
                    } else {
                        *e > start
                    }) && (if w.upper == Boundary::Inclusive {
                        *e <= t
                    } else {
                        *e < t
                    })
                }
                _ => false,
            })
            .collect();
        self.note(
            &f.id,
            "event_window",
            format!(
                "{}: {before} → {} rows in {}{start}, {t}{}",
                ps.id,
                windowed.len(),
                if w.lower == Boundary::Inclusive {
                    "["
                } else {
                    "("
                },
                if w.upper == Boundary::Inclusive {
                    "]"
                } else {
                    ")"
                }
            ),
        );
        let mut envs: Vec<Env> = windowed
            .into_iter()
            .map(|r| Env {
                rows: BTreeMap::from([(input.to_string(), Some(r))]),
            })
            .collect();
        for j in joins {
            let r = self
                .p
                .relationship(&j.relationship)
                .ok_or_else(|| err("UNKNOWN_RELATIONSHIP", "SEM-ID-002", j.relationship.clone()))?;
            let right_input = f
                .input(&j.right_alias)
                .ok_or_else(|| err("UNKNOWN_INPUT", "SEM-REL-001", j.right_alias.clone()))?;
            let left_input = f
                .input(&j.left_alias)
                .ok_or_else(|| err("UNKNOWN_INPUT", "SEM-REL-001", j.left_alias.clone()))?;
            let left_source = self.source(&left_input.source)?;
            let candidates = self.reconstruct(&f.id, &right_input.source, avail)?;
            if let RelationshipTemporal::AsOf {
                valid_from,
                valid_to,
                ..
            } = &r.temporal
            {
                for c in &candidates {
                    if let (V::Ts(a), V::Ts(b)) = (&c[valid_from], &c[valid_to]) {
                        if b <= a {
                            return Err(err(
                                "INVALID_INTERVAL",
                                "SEM-REV-003",
                                format!(
                                    "{}: selected revision has valid_to <= valid_from",
                                    right_input.source
                                ),
                            ));
                        }
                    }
                }
            }
            let mut index: HashMap<String, Vec<usize>> = HashMap::new();
            for (ci, c) in candidates.iter().enumerate() {
                let k: Vec<V> = r.to.columns.iter().map(|col| c[col].clone()).collect();
                if k.iter().any(V::is_null) {
                    continue; // Null keys never match.
                }
                index.entry(key_string(&k)).or_default().push(ci);
            }
            let mut out = Vec::new();
            let mut seen_left = BTreeSet::new();
            let (mut matched, mut unmatched) = (0usize, 0usize);
            // Scan every left row and report by fixed precedence (null key, cardinality, multiple
            // matches, unmatched) so the first diagnostic does not depend on row order and matches
            // the order of the generated assertion queries.
            let mut violations: [Option<EvalError>; 4] = [None, None, None, None];
            let mut record = |slot: usize, e: EvalError| {
                if violations[slot].is_none() {
                    violations[slot] = Some(e);
                }
            };
            for env in envs {
                let left = env.rows.get(&j.left_alias).cloned().flatten();
                let key: Vec<V> = match &left {
                    Some(l) => r.from.columns.iter().map(|c| l[c].clone()).collect(),
                    None => r.from.columns.iter().map(|_| V::Null).collect(),
                };
                let has_null = key.iter().any(V::is_null);
                if has_null && r.null_keys == NullKeys::Error {
                    record(
                        0,
                        err(
                            "NULL_JOIN_KEY",
                            "SEM-REL-003",
                            format!("{}: null join key with null_keys: error", r.id),
                        ),
                    );
                    continue;
                }
                if r.cardinality == Cardinality::OneToOne
                    && !has_null
                    && !seen_left.insert(key_string(&key))
                {
                    record(
                        1,
                        err(
                            "CARDINALITY_VIOLATION",
                            "SEM-REL-002",
                            format!(
                                "{}: repeated eligible left key for a one-to-one relationship",
                                r.id
                            ),
                        ),
                    );
                }
                let mut matches = Vec::new();
                if !has_null {
                    for &ci in index
                        .get(&key_string(&key))
                        .map(Vec::as_slice)
                        .unwrap_or(&[])
                    {
                        self.tick(1)?;
                        let c = &candidates[ci];
                        if let RelationshipTemporal::AsOf {
                            valid_from,
                            valid_to,
                            lower,
                            upper,
                            ..
                        } = &r.temporal
                        {
                            let Some(V::Ts(ev)) =
                                left.as_ref().map(|l| &l[&left_source.time.event_time])
                            else {
                                continue;
                            };
                            let from_ok = match (&c[valid_from], lower) {
                                (V::Ts(a), Boundary::Inclusive) => ev >= a,
                                (V::Ts(a), Boundary::Exclusive) => ev > a,
                                _ => false,
                            };
                            let to_ok = match (&c[valid_to], upper) {
                                (V::Null, _) => true,
                                (V::Ts(b), Boundary::Exclusive) => ev < b,
                                (V::Ts(b), Boundary::Inclusive) => ev <= b,
                                _ => false,
                            };
                            if !(from_ok && to_ok) {
                                continue;
                            }
                        }
                        matches.push(ci);
                    }
                }
                if matches.len() > 1 {
                    record(2, err("MULTIPLE_MATCHES", "SEM-REL-002", format!("{}: {} eligible right rows match one left row; no arbitrary choice is made", r.id, matches.len())));
                    continue;
                }
                let mut env = env;
                if let Some(ci) = matches.first() {
                    matched += 1;
                    env.rows
                        .insert(j.right_alias.clone(), Some(candidates[*ci].clone()));
                    out.push(env);
                } else {
                    unmatched += 1;
                    match r.unmatched {
                        Unmatched::Error => record(
                            3,
                            err(
                                "UNMATCHED_JOIN",
                                "SEM-REL-003",
                                format!("{}: unmatched left row with unmatched: error", r.id),
                            ),
                        ),
                        Unmatched::PreserveNull => {
                            env.rows.insert(j.right_alias.clone(), None);
                            out.push(env);
                        }
                        Unmatched::Drop => {}
                    }
                }
            }
            if let Some(e) = violations.into_iter().flatten().next() {
                return Err(e);
            }
            self.note(
                &f.id,
                "join",
                format!(
                    "{} ({:?}, {:?}): {matched} matched, {unmatched} unmatched",
                    r.id,
                    r.cardinality,
                    r.temporal.temporal_mode()
                ),
            );
            envs = out;
        }
        if let Some(fl) = filter {
            let before = envs.len();
            let mut kept = Vec::new();
            for e in envs {
                if expr::passes(&expr::eval(
                    fl,
                    &Typed {
                        inner: &e,
                        p: self.p,
                            types: self.types.clone(),
                        f,
                    },
                )?) {
                    kept.push(e);
                }
            }
            envs = kept;
            self.note(
                &f.id,
                "predicate",
                format!("{before} → {} rows (only true passes)", envs.len()),
            );
        }
        let mut groups: HashMap<String, Vec<Env>> = HashMap::new();
        for e in envs {
            let mut key = Vec::new();
            let mut ok = true;
            for k in &entity.keys {
                let g = group_by
                    .iter()
                    .find(|g| g.entity_key == k.name)
                    .ok_or_else(|| {
                        err("ENTITY_MAPPING_INCOMPLETE", "SEM-ENT-002", k.name.clone())
                    })?;
                match e.rows.get(&g.input).cloned().flatten() {
                    Some(r) if !r[&g.column].is_null() => key.push(r[&g.column].clone()),
                    _ => ok = false,
                }
            }
            if ok {
                groups.entry(key_string(&key)).or_default().push(e);
            }
        }
        self.note(
            &f.id,
            "group",
            format!(
                "{} entity groups on complete keys ({})",
                groups.len(),
                entity
                    .keys
                    .iter()
                    .map(|k| k.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        );
        Ok(groups)
    }

    /// Reduce-stage assertions over all population entities, before any computation, in the
    /// fixed order NULL_INPUT → NULL_ORDER_KEY → LOOKUP_TIE (mirrors the generated assertions).
    fn reduce_assertions(
        &mut self,
        f: &Feature,
        groups: &HashMap<String, Vec<Env>>,
        population: &[Vec<V>],
    ) -> Result<(), EvalError> {
        let rows_of = |k: &Vec<V>| groups.get(&key_string(k)).map(Vec::as_slice).unwrap_or(&[]);
        match &f.transform {
            Transform::Aggregate {
                nulls: NullPolicy::Error,
                value,
                distinct_by,
                ..
            } => {
                for k in population {
                    for e in rows_of(k) {
                        let env = Typed {
                            inner: e,
                            p: self.p,
                            types: self.types.clone(),
                            f,
                        };
                        let exprs: Vec<&Expr> = value.iter().chain(distinct_by.iter()).collect();
                        for x in exprs {
                            if expr::eval(x, &env)?.is_null() {
                                return Err(err(
                                    "NULL_INPUT",
                                    "SEM-TRANS-004",
                                    "Null aggregate input with nulls: error",
                                ));
                            }
                        }
                    }
                }
            }
            Transform::Lookup { order_by, .. } => {
                let key_of = |e: &Env| -> Vec<V> {
                    order_by
                        .iter()
                        .map(|o| {
                            e.rows
                                .get(&o.input)
                                .cloned()
                                .flatten()
                                .map(|r| r[&o.column].clone())
                                .unwrap_or(V::Null)
                        })
                        .collect()
                };
                for k in population {
                    for e in rows_of(k) {
                        if key_of(e).iter().any(V::is_null) {
                            return Err(err(
                                "NULL_ORDER_KEY",
                                "SEM-TRANS-005",
                                "Lookup ordering key is null",
                            ));
                        }
                    }
                }
                let cmp = |a: &[V], b: &[V]| -> Ordering {
                    for (i, o) in order_by.iter().enumerate() {
                        let ord = compare(&a[i], &b[i]).unwrap_or(Ordering::Equal);
                        if ord != Ordering::Equal {
                            return if o.direction == "desc" {
                                ord.reverse()
                            } else {
                                ord
                            };
                        }
                    }
                    Ordering::Equal
                };
                for k in population {
                    let mut keys: Vec<Vec<V>> = rows_of(k).iter().map(key_of).collect();
                    keys.sort_by(|a, b| cmp(a, b));
                    if keys.len() > 1 && cmp(&keys[0], &keys[1]) == Ordering::Equal {
                        return Err(err("LOOKUP_TIE", "SEM-TRANS-005", "Lookup ordering is tied for the first row; ties are diagnosed even when values are equal"));
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Stage 8 for one entity's eligible rows.
    fn reduce(
        &mut self,
        f: &Feature,
        rows: &[Env],
        static_ty: Option<LogicalType>,
    ) -> Result<(V, String), EvalError> {
        match &f.transform {
            Transform::Aggregate {
                operation,
                value,
                distinct_by,
                nulls,
                ..
            } => {
                if rows.is_empty() {
                    return empty(f);
                }
                let ok = |v: V| Ok((expr::conform(v, static_ty), "ok".to_string()));
                match operation {
                    AggregateOp::Count => ok(V::Int(rows.len() as i64)),
                    AggregateOp::CountDistinct => {
                        let mut set = BTreeSet::new();
                        for e in rows {
                            let tuple: Vec<V> = distinct_by
                                .iter()
                                .map(|d| {
                                    expr::eval(
                                        d,
                                        &Typed {
                                            inner: e,
                                            p: self.p,
                            types: self.types.clone(),
                                            f,
                                        },
                                    )
                                    .map(|v| {
                                        expr::conform(v, self.types.get(self.p, f, d))
                                    })
                                })
                                .collect::<Result<_, _>>()?;
                            if tuple.iter().any(V::is_null) {
                                match nulls {
                                    NullPolicy::Error => {
                                        return Err(err(
                                            "NULL_INPUT",
                                            "SEM-TRANS-004",
                                            "Null distinct_by component with nulls: error",
                                        ))
                                    }
                                    NullPolicy::Propagate => return Ok((V::Null, "ok".into())),
                                    NullPolicy::Ignore => continue,
                                }
                            }
                            set.insert(key_string(&tuple));
                        }
                        ok(V::Int(set.len() as i64))
                    }
                    op => {
                        let expr = value.as_ref().ok_or_else(|| {
                            err(
                                "TYPE_MISMATCH",
                                "SEM-TRANS-004",
                                "Aggregate requires a value expression",
                            )
                        })?;
                        let mut vals = Vec::new();
                        for e in rows {
                            let v = expr::conform(
                                expr::eval(
                                    expr,
                                    &Typed {
                                        inner: e,
                                        p: self.p,
                            types: self.types.clone(),
                                        f,
                                    },
                                )?,
                                // Widen to the value expression's own type (not the transform's
                                // output type), so an integer mean keeps its exact i128 path.
                                self.types.get(self.p, f, expr),
                            );
                            if v.is_null() {
                                match nulls {
                                    NullPolicy::Error => {
                                        return Err(err(
                                            "NULL_INPUT",
                                            "SEM-TRANS-004",
                                            "Null aggregate input with nulls: error",
                                        ))
                                    }
                                    NullPolicy::Propagate => return Ok((V::Null, "ok".into())),
                                    NullPolicy::Ignore => continue,
                                }
                            }
                            vals.push(v);
                        }
                        if vals.is_empty() {
                            // All-null eligible input under ignore: null with state ok, never the empty identity.
                            return Ok((V::Null, "ok".into()));
                        }
                        ok(aggregate(*op, &vals)?)
                    }
                }
            }
            Transform::Recency {
                input, event_time, ..
            } => {
                if rows.is_empty() {
                    return empty(f);
                }
                let latest = rows
                    .iter()
                    .filter_map(|e| {
                        match e
                            .rows
                            .get(input)
                            .cloned()
                            .flatten()
                            .map(|r| r[event_time].clone())
                        {
                            Some(V::Ts(t)) => Some(t),
                            _ => None,
                        }
                    })
                    .max()
                    .ok_or_else(|| {
                        err(
                            "TYPE_MISMATCH",
                            "SEM-TRANS-005",
                            "Recency rows lack event time",
                        )
                    })?;
                let elapsed = self.req.as_of.micros - latest.micros;
                if elapsed < 0 {
                    return Err(err(
                        "NEGATIVE_RECENCY",
                        "SEM-TRANS-005",
                        "Latest eligible event is after as_of",
                    ));
                }
                Ok((
                    expr::conform(V::Int(elapsed.div_euclid(MICROS_PER_SECOND)), static_ty),
                    "ok".into(),
                ))
            }
            Transform::Lookup {
                value, order_by, ..
            } => {
                if rows.is_empty() {
                    return empty(f);
                }
                let key_of = |e: &Env| -> Result<Vec<V>, EvalError> {
                    order_by
                        .iter()
                        .map(|o| {
                            let v = e
                                .rows
                                .get(&o.input)
                                .cloned()
                                .flatten()
                                .map(|r| r[&o.column].clone())
                                .unwrap_or(V::Null);
                            if v.is_null() {
                                Err(err(
                                    "NULL_ORDER_KEY",
                                    "SEM-TRANS-005",
                                    format!("Lookup ordering key {}.{} is null", o.input, o.column),
                                ))
                            } else {
                                Ok(v)
                            }
                        })
                        .collect()
                };
                let mut keyed: Vec<(Vec<V>, &Env)> = rows
                    .iter()
                    .map(|e| key_of(e).map(|k| (k, e)))
                    .collect::<Result<_, _>>()?;
                let cmp = |a: &Vec<V>, b: &Vec<V>| -> Ordering {
                    for (i, o) in order_by.iter().enumerate() {
                        let ord = compare(&a[i], &b[i]).unwrap_or(Ordering::Equal);
                        if ord != Ordering::Equal {
                            return if o.direction == "desc" {
                                ord.reverse()
                            } else {
                                ord
                            };
                        }
                    }
                    Ordering::Equal
                };
                keyed.sort_by(|a, b| cmp(&a.0, &b.0));
                if keyed.len() > 1 && cmp(&keyed[0].0, &keyed[1].0) == Ordering::Equal {
                    return Err(err("LOOKUP_TIE", "SEM-TRANS-005", "Lookup ordering is tied for the first row; ties are diagnosed even when values are equal"));
                }
                let v = expr::eval(
                    value,
                    &Typed {
                        inner: keyed[0].1,
                        p: self.p,
                            types: self.types.clone(),
                        f,
                    },
                )?;
                Ok((expr::conform(v, static_ty), "ok".into()))
            }
            _ => Err(err(
                "UNSUPPORTED_CAPABILITY",
                "SEM-CUSTOM-002",
                f.transform.kind().to_string(),
            )),
        }
    }
}

impl RelationshipTemporal {
    fn temporal_mode(&self) -> &'static str {
        match self {
            RelationshipTemporal::None => "none",
            RelationshipTemporal::AsOf { .. } => "as_of",
        }
    }
}

/// Coverage/outage state for a feature over all transitive sources (SEM-MISS-002/003).
pub fn coverage_state(
    p: &Project,
    f: &Feature,
    as_of: Instant,
    statuses: &[SourceStatus],
) -> Result<Option<&'static str>, EvalError> {
    let t = as_of;
    let sources = feature_sources(p, &f.id);
    let mut unavailable = false;
    let mut insufficient = false;
    let earliest = Instant::parse(&f.history.earliest_as_of)
        .map_err(|m| err("INVALID_TIMESTAMP", "SEM-TIME-003", m))?;
    if t < earliest {
        insufficient = true;
    }
    let start = t
        .checked_sub_seconds(f.history.required_seconds)
        .ok_or_else(|| {
            err(
                "NUMERIC_OVERFLOW",
                "SEM-TIME-001",
                "History interval underflows",
            )
        })?;
    for sid in &sources {
        let s = p
            .source(sid)
            .ok_or_else(|| err("UNKNOWN_SOURCE", "SEM-ID-002", sid.clone()))?;
        match statuses.iter().find(|x| x.source() == sid).ok_or_else(|| {
            err(
                "MISSING_SOURCE_STATUS",
                "SEM-MISS-003",
                format!("No source status evidence for {sid}"),
            )
        })? {
            SourceStatus::Unavailable { .. } => unavailable = true,
            SourceStatus::Available {
                coverage_start,
                coverage_end,
                ..
            } => {
                let cs = Instant::parse(coverage_start)
                    .map_err(|m| err("INVALID_TIMESTAMP", "SEM-TIME-003", m))?;
                let ce = Instant::parse(coverage_end)
                    .map_err(|m| err("INVALID_TIMESTAMP", "SEM-TIME-003", m))?;
                let end_ok = match f.time.window.upper {
                    Boundary::Inclusive => ce > t,
                    Boundary::Exclusive => ce >= t,
                };
                if cs > start || !end_ok {
                    insufficient = true;
                }
            }
        }
        let reproducible = Instant::parse(&s.retention.earliest_reproducible_at)
            .map_err(|m| err("INVALID_TIMESTAMP", "SEM-TIME-003", m))?;
        if t < reproducible {
            insufficient = true;
        }
    }
    Ok(if unavailable {
        Some("source_unavailable")
    } else if insufficient {
        Some("insufficient_history")
    } else {
        None
    })
}

fn rows_identical(a: &TRow, b: &TRow) -> bool {
    a.len() == b.len()
        && a.iter()
            .all(|(k, v)| b.get(k).is_some_and(|w| identical(v, w)))
}

#[derive(Clone)]
struct Env {
    rows: BTreeMap<String, Option<TRow>>,
}

impl ValueEnv for Env {
    fn column(&self, input: &str, column: &str) -> Result<V, EvalError> {
        match self.rows.get(input) {
            None => Err(err(
                "UNKNOWN_INPUT",
                "SEM-REL-001",
                format!("Alias {input} is not reachable"),
            )),
            Some(None) => Ok(V::Null), // Unmatched side of a left join.
            Some(Some(r)) => r.get(column).cloned().ok_or_else(|| {
                err(
                    "UNKNOWN_COLUMN",
                    "SEM-TRANS-001",
                    format!("{input}.{column}"),
                )
            }),
        }
    }
    fn feature(&self, feature: &str, _: &str) -> Result<V, EvalError> {
        Err(err(
            "UNSUPPORTED_ROW_LEVEL_DEPENDENCY",
            "SEM-ID-003",
            feature.to_string(),
        ))
    }
}

/// Adds the core's static typing to a value environment (for integer→number widening).
struct Typed<'a, E: ValueEnv> {
    inner: &'a E,
    p: &'a Project,
    f: &'a Feature,
    types: std::rc::Rc<TypeCache>,
}

/// Memoized static types per (feature, expression node) for one evaluation: the type checker
/// runs once per node instead of once per row. Keys are addresses of nodes owned by the project,
/// which outlives the evaluation.
#[derive(Default)]
struct TypeCache(std::cell::RefCell<HashMap<(usize, usize), Option<LogicalType>>>);

impl TypeCache {
    fn get(&self, p: &Project, f: &Feature, e: &Expr) -> Option<LogicalType> {
        let key = (f as *const Feature as usize, e as *const Expr as usize);
        if let Some(t) = self.0.borrow().get(&key) {
            return *t;
        }
        let t = crate::validate::expr_static_type(p, f, e);
        self.0.borrow_mut().insert(key, t);
        t
    }
}

impl<E: ValueEnv> ValueEnv for Typed<'_, E> {
    fn column(&self, input: &str, column: &str) -> Result<V, EvalError> {
        self.inner.column(input, column)
    }
    fn feature(&self, feature: &str, version: &str) -> Result<V, EvalError> {
        self.inner.feature(feature, version)
    }
    fn static_type(&self, e: &Expr) -> Option<LogicalType> {
        self.types.get(self.p, self.f, e)
    }
}

struct DepEnv<'a> {
    deps: &'a HashMap<String, FeatureOutput>,
    index: usize,
}

impl ValueEnv for DepEnv<'_> {
    fn column(&self, input: &str, column: &str) -> Result<V, EvalError> {
        Err(err(
            "EXPRESSION_SOURCE_INPUT",
            "SEM-TRANS-001",
            format!("{input}.{column}"),
        ))
    }
    fn feature(&self, feature: &str, _: &str) -> Result<V, EvalError> {
        self.deps
            .get(feature)
            .map(|o| o.values[self.index].0.clone())
            .ok_or_else(|| err("UNKNOWN_FEATURE", "SEM-ID-003", feature.to_string()))
    }
}

fn empty(f: &Feature) -> Result<(V, String), EvalError> {
    match &f.missingness.empty {
        EmptyPolicy::Error => Err(err(
            "EMPTY_FEATURE",
            "SEM-MISS-002",
            format!("{}: no eligible rows and empty action error", f.id),
        )),
        EmptyPolicy::Null => Ok((V::Null, "empty".into())),
        EmptyPolicy::Value { value } => Ok((
            V::from_json(value, f.output.ty)
                .map_err(|m| err("TYPE_MISMATCH", "SEM-MISS-002", m))?,
            "empty".into(),
        )),
    }
}

fn missing(f: &Feature, state: &str) -> Result<(V, String), EvalError> {
    let policy = if state == "source_unavailable" {
        f.missingness.source_unavailable
    } else {
        f.missingness.insufficient_history
    };
    match policy {
        NullOrError::Error => Err(err(
            &state.to_ascii_uppercase(),
            "SEM-MISS-001",
            format!("{}: {} with policy error", f.id, state.replace('_', " ")),
        )),
        NullOrError::Null => Ok((V::Null, state.into())),
    }
}

/// Correctly rounded sum of binary64 values (Shewchuk / Python `math.fsum`).
pub fn fsum(values: &[f64]) -> f64 {
    let mut partials: Vec<f64> = Vec::new();
    for &x0 in values {
        let mut x = x0;
        let mut i = 0;
        for j in 0..partials.len() {
            let mut y = partials[j];
            if x.abs() < y.abs() {
                std::mem::swap(&mut x, &mut y);
            }
            let hi = x + y;
            let lo = y - (hi - x);
            if lo != 0.0 {
                partials[i] = lo;
                i += 1;
            }
            x = hi;
        }
        partials.truncate(i);
        partials.push(x);
    }
    // Final rounding with half-even correction.
    let mut n = partials.len();
    if n == 0 {
        return 0.0;
    }
    n -= 1;
    let mut hi = partials[n];
    let mut lo = 0.0;
    while n > 0 {
        let x = hi;
        n -= 1;
        let y = partials[n];
        hi = x + y;
        let yr = hi - x;
        lo = y - yr;
        if lo != 0.0 {
            break;
        }
    }
    if n > 0 && ((lo < 0.0 && partials[n - 1] < 0.0) || (lo > 0.0 && partials[n - 1] > 0.0)) {
        let y = lo * 2.0;
        let x = hi + y;
        if y == x - hi {
            hi = x;
        }
    }
    hi
}

fn aggregate(op: AggregateOp, vals: &[V]) -> Result<V, EvalError> {
    let all_int = vals.iter().all(|v| matches!(v, V::Int(_)));
    let as_f = |v: &V| match v {
        V::Int(i) => *i as f64,
        V::Num(f) => *f,
        _ => f64::NAN,
    };
    let check = |f: f64| {
        if f.is_finite() {
            Ok(V::Num(f))
        } else {
            Err(err(
                "NON_FINITE_RESULT",
                "SEM-TRANS-006",
                "Aggregate produced a non-finite number",
            ))
        }
    };
    match op {
        AggregateOp::Sum => {
            if all_int {
                let s: i128 = vals
                    .iter()
                    .map(|v| if let V::Int(i) = v { i128::from(*i) } else { 0 })
                    .sum();
                i64::try_from(s).map(V::Int).map_err(|_| {
                    err(
                        "NUMERIC_OVERFLOW",
                        "SEM-TRANS-006",
                        "Aggregate signed 64-bit overflow",
                    )
                })
            } else {
                check(fsum(&vals.iter().map(as_f).collect::<Vec<_>>()))
            }
        }
        AggregateOp::Mean => {
            if all_int {
                let s: i128 = vals
                    .iter()
                    .map(|v| if let V::Int(i) = v { i128::from(*i) } else { 0 })
                    .sum();
                check(s as f64 / vals.len() as f64)
            } else {
                check(fsum(&vals.iter().map(as_f).collect::<Vec<_>>()) / vals.len() as f64)
            }
        }
        AggregateOp::Min | AggregateOp::Max => {
            let mut best = vals[0].clone();
            for v in &vals[1..] {
                let ord = compare(v, &best).ok_or_else(|| {
                    err(
                        "TYPE_MISMATCH",
                        "SEM-TRANS-004",
                        "Incomparable aggregate values",
                    )
                })?;
                if (op == AggregateOp::Min && ord == Ordering::Less)
                    || (op == AggregateOp::Max && ord == Ordering::Greater)
                {
                    best = v.clone();
                }
            }
            Ok(best)
        }
        _ => Err(err(
            "UNSUPPORTED_CAPABILITY",
            "SEM-TRANS-004",
            format!("{op:?}"),
        )),
    }
}

/// Stage 9: output type, constraints and quality checks.
fn check_output(f: &Feature, v: &V, state: &str) -> Result<(), EvalError> {
    let o = &f.output;
    let type_ok = match v {
        V::Null => o.nullable,
        V::Int(_) => o.ty == LogicalType::Integer,
        V::Num(x) => o.ty == LogicalType::Number && x.is_finite(),
        V::Bool(_) => o.ty == LogicalType::Boolean,
        V::Str(_) => o.ty == LogicalType::String,
        V::Ts(_) => o.ty == LogicalType::Timestamp,
        V::Date(_) => o.ty == LogicalType::Date,
    };
    if !type_ok {
        return Err(err(
            "OUTPUT_TYPE_MISMATCH",
            "SEM-OUT-001",
            format!(
                "{}: {} value violates output type/nullability {}{}",
                f.id,
                v.type_name(),
                o.ty.as_str(),
                if o.nullable { "?" } else { "" }
            ),
        ));
    }
    let num = match v {
        V::Int(i) => Some(*i as f64),
        V::Num(x) => Some(*x),
        _ => None,
    };
    // Exact comparison against binary64 bounds (no lossy i64→f64 cast).
    let below = |m: f64| match v {
        V::Int(i) => crate::value::cmp_int_num(*i, m) == Ordering::Less,
        V::Num(x) => *x < m,
        _ => false,
    };
    let above = |m: f64| match v {
        V::Int(i) => crate::value::cmp_int_num(*i, m) == Ordering::Greater,
        V::Num(x) => *x > m,
        _ => false,
    };
    if o.constraints.minimum.is_some_and(below) {
        return Err(err(
            "QUALITY_VIOLATION",
            "SEM-OUT-001",
            format!("{}: output below minimum", f.id),
        ));
    }
    if o.constraints.maximum.is_some_and(above) {
        return Err(err(
            "QUALITY_VIOLATION",
            "SEM-OUT-001",
            format!("{}: output above maximum", f.id),
        ));
    }
    if let Some(allowed) = &o.constraints.enumeration {
        if !v.is_null()
            && !allowed
                .iter()
                .any(|a| V::from_json(a, o.ty).is_ok_and(|a| identical(&a, v)))
        {
            return Err(err(
                "QUALITY_VIOLATION",
                "SEM-OUT-001",
                format!("{}: output outside enum", f.id),
            ));
        }
    }
    for q in &f.quality.checks {
        let violated = match q {
            QualityCheck::NotNull { .. } => (state == "ok" || state == "empty") && v.is_null(),
            QualityCheck::Range {
                minimum, maximum, ..
            } => minimum.is_some_and(below) || maximum.is_some_and(above),
            QualityCheck::Finite { .. } => {
                matches!(v, V::Num(x) if !x.is_finite()) || (!v.is_null() && num.is_none())
            }
        };
        if violated {
            if q.severity() == "warn" || f.quality.on_violation == "quarantine" {
                return Err(err("UNSUPPORTED_CAPABILITY", "SEM-QUAL-001", "Warning/quarantine quality outcomes require an artifact sink this evaluator does not provide"));
            }
            return Err(err(
                "QUALITY_VIOLATION",
                "SEM-QUAL-001",
                format!("{}: declared quality check failed", f.id),
            ));
        }
    }
    Ok(())
}

fn key_row(entity: &Entity, key: &[V]) -> Map<String, Json> {
    entity
        .keys
        .iter()
        .zip(key)
        .map(|(k, v)| (k.name.clone(), v.to_json()))
        .collect()
}

/// Evaluate one feature for one request (source-as-known or replay).
pub fn evaluate(
    p: &Project,
    feature_id: &str,
    req: &Request,
    limits: &Limits,
) -> Result<Evaluation, EvalError> {
    let mut ev = Evaluator {
        p,
        req,
        limits,
        trace: Vec::new(),
        validated: BTreeMap::new(),
        reconstructed: HashMap::new(),
        types: Default::default(),
        work: 0,
    };
    let target = ev.feature_by_id(feature_id)?;
    let entity = p
        .entity(&target.entity)
        .ok_or_else(|| err("UNKNOWN_ENTITY", "SEM-ID-002", target.entity.clone()))?;
    // Stage 1 for every direct, transitive and population source.
    for sid in feature_sources(p, feature_id) {
        ev.status(&sid)?;
        if !req.source_rows.contains_key(&sid) {
            return Err(err(
                "MISSING_SOURCE_ROWS",
                "SEM-TEST-001",
                format!("No rows (not even an explicit empty list) supplied for {sid}"),
            ));
        }
        if !matches!(ev.status(&sid)?, SourceStatus::Unavailable { .. }) {
            ev.validate_rows(&sid)?;
        }
    }
    let population = ev.population(entity)?;
    if population.len() > limits.max_output_rows {
        return Err(err(
            "RESOURCE_LIMIT_EXCEEDED",
            "SEM-BUDGET-001",
            "Population exceeds the output-row limit",
        ));
    }
    if req.mode == SelectionMode::HistoricalServingReplay {
        let rows = replay(p, target, entity, &population, req)?;
        ev.note(
            &target.id,
            "replay",
            format!(
                "{} entities selected from observed publications",
                rows.len()
            ),
        );
        return Ok(Evaluation {
            rows,
            trace: ev.trace,
        });
    }
    // Reconstruct every available input source of every feature, in plan order (primary input,
    // then join targets), before any transform: reconstruction errors therefore surface in the
    // same canonical order as the generated statements.
    for fid in dependency_order(p, &[feature_id.to_string()]) {
        let f = ev.feature_by_id(&fid)?;
        if let Some((input, joins, _, _)) = f.transform.row_shape() {
            let mut aliases = vec![input.to_string()];
            aliases.extend(joins.iter().map(|j| j.right_alias.clone()));
            for a in aliases {
                let Some(inp) = f.input(&a) else { continue };
                if matches!(ev.status(&inp.source)?, SourceStatus::Available { .. }) {
                    ev.reconstruct(&f.id, &inp.source, f.time.availability.upper)?;
                }
            }
        }
    }
    let mut outputs: HashMap<String, FeatureOutput> = HashMap::new();
    for fid in dependency_order(p, &[feature_id.to_string()]) {
        let f = ev.feature_by_id(&fid)?;
        let deps: HashMap<String, FeatureOutput> = f
            .dependencies
            .iter()
            .filter_map(|d| {
                outputs
                    .get(&d.feature)
                    .map(|o| (d.feature.clone(), o.clone()))
            })
            .collect();
        let mut out = ev.compute(f, &population, &deps, entity)?;
        // Integer results declared as number outputs widen (SEM-TRANS-006).
        for (v, _) in out.values.iter_mut() {
            *v = expr::conform(std::mem::replace(v, V::Null), Some(f.output.ty));
        }
        for (v, state) in &out.values {
            check_output(f, v, state)?;
        }
        outputs.insert(fid, out);
    }
    let rows = population
        .iter()
        .zip(&outputs[feature_id].values)
        .map(|(k, (v, s))| OutputRow {
            entity_keys: key_row(entity, k),
            value: v.to_json(),
            state: s.clone(),
        })
        .collect();
    Ok(Evaluation {
        rows,
        trace: ev.trace,
    })
}

/// Historical-serving replay from observed publications (SEM-REPLAY-001/002).
fn replay(
    p: &Project,
    f: &Feature,
    entity: &Entity,
    population: &[Vec<V>],
    req: &Request,
) -> Result<Vec<OutputRow>, EvalError> {
    let (Some(published), Some(requested), Some(cid)) =
        (req.published_values, req.requested_at, req.consumer)
    else {
        return Err(err("REPLAY_CONTEXT_REQUIRED", "SEM-REPLAY-001", "Replay requires observed publications, requested_at and a consumer; recomputation is not proof of historical serving"));
    };
    let consumer = p
        .consumer(cid)
        .ok_or_else(|| err("UNKNOWN_CONSUMER", "SEM-ID-002", cid.to_string()))?;
    if consumer.selection.mode != SelectionMode::HistoricalServingReplay {
        return Err(err(
            "CONSUMER_MODE_MISMATCH",
            "SEM-REPLAY-001",
            "Selected consumer does not permit replay",
        ));
    }
    let fs = p
        .feature_set(&consumer.feature_set.feature_set)
        .ok_or_else(|| {
            err(
                "UNKNOWN_FEATURE_SET",
                "SEM-ID-002",
                consumer.feature_set.feature_set.clone(),
            )
        })?;
    if !fs
        .features
        .iter()
        .any(|m| m.id == f.id && m.version == f.version)
    {
        return Err(err(
            "CONSUMER_FEATURE_MISMATCH",
            "SEM-REPLAY-002",
            "Feature is not a pinned member of the consumer feature set",
        ));
    }
    let records: Vec<crate::consumer::PublicationRecord> = published
        .iter()
        .filter(|pv| pv.feature == f.id && pv.version == f.version)
        .map(|pv| crate::consumer::PublicationRecord::from_fixture(pv, entity, f))
        .collect::<Result<_, _>>()?;
    let mut out = Vec::new();
    for key in population {
        let sel = crate::consumer::select_replay(
            &records,
            key,
            req.as_of,
            requested,
            consumer.max_staleness_seconds,
        )?;
        let (value, state) = match sel {
            None => (V::Null, "not_published".to_string()),
            Some(r) => (r.value.clone(), r.state.clone()),
        };
        if value.is_null() && consumer.missing_value_policy == MissingValuePolicy::Reject {
            return Err(err(
                "MISSING_FEATURE_VALUE",
                "SEM-FRESH-002",
                format!(
                    "Consumer rejects the selected null ({state}); no fallback to an older value"
                ),
            ));
        }
        out.push(OutputRow {
            entity_keys: key_row(entity, key),
            value: value.to_json(),
            state,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::fsum;

    #[test]
    fn fsum_is_exact() {
        assert_eq!(fsum(&[0.1; 10]), 1.0);
        assert_eq!(fsum(&[1e100, 1.0, -1e100]), 1.0);
        assert_eq!(fsum(&[]), 0.0);
    }
}

/// Reconstruct one source as known at `cutoff` (stages 1–5), for reuse by
/// label construction and other bounded local paths.
pub fn reconstruct_rows(
    p: &Project,
    source_id: &str,
    source_rows: &Map<String, Json>,
    cutoff: Instant,
    upper: Boundary,
    limits: &Limits,
) -> Result<Vec<TRow>, EvalError> {
    let req = Request {
        as_of: cutoff,
        source_rows,
        source_status: &[],
        expected_population: None,
        mode: SelectionMode::SourceAsKnown,
        requested_at: None,
        published_values: None,
        consumer: None,
        capabilities: &[],
    };
    let mut ev = Evaluator {
        p,
        req: &req,
        limits,
        trace: Vec::new(),
        validated: BTreeMap::new(),
        reconstructed: HashMap::new(),
        types: Default::default(),
        work: 0,
    };
    ev.reconstruct("", source_id, upper)
}
