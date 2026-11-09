//! Reference, type, lineage and binding validation (stage 3).
//!
//! Runs only on a schema-valid, model-conforming project. Every diagnostic
//! carries a JSON-pointer path (by array index, so it maps to a source
//! location), the SEM-* rule it enforces and the related object IDs.

use crate::diag::{Diagnostic, Stage};
use crate::expr::{self, Ty, TypeEnv};
use crate::model::*;
use crate::time::Instant;
use crate::value::V;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

struct Ctx<'a> {
    p: &'a Project,
    out: Vec<Diagnostic>,
}

impl<'a> Ctx<'a> {
    fn err(
        &mut self,
        code: &str,
        rule: &str,
        path: impl Into<String>,
        msg: impl Into<String>,
        related: &[&str],
    ) {
        self.out.push(
            Diagnostic::new(code, Stage::References, rule, path, msg)
                .related(related.iter().map(|s| s.to_string())),
        );
    }
    fn warn(
        &mut self,
        code: &str,
        rule: &str,
        path: impl Into<String>,
        msg: impl Into<String>,
        related: &[&str],
    ) {
        self.out.push(
            Diagnostic::new(code, Stage::References, rule, path, msg)
                .related(related.iter().map(|s| s.to_string()))
                .warning(),
        );
    }
    fn ts(&mut self, value: &str, path: String, related: &[&str]) -> Option<Instant> {
        match Instant::parse(value) {
            Ok(t) => Some(t),
            Err(m) => {
                self.err("INVALID_TIMESTAMP", "SEM-TIME-003", path, m, related);
                None
            }
        }
    }
}

fn idx<T>(items: &[T], f: impl Fn(&T) -> &str, id: &str) -> Option<usize> {
    items.iter().position(|x| f(x) == id)
}

/// All logical source IDs a feature reads, directly, through dependencies and its population.
pub fn feature_sources(p: &Project, feature_id: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut seen = HashSet::new();
    fn walk(p: &Project, id: &str, out: &mut BTreeSet<String>, seen: &mut HashSet<String>) {
        if !seen.insert(id.to_string()) {
            return;
        }
        if let Some(f) = p.feature(id) {
            out.extend(f.inputs.iter().map(|i| i.source.clone()));
            if let Some(e) = p.entity(&f.entity) {
                out.insert(e.population.source.clone());
            }
            for d in &f.dependencies {
                walk(p, &d.feature, out, seen);
            }
        }
    }
    walk(p, feature_id, &mut out, &mut seen);
    out
}

/// Every feature ID in the transitive dependency closure, dependencies first.
pub fn dependency_order(p: &Project, roots: &[String]) -> Vec<String> {
    let mut order = Vec::new();
    let mut seen = HashSet::new();
    fn visit(p: &Project, id: &str, order: &mut Vec<String>, seen: &mut HashSet<String>) {
        if !seen.insert(id.to_string()) {
            return;
        }
        if let Some(f) = p.feature(id) {
            for d in &f.dependencies {
                visit(p, &d.feature, order, seen);
            }
        }
        order.push(id.to_string());
    }
    for r in roots {
        visit(p, r, &mut order, &mut seen);
    }
    order
}

struct FeatureEnv<'a> {
    p: &'a Project,
    f: &'a Feature,
    allow_columns: bool,
    allow_features: bool,
    used_columns: std::cell::RefCell<Vec<(String, String)>>,
    used_features: std::cell::RefCell<Vec<(String, String)>>,
    /// Aliases on the nullable side of a left join: their columns may be null.
    nullable_aliases: HashSet<String>,
}

impl TypeEnv for FeatureEnv<'_> {
    fn column(&self, input: &str, column: &str) -> Result<Ty, (String, String)> {
        if !self.allow_columns {
            return Err(("EXPRESSION_SOURCE_INPUT".into(), format!("Entity-level expressions cannot read source column {input}.{column}; use pinned feature dependencies")));
        }
        let inp = self.f.input(input).ok_or_else(|| {
            (
                "UNKNOWN_INPUT".to_string(),
                format!("Unknown input alias {input}"),
            )
        })?;
        if !inp.columns.iter().any(|c| c == column) {
            return Err((
                "UNDECLARED_INPUT_COLUMN".into(),
                format!("Column {column} is not declared in input {input}"),
            ));
        }
        let src = self
            .p
            .source(&inp.source)
            .ok_or_else(|| ("UNKNOWN_SOURCE".to_string(), inp.source.clone()))?;
        let col = src.column(column).ok_or_else(|| {
            (
                "UNKNOWN_COLUMN".to_string(),
                format!("Unknown source column {}.{column}", src.id),
            )
        })?;
        self.used_columns
            .borrow_mut()
            .push((input.to_string(), column.to_string()));
        Ok(Ty::of(
            col.ty,
            col.nullable || self.nullable_aliases.contains(input),
        ))
    }
    fn feature(&self, feature: &str, version: &str) -> Result<Ty, (String, String)> {
        if !self.allow_features {
            return Err(("UNSUPPORTED_ROW_LEVEL_DEPENDENCY".into(), format!("Row-level expressions cannot read feature {feature}; only same-entity as-of dependencies in entity-level transforms are supported")));
        }
        let target = self.p.feature(feature).ok_or_else(|| {
            (
                "UNKNOWN_FEATURE".to_string(),
                format!("Unknown feature {feature}"),
            )
        })?;
        if target.version != version {
            return Err((
                "VERSION_MISMATCH".into(),
                format!(
                    "Expression pins {feature}@{version}; project has {}",
                    target.version
                ),
            ));
        }
        if !self
            .f
            .dependencies
            .iter()
            .any(|d| d.feature == feature && d.version == version)
        {
            return Err((
                "UNDECLARED_DEPENDENCY".into(),
                format!(
                    "Expression reads {feature}@{version} without a matching declared dependency"
                ),
            ));
        }
        self.used_features
            .borrow_mut()
            .push((feature.to_string(), version.to_string()));
        Ok(Ty::of(target.output.ty, target.output.nullable))
    }
}

pub fn semantic_diagnostics(p: &Project) -> Vec<Diagnostic> {
    let mut c = Ctx { p, out: Vec::new() };
    check_ids(&mut c);
    check_sources(&mut c);
    check_entities(&mut c);
    check_relationships(&mut c);
    for (i, f) in p.features.iter().enumerate() {
        check_feature(&mut c, i, f);
    }
    check_cycles(&mut c);
    check_feature_sets_and_consumers(&mut c);
    for (i, d) in p.deployments.iter().enumerate() {
        check_deployment(&mut c, i, d);
    }
    check_suites(&mut c);
    c.out
}

fn check_ids(c: &mut Ctx) {
    let p = c.p;
    let collections: Vec<(&str, Vec<&str>)> = vec![
        (
            "entities",
            p.entities.iter().map(|x| x.id.as_str()).collect(),
        ),
        ("sources", p.sources.iter().map(|x| x.id.as_str()).collect()),
        (
            "relationships",
            p.relationships.iter().map(|x| x.id.as_str()).collect(),
        ),
        (
            "features",
            p.features.iter().map(|x| x.id.as_str()).collect(),
        ),
        (
            "feature_sets",
            p.feature_sets.iter().map(|x| x.id.as_str()).collect(),
        ),
        (
            "consumers",
            p.consumers.iter().map(|x| x.id.as_str()).collect(),
        ),
        (
            "deployments",
            p.deployments.iter().map(|x| x.id.as_str()).collect(),
        ),
        (
            "acceptance_suites",
            p.acceptance_suites.iter().map(|x| x.id.as_str()).collect(),
        ),
    ];
    for (name, ids) in collections {
        let mut seen = HashSet::new();
        for (i, id) in ids.iter().enumerate() {
            if !seen.insert(*id) {
                c.err(
                    "DUPLICATE_ID",
                    "SEM-ID-001",
                    format!("/{name}/{i}/id"),
                    format!(
                        "Duplicate {name} id {id}; each ID appears once (one version per project)"
                    ),
                    &[id],
                );
            }
        }
    }
}

fn check_sources(c: &mut Ctx) {
    let p = c.p;
    for (si, s) in p.sources.iter().enumerate() {
        let base = format!("/sources/{si}");
        let mut names = HashSet::new();
        for (ci, col) in s.columns.iter().enumerate() {
            if !names.insert(col.name.as_str()) {
                c.err(
                    "DUPLICATE_COLUMN",
                    "SEM-SRC-001",
                    format!("{base}/columns/{ci}/name"),
                    format!("Duplicate column {}", col.name),
                    &[&s.id],
                );
            }
        }
        let check_col = |c: &mut Ctx, name: &str, path: String| -> Option<Column> {
            match s.column(name) {
                Some(col) => Some(col.clone()),
                None => {
                    c.err(
                        "UNKNOWN_COLUMN",
                        "SEM-SRC-001",
                        path,
                        format!("Unknown source column {}.{name}", s.id),
                        &[&s.id],
                    );
                    None
                }
            }
        };
        for (k, name) in s.primary_key.iter().enumerate() {
            if let Some(col) = check_col(c, name, format!("{base}/primary_key/{k}")) {
                if col.nullable {
                    c.err(
                        "NULLABLE_IDENTITY",
                        "SEM-SRC-002",
                        format!("{base}/primary_key/{k}"),
                        format!("Identity column {}.{name} must be non-null", s.id),
                        &[&s.id],
                    );
                }
            }
        }
        if s.primary_key.len() != s.primary_key.iter().collect::<HashSet<_>>().len() {
            c.err(
                "DUPLICATE_KEY_COLUMN",
                "SEM-SRC-002",
                format!("{base}/primary_key"),
                "Primary key repeats a column",
                &[&s.id],
            );
        }
        for (k, name) in s.record_semantics.logical_key().iter().enumerate() {
            if let Some(col) =
                check_col(c, name, format!("{base}/record_semantics/logical_key/{k}"))
            {
                if col.nullable {
                    c.err(
                        "NULLABLE_IDENTITY",
                        "SEM-SRC-002",
                        format!("{base}/record_semantics/logical_key/{k}"),
                        format!("Logical key column {}.{name} must be non-null", s.id),
                        &[&s.id],
                    );
                }
            }
        }
        for (field, name) in [
            ("event_time", &s.time.event_time),
            ("available_time", &s.time.available_time),
        ] {
            if let Some(col) = check_col(c, name, format!("{base}/time/{field}")) {
                if col.ty != LogicalType::Timestamp || col.nullable {
                    c.err(
                        "INVALID_TIME_COLUMN",
                        "SEM-SRC-001",
                        format!("{base}/time/{field}"),
                        format!("{field} must identify a non-null UTC timestamp column"),
                        &[&s.id],
                    );
                }
            }
        }
        if let Some(rev) = s.record_semantics.revision() {
            for (k, o) in rev.order_by.iter().enumerate() {
                if let Some(col) = check_col(
                    c,
                    &o.column,
                    format!("{base}/record_semantics/revision/order_by/{k}/column"),
                ) {
                    if col.nullable {
                        c.err(
                            "NULLABLE_REVISION",
                            "SEM-REV-001",
                            format!("{base}/record_semantics/revision/order_by/{k}"),
                            "Revision ordering columns must be non-null",
                            &[&s.id],
                        );
                    }
                }
            }
            match (&rev.tombstone_column, rev.tombstone_value) {
                (Some(tc), Some(_)) => {
                    if let Some(col) = check_col(
                        c,
                        tc,
                        format!("{base}/record_semantics/revision/tombstone_column"),
                    ) {
                        if col.ty != LogicalType::Boolean || col.nullable {
                            c.err("TYPE_MISMATCH", "SEM-SRC-001", format!("{base}/record_semantics/revision/tombstone_column"), "Tombstone column must be a non-null boolean matching tombstone_value", &[&s.id]);
                        }
                    }
                }
                (None, None) => {}
                _ => c.err(
                    "TOMBSTONE_INCOMPLETE",
                    "SEM-REV-001",
                    format!("{base}/record_semantics/revision"),
                    "tombstone_column and tombstone_value must be declared together",
                    &[&s.id],
                ),
            }
        }
        let upstream: HashSet<&str> = s
            .lineage
            .upstream_columns
            .iter()
            .map(|u| u.column.as_str())
            .collect();
        for col in &s.columns {
            if !upstream.contains(col.name.as_str()) {
                c.err(
                    "MISSING_SOURCE_LINEAGE",
                    "SEM-SRC-004",
                    format!("{base}/lineage/upstream_columns"),
                    format!("Column {} has no upstream provenance", col.name),
                    &[&s.id],
                );
            }
        }
        for (k, u) in s.lineage.upstream_columns.iter().enumerate() {
            check_col(
                c,
                &u.column,
                format!("{base}/lineage/upstream_columns/{k}/column"),
            );
        }
        c.ts(
            &s.retention.earliest_reproducible_at,
            format!("{base}/retention/earliest_reproducible_at"),
            &[&s.id],
        );
    }
}

fn check_entities(c: &mut Ctx) {
    let p = c.p;
    for (ei, e) in p.entities.iter().enumerate() {
        let base = format!("/entities/{ei}");
        let src = p.source(&e.population.source);
        if src.is_none() {
            c.err(
                "UNKNOWN_SOURCE",
                "SEM-ID-002",
                format!("{base}/population/source"),
                format!("Unknown population source {}", e.population.source),
                &[&e.id],
            );
        }
        let mut seen = HashSet::new();
        for (ki, k) in e.keys.iter().enumerate() {
            if !seen.insert(k.name.as_str()) {
                c.err(
                    "DUPLICATE_ENTITY_KEY",
                    "SEM-ENT-001",
                    format!("{base}/keys/{ki}"),
                    format!("Duplicate entity key {}", k.name),
                    &[&e.id],
                );
            }
            if let Some(s) = src {
                match s.column(&k.source_column) {
                    None => c.err(
                        "UNKNOWN_COLUMN",
                        "SEM-SRC-001",
                        format!("{base}/keys/{ki}/source_column"),
                        format!("Unknown population column {}.{}", s.id, k.source_column),
                        &[&e.id, &s.id],
                    ),
                    Some(col) if col.ty != k.ty || col.nullable => c.err(
                        "ENTITY_KEY_TYPE",
                        "SEM-ENT-001",
                        format!("{base}/keys/{ki}"),
                        "Entity key must match a non-null population column of the same type",
                        &[&e.id, &s.id],
                    ),
                    _ => {}
                }
            }
        }
        if let Some(s) = src {
            let mapped: HashSet<&str> = e.keys.iter().map(|k| k.source_column.as_str()).collect();
            let logical: HashSet<&str> = s
                .record_semantics
                .logical_key()
                .iter()
                .map(String::as_str)
                .collect();
            if !mapped.is_subset(&logical) && !logical.is_subset(&mapped) {
                c.warn("POPULATION_KEY_GRAIN", "SEM-ENT-001", format!("{base}/keys"), "Entity keys and the population source logical key differ; duplicate keys will be rejected at evaluation", &[&e.id, &s.id]);
            }
        }
    }
}

fn tenant_keys(p: &Project) -> BTreeSet<String> {
    p.entities
        .iter()
        .flat_map(|e| {
            e.keys
                .iter()
                .filter(|k| k.is_tenant)
                .map(|k| k.name.clone())
        })
        .collect()
}

fn check_relationships(c: &mut Ctx) {
    let p = c.p;
    let tenants = tenant_keys(p);
    for (ri, r) in p.relationships.iter().enumerate() {
        let base = format!("/relationships/{ri}");
        let ls = p.source(&r.from.source);
        let rs = p.source(&r.to.source);
        for (side, endpoint, src) in [("from", &r.from, ls), ("to", &r.to, rs)] {
            if src.is_none() {
                c.err(
                    "UNKNOWN_SOURCE",
                    "SEM-ID-002",
                    format!("{base}/{side}/source"),
                    format!("Unknown relationship source {}", endpoint.source),
                    &[&r.id],
                );
            }
        }
        if r.from.columns.len() != r.to.columns.len() {
            c.err(
                "RELATIONSHIP_ARITY",
                "SEM-REL-001",
                base.clone(),
                "Relationship endpoint key counts differ",
                &[&r.id],
            );
        }
        if let (Some(ls), Some(rs)) = (ls, rs) {
            for (k, (a, b)) in r.from.columns.iter().zip(r.to.columns.iter()).enumerate() {
                let ca = ls.column(a);
                let cb = rs.column(b);
                if ca.is_none() {
                    c.err(
                        "UNKNOWN_COLUMN",
                        "SEM-REL-001",
                        format!("{base}/from/columns/{k}"),
                        format!("Unknown column {}.{a}", ls.id),
                        &[&r.id],
                    );
                }
                if cb.is_none() {
                    c.err(
                        "UNKNOWN_COLUMN",
                        "SEM-REL-001",
                        format!("{base}/to/columns/{k}"),
                        format!("Unknown column {}.{b}", rs.id),
                        &[&r.id],
                    );
                }
                if let (Some(ca), Some(cb)) = (ca, cb) {
                    if ca.ty != cb.ty {
                        c.err(
                            "RELATIONSHIP_KEY_TYPE",
                            "SEM-REL-001",
                            format!("{base}/to/columns/{k}"),
                            format!(
                                "Join key types differ: {} vs {}",
                                ca.ty.as_str(),
                                cb.ty.as_str()
                            ),
                            &[&r.id],
                        );
                    }
                }
            }
            for t in &tenants {
                if ls.column(t).is_some()
                    && rs.column(t).is_some()
                    && !r
                        .from
                        .columns
                        .iter()
                        .zip(&r.to.columns)
                        .any(|(a, b)| a == t && b == t)
                {
                    c.err(
                        "TENANT_JOIN_MISSING",
                        "SEM-ENT-002",
                        base.clone(),
                        format!(
                            "Both sides contain tenant key {t}, but it is not part of the join"
                        ),
                        &[&r.id],
                    );
                }
            }
            if let RelationshipTemporal::AsOf {
                valid_from,
                valid_to,
                ..
            } = &r.temporal
            {
                for (name, col) in [("valid_from", valid_from), ("valid_to", valid_to)] {
                    match rs.column(col) {
                        None => c.err("UNKNOWN_COLUMN", "SEM-REL-004", format!("{base}/temporal/{name}"), format!("Unknown column {}.{col}", rs.id), &[&r.id]),
                        Some(cc) if cc.ty != LogicalType::Timestamp || (name == "valid_from" && cc.nullable) => {
                            c.err("INVALID_TIME_COLUMN", "SEM-REL-004", format!("{base}/temporal/{name}"), "Historical intervals require timestamp columns and a non-null valid_from", &[&r.id])
                        }
                        _ => {}
                    }
                }
                let lk: HashSet<&str> = rs
                    .record_semantics
                    .logical_key()
                    .iter()
                    .map(String::as_str)
                    .collect();
                let jk: HashSet<&str> = r.to.columns.iter().map(String::as_str).collect();
                if rs.record_semantics.revision().is_some() && lk.is_subset(&jk) {
                    c.err("DIMENSION_INTERVAL_IDENTITY", "SEM-REV-003", format!("{base}/to"), "A historical dimension's logical key must distinguish valid-time intervals beyond the join key", &[&r.id, &rs.id]);
                }
            }
        }
        let conflict = matches!(
            (r.join_type, r.unmatched),
            (JoinType::Inner, Unmatched::PreserveNull) | (JoinType::Left, Unmatched::Drop)
        );
        if conflict {
            c.err(
                "JOIN_POLICY_CONFLICT",
                "SEM-REL-003",
                base.clone(),
                "Join type and unmatched behavior conflict",
                &[&r.id],
            );
        }
    }
}

fn required_lineage(
    p: &Project,
    f: &Feature,
) -> BTreeMap<(String, String), BTreeSet<&'static str>> {
    let mut req: BTreeMap<(String, String), BTreeSet<&'static str>> = BTreeMap::new();
    let mut add = |s: &str, col: &str, role: &'static str| {
        req.entry((s.to_string(), col.to_string()))
            .or_default()
            .insert(role);
    };
    let alias_source: HashMap<&str, &str> = f
        .inputs
        .iter()
        .map(|i| (i.alias.as_str(), i.source.as_str()))
        .collect();
    for i in &f.inputs {
        if let Some(s) = p.source(&i.source) {
            add(&s.id, &s.time.available_time, "availability_time");
            if let Some(rev) = s.record_semantics.revision() {
                for o in &rev.order_by {
                    add(&s.id, &o.column, "revision");
                }
                if let Some(t) = &rev.tombstone_column {
                    add(&s.id, t, "deletion");
                }
            }
        }
    }
    if let Some((input, joins, group_by, filter)) = f.transform.row_shape() {
        if let Some(s) = alias_source.get(input).and_then(|s| p.source(s)) {
            add(&s.id, &s.time.event_time, "event_time");
        }
        for g in group_by {
            if let Some(s) = alias_source.get(g.input.as_str()) {
                add(s, &g.column, "entity_key");
            }
        }
        for j in joins {
            if let Some(r) = p.relationship(&j.relationship) {
                for col in &r.from.columns {
                    add(&r.from.source, col, "join_key");
                }
                for col in &r.to.columns {
                    add(&r.to.source, col, "join_key");
                }
                if let RelationshipTemporal::AsOf {
                    valid_from,
                    valid_to,
                    ..
                } = &r.temporal
                {
                    add(&r.to.source, valid_from, "valid_time");
                    add(&r.to.source, valid_to, "valid_time");
                }
            }
        }
        let mut cols = Vec::new();
        if let Some(fl) = filter {
            collect_columns(fl, &mut cols);
        }
        for (a, col) in cols {
            if let Some(s) = alias_source.get(a.as_str()) {
                add(s, &col, "filter");
            }
        }
        let mut vals = Vec::new();
        match &f.transform {
            Transform::Aggregate {
                value, distinct_by, ..
            } => {
                if let Some(v) = value {
                    collect_columns(v, &mut vals);
                }
                for d in distinct_by {
                    collect_columns(d, &mut vals);
                }
            }
            Transform::Lookup {
                value, order_by, ..
            } => {
                collect_columns(value, &mut vals);
                for o in order_by {
                    if let Some(s) = alias_source.get(o.input.as_str()) {
                        add(s, &o.column, "ordering");
                    }
                }
            }
            Transform::Recency {
                input, event_time, ..
            } => {
                if let Some(s) = alias_source.get(input.as_str()) {
                    add(s, event_time, "event_time");
                }
            }
            _ => {}
        }
        for (a, col) in vals {
            if let Some(s) = alias_source.get(a.as_str()) {
                add(s, &col, "value");
            }
        }
    }
    if let Some(e) = p.entity(&f.entity) {
        for k in &e.keys {
            add(&e.population.source, &k.source_column, "population");
        }
        if let Some(s) = p.source(&e.population.source) {
            add(&s.id, &s.time.event_time, "population");
            add(&s.id, &s.time.available_time, "population");
        }
    }
    req
}

pub fn collect_columns(e: &Expr, out: &mut Vec<(String, String)>) {
    match e {
        Expr::Column { input, column } => out.push((input.clone(), column.clone())),
        Expr::Call { args, .. } => args.iter().for_each(|a| collect_columns(a, out)),
        _ => {}
    }
}

pub fn collect_features(e: &Expr, out: &mut Vec<(String, String)>) {
    match e {
        Expr::Feature { feature, version } => out.push((feature.clone(), version.clone())),
        Expr::Call { args, .. } => args.iter().for_each(|a| collect_features(a, out)),
        _ => {}
    }
}

/// Static type of any expression within a feature (core type checker; single authority).
pub fn expr_static_type(p: &Project, f: &Feature, e: &Expr) -> Option<LogicalType> {
    let env = feature_env(p, f);
    expr::check(e, &env, "", &mut Vec::new()).and_then(|t| t.ty)
}

/// Static output type implied by a feature's transform, if determinable.
pub fn transform_type(p: &Project, f: &Feature) -> Option<Ty> {
    let mut errors = Vec::new();
    let env = feature_env(p, f);
    match &f.transform {
        Transform::Aggregate {
            operation,
            value,
            nulls,
            ..
        } => match operation {
            AggregateOp::Count | AggregateOp::CountDistinct => Some(Ty::of(
                LogicalType::Integer,
                *nulls == NullPolicy::Propagate,
            )),
            AggregateOp::Mean => value
                .as_ref()
                .and_then(|v| expr::check(v, &env, "", &mut errors))
                .map(|t| Ty::of(LogicalType::Number, t.nullable)),
            _ => value
                .as_ref()
                .and_then(|v| expr::check(v, &env, "", &mut errors)),
        },
        Transform::Recency { .. } => Some(Ty::of(LogicalType::Integer, false)),
        Transform::Lookup { value, .. } => expr::check(value, &env, "", &mut errors),
        Transform::Expression { expression } => expr::check(expression, &env, "", &mut errors),
        Transform::Learned { contract, .. } | Transform::Custom { contract, .. } => {
            Some(Ty::of(contract.output_type, contract.output_nullable))
        }
    }
}

fn feature_env<'a>(p: &'a Project, f: &'a Feature) -> FeatureEnv<'a> {
    let mut nullable_aliases = HashSet::new();
    if let Some((_, joins, _, _)) = f.transform.row_shape() {
        for j in joins {
            if let Some(r) = p.relationship(&j.relationship) {
                if r.join_type == JoinType::Left {
                    nullable_aliases.insert(j.right_alias.clone());
                }
            }
        }
    }
    let row_level = f.transform.row_shape().is_some();
    FeatureEnv {
        p,
        f,
        allow_columns: row_level,
        allow_features: !row_level,
        used_columns: Default::default(),
        used_features: Default::default(),
        nullable_aliases,
    }
}

fn check_feature(c: &mut Ctx, fi: usize, f: &Feature) {
    let p = c.p;
    let base = format!("/features/{fi}");
    let fid = f.id.as_str();
    let entity = p.entity(&f.entity);
    if entity.is_none() {
        c.err(
            "UNKNOWN_ENTITY",
            "SEM-ID-002",
            format!("{base}/entity"),
            format!("Unknown entity {}", f.entity),
            &[fid],
        );
    }
    // Inputs and implicit identity/time/revision columns.
    let mut aliases = HashSet::new();
    for (ii, inp) in f.inputs.iter().enumerate() {
        if !aliases.insert(inp.alias.as_str()) {
            c.err(
                "DUPLICATE_INPUT_ALIAS",
                "SEM-REL-001",
                format!("{base}/inputs/{ii}/alias"),
                format!("Input alias {} is introduced more than once", inp.alias),
                &[fid],
            );
        }
        let Some(s) = p.source(&inp.source) else {
            c.err(
                "UNKNOWN_SOURCE",
                "SEM-ID-002",
                format!("{base}/inputs/{ii}/source"),
                format!("Unknown source {}", inp.source),
                &[fid],
            );
            continue;
        };
        for (k, col) in inp.columns.iter().enumerate() {
            if s.column(col).is_none() {
                c.err(
                    "UNKNOWN_COLUMN",
                    "SEM-TRANS-001",
                    format!("{base}/inputs/{ii}/columns/{k}"),
                    format!("Unknown source column {}.{col}", s.id),
                    &[fid, &s.id],
                );
            }
        }
        let mut implicit: Vec<String> = s.record_semantics.logical_key().to_vec();
        implicit.push(s.time.event_time.clone());
        implicit.push(s.time.available_time.clone());
        if let Some(rev) = s.record_semantics.revision() {
            implicit.extend(rev.order_by.iter().map(|o| o.column.clone()));
            implicit.extend(rev.tombstone_column.iter().cloned());
        }
        for col in implicit {
            if !inp.columns.contains(&col) {
                c.err(
                    "MISSING_INPUT_COLUMN",
                    "SEM-TRANS-001",
                    format!("{base}/inputs/{ii}/columns"),
                    format!(
                        "Required identity/time/revision column {}.{col} is not declared",
                        s.id
                    ),
                    &[fid, &s.id],
                );
            }
        }
    }
    // Dependencies.
    let mut dep_seen = HashSet::new();
    for (di, d) in f.dependencies.iter().enumerate() {
        if !dep_seen.insert(d.feature.as_str()) {
            c.err(
                "DUPLICATE_DEPENDENCY",
                "SEM-ID-003",
                format!("{base}/dependencies/{di}"),
                format!("Dependency {} declared twice", d.feature),
                &[fid],
            );
        }
        match p.feature(&d.feature) {
            None => c.err(
                "UNKNOWN_FEATURE",
                "SEM-ID-002",
                format!("{base}/dependencies/{di}/feature"),
                format!("Unknown feature {}", d.feature),
                &[fid],
            ),
            Some(t) => {
                if t.version != d.version {
                    c.err(
                        "VERSION_MISMATCH",
                        "SEM-ID-002",
                        format!("{base}/dependencies/{di}/version"),
                        format!(
                            "Requested {}@{}; project has {}",
                            d.feature, d.version, t.version
                        ),
                        &[fid, &d.feature],
                    );
                }
                if t.entity != f.entity {
                    c.err(
                        "DEPENDENCY_ENTITY_MISMATCH",
                        "SEM-ID-003",
                        format!("{base}/dependencies/{di}"),
                        "Baseline dependencies require the same entity",
                        &[fid, &d.feature],
                    );
                }
            }
        }
    }
    let env = feature_env(p, f);
    let mut type_errors = Vec::new();
    let tpath = format!("{base}/transform");
    // Transform-specific checks.
    match &f.transform {
        Transform::Expression { expression } => {
            if !f.inputs.is_empty() {
                c.err(
                    "EXPRESSION_SOURCE_INPUT",
                    "SEM-TRANS-001",
                    format!("{base}/inputs"),
                    "Derived expressions must use declared feature dependencies, not source inputs",
                    &[fid],
                );
            }
            expr::check(
                expression,
                &env,
                &format!("{tpath}/expression"),
                &mut type_errors,
            );
        }
        Transform::Learned {
            contract,
            conformance_suite,
            ..
        }
        | Transform::Custom {
            contract,
            conformance_suite,
            ..
        } => {
            for (k, inp) in contract.inputs.iter().enumerate() {
                if let Some(t) = expr::check(
                    &inp.expression,
                    &env,
                    &format!("{tpath}/contract/inputs/{k}/expression"),
                    &mut type_errors,
                ) {
                    if t.ty.is_some_and(|x| {
                        x != inp.ty && !(x == LogicalType::Integer && inp.ty == LogicalType::Number)
                    }) || (t.nullable && !inp.nullable)
                    {
                        c.err(
                            "TYPE_MISMATCH",
                            "SEM-CUSTOM-001",
                            format!("{tpath}/contract/inputs/{k}"),
                            format!(
                                "Contract input {} type/nullability disagrees with its expression",
                                inp.name
                            ),
                            &[fid],
                        );
                    }
                }
            }
            if !f.inputs.is_empty() {
                c.err("EXPRESSION_SOURCE_INPUT", "SEM-CUSTOM-001", format!("{base}/inputs"), "Learned/custom contracts in this profile read pinned feature dependencies only", &[fid]);
            }
            if p.suite(conformance_suite).is_none() {
                c.err(
                    "UNKNOWN_SUITE",
                    "SEM-ID-002",
                    format!("{tpath}/conformance_suite"),
                    format!("Unknown suite {conformance_suite}"),
                    &[fid],
                );
            }
            if let Transform::Learned {
                artifact, fitting, ..
            } = &f.transform
            {
                if artifact.split_id != fitting.split_id {
                    c.err(
                        "FIT_SPLIT_MISMATCH",
                        "SEM-FIT-003",
                        format!("{tpath}/artifact/split_id"),
                        "Artifact split differs from its fitting split",
                        &[fid],
                    );
                }
                let a = format!("{tpath}/artifact");
                let created = c.ts(&artifact.created_at, format!("{a}/created_at"), &[fid]);
                let cutoff = c.ts(
                    &artifact.training_cutoff,
                    format!("{a}/training_cutoff"),
                    &[fid],
                );
                let avail = c.ts(
                    &artifact.training_available_cutoff,
                    format!("{a}/training_available_cutoff"),
                    &[fid],
                );
                let ts = c.ts(
                    &fitting.training_start,
                    format!("{tpath}/fitting/training_start"),
                    &[fid],
                );
                let te = c.ts(
                    &fitting.training_end,
                    format!("{tpath}/fitting/training_end"),
                    &[fid],
                );
                c.ts(
                    &fitting.label_availability_cutoff,
                    format!("{tpath}/fitting/label_availability_cutoff"),
                    &[fid],
                );
                if let (Some(ts), Some(te)) = (ts, te) {
                    if ts >= te {
                        c.err(
                            "INVALID_FIT_WINDOW",
                            "SEM-FIT-001",
                            format!("{tpath}/fitting"),
                            "training_start must precede training_end",
                            &[fid],
                        );
                    }
                }
                if let (Some(created), Some(avail)) = (created, avail) {
                    if avail > created {
                        c.err(
                            "INVALID_FIT_WINDOW",
                            "SEM-FIT-001",
                            format!("{a}"),
                            "An artifact cannot be created before its training data was available",
                            &[fid],
                        );
                    }
                }
                if let (Some(cut), Some(avail)) = (cutoff, avail) {
                    if cut > avail {
                        c.err(
                            "INVALID_FIT_WINDOW",
                            "SEM-FIT-001",
                            format!("{a}"),
                            "training_cutoff must not exceed training_available_cutoff",
                            &[fid],
                        );
                    }
                }
            }
        }
        Transform::Aggregate { .. } | Transform::Recency { .. } | Transform::Lookup { .. } => {
            let (input, joins, group_by, filter) = f.transform.row_shape().unwrap();
            if f.input(input).is_none() {
                c.err(
                    "UNKNOWN_INPUT",
                    "SEM-REL-001",
                    format!("{tpath}/input"),
                    format!("Unknown input alias {input}"),
                    &[fid],
                );
            }
            let mut reached: HashSet<&str> = HashSet::from([input]);
            for (ji, j) in joins.iter().enumerate() {
                let jp = format!("{tpath}/joins/{ji}");
                let Some(r) = p.relationship(&j.relationship) else {
                    c.err(
                        "UNKNOWN_RELATIONSHIP",
                        "SEM-ID-002",
                        format!("{jp}/relationship"),
                        format!("Unknown relationship {}", j.relationship),
                        &[fid],
                    );
                    continue;
                };
                if r.version != j.version {
                    c.err(
                        "VERSION_MISMATCH",
                        "SEM-ID-002",
                        format!("{jp}/version"),
                        format!(
                            "Join pins {}@{}; project has {}",
                            r.id, j.version, r.version
                        ),
                        &[fid, &r.id],
                    );
                }
                let (Some(l), Some(rt)) = (f.input(&j.left_alias), f.input(&j.right_alias)) else {
                    c.err(
                        "UNKNOWN_INPUT",
                        "SEM-REL-001",
                        jp.clone(),
                        "Join aliases must be declared feature inputs",
                        &[fid, &r.id],
                    );
                    continue;
                };
                if l.source != r.from.source || rt.source != r.to.source {
                    c.err(
                        "RELATIONSHIP_ENDPOINT_MISMATCH",
                        "SEM-REL-001",
                        jp.clone(),
                        "Alias sources do not match the relationship endpoints",
                        &[fid, &r.id],
                    );
                }
                if !reached.contains(j.left_alias.as_str())
                    || reached.contains(j.right_alias.as_str())
                {
                    c.err("JOIN_ORDER_INVALID", "SEM-REL-001", jp.clone(), "Each join must extend the reachable row environment with a new alias exactly once", &[fid, &r.id]);
                }
                reached.insert(j.right_alias.as_str());
                for (endpoint, inp) in [(&r.from, l), (&r.to, rt)] {
                    for col in &endpoint.columns {
                        if !inp.columns.contains(col) {
                            c.err(
                                "MISSING_INPUT_COLUMN",
                                "SEM-TRANS-001",
                                jp.clone(),
                                format!("Join column {}.{col} is not declared", endpoint.source),
                                &[fid, &r.id],
                            );
                        }
                    }
                }
                if let RelationshipTemporal::AsOf {
                    valid_from,
                    valid_to,
                    ..
                } = &r.temporal
                {
                    for col in [valid_from, valid_to] {
                        if !rt.columns.contains(col) {
                            c.err(
                                "MISSING_INPUT_COLUMN",
                                "SEM-TRANS-001",
                                jp.clone(),
                                format!(
                                    "Historical interval column {}.{col} is not declared",
                                    rt.source
                                ),
                                &[fid, &r.id],
                            );
                        }
                    }
                }
            }
            let declared: HashSet<&str> = f.inputs.iter().map(|i| i.alias.as_str()).collect();
            if declared != reached {
                c.err("UNUSED_INPUT", "SEM-REL-001", format!("{base}/inputs"), "Every input alias must participate in the transform through its declared joins", &[fid]);
            }
            if let Some(fl) = filter {
                if let Some(t) = expr::check(fl, &env, &format!("{tpath}/filter"), &mut type_errors)
                {
                    if t.ty.is_some_and(|x| x != LogicalType::Boolean) {
                        c.err(
                            "TYPE_MISMATCH",
                            "SEM-TRANS-003",
                            format!("{tpath}/filter"),
                            "Filter must return boolean",
                            &[fid],
                        );
                    }
                }
            }
            if let Some(e) = entity {
                let mapped: Vec<&str> = group_by.iter().map(|g| g.entity_key.as_str()).collect();
                let unique: HashSet<&str> = mapped.iter().copied().collect();
                if unique.len() != mapped.len() {
                    c.err(
                        "DUPLICATE_ENTITY_MAPPING",
                        "SEM-ENT-002",
                        format!("{tpath}/group_by"),
                        "Each entity key must be mapped exactly once",
                        &[fid],
                    );
                }
                let keys: HashSet<&str> = e.keys.iter().map(|k| k.name.as_str()).collect();
                if unique != keys {
                    c.err(
                        "ENTITY_MAPPING_INCOMPLETE",
                        "SEM-ENT-002",
                        format!("{tpath}/group_by"),
                        "Group-by must map every entity key, including tenant keys, exactly once",
                        &[fid, &e.id],
                    );
                }
                for (gi, g) in group_by.iter().enumerate() {
                    let gp = format!("{tpath}/group_by/{gi}");
                    match env.column(&g.input, &g.column) {
                        Err((code, msg)) => c.err(&code, "SEM-TRANS-001", gp, msg, &[fid]),
                        Ok(t) => {
                            if let Some(k) = e.keys.iter().find(|k| k.name == g.entity_key) {
                                if t.ty != Some(k.ty) {
                                    c.err(
                                        "ENTITY_KEY_TYPE",
                                        "SEM-ENT-001",
                                        gp,
                                        "Feature entity mapping has an incompatible type",
                                        &[fid, &e.id],
                                    );
                                }
                            }
                        }
                    }
                }
            }
            match &f.transform {
                Transform::Aggregate {
                    operation,
                    value,
                    distinct_by,
                    nulls,
                    ..
                } => {
                    if let Some(v) = value {
                        if let Some(t) =
                            expr::check(v, &env, &format!("{tpath}/value"), &mut type_errors)
                        {
                            if matches!(operation, AggregateOp::Sum | AggregateOp::Mean)
                                && t.ty.is_some_and(|x| !x.is_numeric())
                            {
                                c.err(
                                    "TYPE_MISMATCH",
                                    "SEM-TRANS-004",
                                    format!("{tpath}/value"),
                                    format!("{operation:?} requires a numeric expression"),
                                    &[fid],
                                );
                            }
                            if matches!(
                                operation,
                                AggregateOp::Sum
                                    | AggregateOp::Mean
                                    | AggregateOp::Min
                                    | AggregateOp::Max
                            ) && t.nullable
                                && !f.output.nullable
                                && *nulls != NullPolicy::Error
                            {
                                c.err("NULLABILITY_CONFLICT", "SEM-TRANS-004", format!("{base}/output/nullable"), "A nullable aggregate input can produce null (all-null or propagated); the output must be nullable", &[fid]);
                            }
                        }
                    }
                    for (k, d) in distinct_by.iter().enumerate() {
                        expr::check(
                            d,
                            &env,
                            &format!("{tpath}/distinct_by/{k}"),
                            &mut type_errors,
                        );
                    }
                    if f.missingness.null_input != *nulls {
                        c.err(
                            "NULL_POLICY_CONFLICT",
                            "SEM-TRANS-004",
                            format!("{base}/missingness/null_input"),
                            "Aggregate nulls and feature missingness.null_input must agree",
                            &[fid],
                        );
                    }
                }
                Transform::Recency {
                    input, event_time, ..
                } => {
                    if let Some(src) = f.input(input).and_then(|i| p.source(&i.source)) {
                        if &src.time.event_time != event_time {
                            c.err(
                                "RECENCY_EVENT_TIME",
                                "SEM-TRANS-005",
                                format!("{tpath}/event_time"),
                                format!(
                                    "Recency must measure the window's event-time column {}",
                                    src.time.event_time
                                ),
                                &[fid],
                            );
                        }
                    }
                    match env.column(input, event_time) {
                        Err((code, msg)) => c.err(
                            &code,
                            "SEM-TRANS-005",
                            format!("{tpath}/event_time"),
                            msg,
                            &[fid],
                        ),
                        Ok(t) if t.ty != Some(LogicalType::Timestamp) => c.err(
                            "INVALID_TIME_COLUMN",
                            "SEM-TRANS-005",
                            format!("{tpath}/event_time"),
                            "Recency requires a timestamp column",
                            &[fid],
                        ),
                        Ok(_) => {}
                    }
                }
                Transform::Lookup {
                    value, order_by, ..
                } => {
                    if let Some(t) =
                        expr::check(value, &env, &format!("{tpath}/value"), &mut type_errors)
                    {
                        if t.nullable && !f.output.nullable {
                            c.err(
                                "NULLABILITY_CONFLICT",
                                "SEM-TRANS-005",
                                format!("{base}/output/nullable"),
                                "A selected lookup null remains null; the output must be nullable",
                                &[fid],
                            );
                        }
                    }
                    for (k, o) in order_by.iter().enumerate() {
                        if let Err((code, msg)) = env.column(&o.input, &o.column) {
                            c.err(
                                &code,
                                "SEM-TRANS-005",
                                format!("{tpath}/order_by/{k}"),
                                msg,
                                &[fid],
                            );
                        }
                    }
                }
                _ => {}
            }
        }
    }
    for e in type_errors {
        let rule = match e.code.as_str() {
            "UNDECLARED_DEPENDENCY" | "UNSUPPORTED_ROW_LEVEL_DEPENDENCY" => "SEM-ID-003",
            "UNKNOWN_INPUT"
            | "UNDECLARED_INPUT_COLUMN"
            | "UNKNOWN_COLUMN"
            | "EXPRESSION_SOURCE_INPUT" => "SEM-TRANS-001",
            "VERSION_MISMATCH" | "UNKNOWN_FEATURE" => "SEM-ID-002",
            _ => "SEM-TRANS-002",
        };
        c.err(&e.code, rule, e.path, e.message, &[fid]);
    }
    // Declared dependencies must be used, and lineage must list them.
    let used: HashSet<(String, String)> = env.used_features.borrow().iter().cloned().collect();
    for (di, d) in f.dependencies.iter().enumerate() {
        if !used.contains(&(d.feature.clone(), d.version.clone())) {
            c.err(
                "UNUSED_DEPENDENCY",
                "SEM-ID-003",
                format!("{base}/dependencies/{di}"),
                format!(
                    "Declared dependency {}@{} is not referenced by the transform",
                    d.feature, d.version
                ),
                &[fid],
            );
        }
        if !f
            .lineage
            .dependencies
            .iter()
            .any(|l| l.feature == d.feature && l.version == d.version)
        {
            c.err(
                "MISSING_LINEAGE",
                "SEM-LIN-002",
                format!("{base}/lineage/dependencies"),
                format!(
                    "Dependency {}@{} is missing from lineage",
                    d.feature, d.version
                ),
                &[fid],
            );
        }
    }
    for (k, l) in f.lineage.dependencies.iter().enumerate() {
        if !f
            .dependencies
            .iter()
            .any(|d| d.feature == l.feature && d.version == l.version)
        {
            c.err(
                "LINEAGE_UNDECLARED_DEPENDENCY",
                "SEM-LIN-002",
                format!("{base}/lineage/dependencies/{k}"),
                format!(
                    "Lineage names {}@{} which is not a declared dependency",
                    l.feature, l.version
                ),
                &[fid],
            );
        }
    }
    // Column lineage with roles (SEM-LIN-001).
    let mut declared: HashMap<(String, String), HashSet<String>> = HashMap::new();
    for (k, l) in f.lineage.sources.iter().enumerate() {
        match p.source(&l.source) {
            None => c.err(
                "UNKNOWN_SOURCE",
                "SEM-LIN-002",
                format!("{base}/lineage/sources/{k}/source"),
                format!("Unknown lineage source {}", l.source),
                &[fid],
            ),
            Some(s) if s.column(&l.column).is_none() => c.err(
                "UNKNOWN_COLUMN",
                "SEM-LIN-002",
                format!("{base}/lineage/sources/{k}/column"),
                format!(
                    "Lineage references nonexistent column {}.{}",
                    l.source, l.column
                ),
                &[fid],
            ),
            _ => {}
        }
        declared
            .entry((l.source.clone(), l.column.clone()))
            .or_default()
            .extend(l.roles.iter().cloned());
    }
    for ((s, col), roles) in required_lineage(p, f) {
        match declared.get(&(s.clone(), col.clone())) {
            None => c.err(
                "MISSING_LINEAGE",
                "SEM-LIN-001",
                format!("{base}/lineage/sources"),
                format!(
                    "Missing lineage for {s}.{col} (roles: {})",
                    roles.iter().cloned().collect::<Vec<_>>().join(", ")
                ),
                &[fid],
            ),
            Some(have) => {
                let missing: Vec<&str> = roles
                    .iter()
                    .filter(|r| !have.contains(**r))
                    .copied()
                    .collect();
                if !missing.is_empty() {
                    c.err(
                        "MISSING_LINEAGE_ROLE",
                        "SEM-LIN-001",
                        format!("{base}/lineage/sources"),
                        format!("Lineage for {s}.{col} omits role(s) {}", missing.join(", ")),
                        &[fid],
                    );
                }
            }
        }
    }
    // Output type and missingness contracts.
    if let Some(t) = transform_type(p, f) {
        if let Some(ty) = t.ty {
            let ok = ty == f.output.ty
                || (ty == LogicalType::Integer && f.output.ty == LogicalType::Number);
            if !ok {
                c.err(
                    "OUTPUT_TYPE_MISMATCH",
                    "SEM-OUT-001",
                    format!("{base}/output/type"),
                    format!(
                        "Transform produces {}, output declares {}",
                        ty.as_str(),
                        f.output.ty.as_str()
                    ),
                    &[fid],
                );
            }
        }
        if matches!(f.transform, Transform::Expression { .. }) && t.nullable && !f.output.nullable {
            c.err(
                "NULLABILITY_CONFLICT",
                "SEM-OUT-001",
                format!("{base}/output/nullable"),
                "The expression can produce null; the output must be nullable",
                &[fid],
            );
        }
    }
    let m = &f.missingness;
    if !f.output.nullable
        && (matches!(m.empty, EmptyPolicy::Null)
            || m.insufficient_history == NullOrError::Null
            || m.source_unavailable == NullOrError::Null
            || m.null_input == NullPolicy::Propagate)
    {
        c.err(
            "NULLABILITY_CONFLICT",
            "SEM-MISS-001",
            format!("{base}/missingness"),
            "Missingness policy can produce null for a non-nullable output",
            &[fid],
        );
    }
    if let EmptyPolicy::Value { value } = &m.empty {
        match V::from_json(value, f.output.ty) {
            Ok(V::Null) if !f.output.nullable => c.err(
                "TYPE_MISMATCH",
                "SEM-MISS-002",
                format!("{base}/missingness/empty/value"),
                "Empty value is null for a non-null output",
                &[fid],
            ),
            Ok(_) => {}
            Err(msg) => c.err(
                "TYPE_MISMATCH",
                "SEM-MISS-002",
                format!("{base}/missingness/empty/value"),
                format!("Empty result value does not match output type: {msg}"),
                &[fid],
            ),
        }
        if matches!(
            f.transform,
            Transform::Aggregate {
                operation: AggregateOp::Sum
                    | AggregateOp::Mean
                    | AggregateOp::Min
                    | AggregateOp::Max,
                ..
            }
        ) && !value.is_null()
        {
            c.warn("EMPTY_IDENTITY_REVIEW", "SEM-TRANS-004", format!("{base}/missingness/empty"), "A known-empty identity for sum/mean/min/max is a semantic choice; confirm it is intended", &[fid]);
        }
    }
    if f.history.required_seconds < f.time.window.lookback_seconds {
        c.err(
            "HISTORY_WINDOW_MISMATCH",
            "SEM-MISS-003",
            format!("{base}/history/required_seconds"),
            "Contiguous history must cover the feature window",
            &[fid],
        );
    }
    if f.time.window.lookback_seconds <= 0 {
        c.err(
            "INVALID_WINDOW",
            "SEM-TIME-001",
            format!("{base}/time/window/lookback_seconds"),
            "Lookback must be a positive number of elapsed seconds",
            &[fid],
        );
    }
    c.ts(
        &f.history.earliest_as_of,
        format!("{base}/history/earliest_as_of"),
        &[fid],
    );
    let cons = &f.output.constraints;
    if let (Some(a), Some(b)) = (cons.minimum, cons.maximum) {
        if a > b {
            c.err(
                "INVALID_RANGE",
                "SEM-OUT-001",
                format!("{base}/output/constraints"),
                "Minimum exceeds maximum",
                &[fid],
            );
        }
    }
    if (cons.minimum.is_some() || cons.maximum.is_some()) && !f.output.ty.is_numeric() {
        c.err(
            "INVALID_RANGE",
            "SEM-OUT-001",
            format!("{base}/output/constraints"),
            "Numeric range constraints require a numeric output",
            &[fid],
        );
    }
    if let Some(values) = &cons.enumeration {
        for (k, v) in values.iter().enumerate() {
            if V::from_json(v, f.output.ty).is_err() {
                c.err(
                    "TYPE_MISMATCH",
                    "SEM-OUT-001",
                    format!("{base}/output/constraints/enum/{k}"),
                    "Enum value does not match the output type",
                    &[fid],
                );
            }
        }
    }
    for (k, q) in f.quality.checks.iter().enumerate() {
        if let QualityCheck::Range {
            minimum: None,
            maximum: None,
            ..
        } = q
        {
            c.err(
                "QUALITY_CHECK_INCOMPLETE",
                "SEM-QUAL-001",
                format!("{base}/quality/checks/{k}"),
                "A range check needs a minimum or maximum threshold",
                &[fid],
            );
        }
    }
    for (k, s) in f.acceptance.suites.iter().enumerate() {
        if p.suite(s).is_none() {
            c.err(
                "UNKNOWN_SUITE",
                "SEM-ID-002",
                format!("{base}/acceptance/suites/{k}"),
                format!("Unknown suite {s}"),
                &[fid],
            );
        }
    }
}

fn check_cycles(c: &mut Ctx) {
    let p = c.p;
    let mut state: HashMap<&str, u8> = HashMap::new();
    fn visit<'a>(
        p: &'a Project,
        id: &'a str,
        state: &mut HashMap<&'a str, u8>,
        stack: &mut Vec<&'a str>,
        cycles: &mut Vec<Vec<String>>,
    ) {
        match state.get(id) {
            Some(2) => return,
            Some(1) => {
                let start = stack.iter().position(|x| *x == id).unwrap_or(0);
                cycles.push(stack[start..].iter().map(|s| s.to_string()).collect());
                return;
            }
            _ => {}
        }
        state.insert(id, 1);
        stack.push(id);
        if let Some(f) = p.feature(id) {
            for d in &f.dependencies {
                if p.feature(&d.feature).is_some() {
                    visit(p, &d.feature, state, stack, cycles);
                }
            }
        }
        stack.pop();
        state.insert(id, 2);
    }
    let mut cycles = Vec::new();
    for f in &p.features {
        visit(p, &f.id, &mut state, &mut Vec::new(), &mut cycles);
    }
    for cy in cycles {
        let i = idx(&p.features, |f| &f.id, &cy[0]).unwrap_or(0);
        c.err(
            "DEPENDENCY_CYCLE",
            "SEM-ID-003",
            format!("/features/{i}/dependencies"),
            format!("Dependency cycle: {}", cy.join(" -> ")),
            &cy.iter().map(String::as_str).collect::<Vec<_>>(),
        );
    }
}

fn check_feature_sets_and_consumers(c: &mut Ctx) {
    let p = c.p;
    for (si, s) in p.feature_sets.iter().enumerate() {
        let base = format!("/feature_sets/{si}");
        if p.entity(&s.entity).is_none() {
            c.err(
                "UNKNOWN_ENTITY",
                "SEM-ID-002",
                format!("{base}/entity"),
                format!("Unknown entity {}", s.entity),
                &[&s.id],
            );
        }
        let mut seen = HashSet::new();
        for (k, m) in s.features.iter().enumerate() {
            if !seen.insert(m.id.as_str()) {
                c.err(
                    "DUPLICATE_FEATURE_MEMBER",
                    "SEM-ID-001",
                    format!("{base}/features/{k}"),
                    format!("Duplicate member {}", m.id),
                    &[&s.id],
                );
            }
            match p.feature(&m.id) {
                None => c.err(
                    "UNKNOWN_FEATURE",
                    "SEM-ID-002",
                    format!("{base}/features/{k}/id"),
                    format!("Unknown feature {}", m.id),
                    &[&s.id],
                ),
                Some(f) => {
                    if f.version != m.version {
                        c.err(
                            "VERSION_MISMATCH",
                            "SEM-ID-002",
                            format!("{base}/features/{k}/version"),
                            format!("Pinned {}@{}; project has {}", m.id, m.version, f.version),
                            &[&s.id, &m.id],
                        );
                    }
                    if f.entity != s.entity {
                        c.err(
                            "FEATURE_SET_ENTITY_MISMATCH",
                            "SEM-ENT-001",
                            format!("{base}/features/{k}"),
                            format!("{} has entity {}, set has {}", m.id, f.entity, s.entity),
                            &[&s.id, &m.id],
                        );
                    }
                }
            }
        }
    }
    for (ci, cons) in p.consumers.iter().enumerate() {
        let base = format!("/consumers/{ci}");
        let fs = p.feature_set(&cons.feature_set.feature_set);
        match fs {
            None => c.err(
                "UNKNOWN_FEATURE_SET",
                "SEM-ID-002",
                format!("{base}/feature_set"),
                format!("Unknown feature set {}", cons.feature_set.feature_set),
                &[&cons.id],
            ),
            Some(s) if s.version != cons.feature_set.version => c.err(
                "VERSION_MISMATCH",
                "SEM-CONS-001",
                format!("{base}/feature_set/version"),
                format!(
                    "Consumer pins {}@{}; project has {}",
                    s.id, cons.feature_set.version, s.version
                ),
                &[&cons.id, &s.id],
            ),
            _ => {}
        }
        let mapped: Vec<&str> = cons
            .entity_inputs
            .iter()
            .map(|m| m.entity_key.as_str())
            .collect();
        let unique: HashSet<&str> = mapped.iter().copied().collect();
        if unique.len() != mapped.len() {
            c.err(
                "DUPLICATE_ENTITY_MAPPING",
                "SEM-CONS-001",
                format!("{base}/entity_inputs"),
                "Consumer repeats an entity key",
                &[&cons.id],
            );
        }
        let fields: Vec<&str> = cons
            .entity_inputs
            .iter()
            .map(|m| m.input_field.as_str())
            .chain([
                cons.time_inputs.as_of.as_str(),
                cons.time_inputs.requested_at.as_str(),
            ])
            .chain(cons.time_inputs.dataset_cutoff.as_deref())
            .collect();
        if fields.len() != fields.iter().collect::<HashSet<_>>().len() {
            c.err(
                "DUPLICATE_REQUEST_FIELD",
                "SEM-CONS-001",
                format!("{base}/entity_inputs"),
                "Request fields for keys and times must be distinct",
                &[&cons.id],
            );
        }
        if let Some(e) = fs.and_then(|s| p.entity(&s.entity)) {
            let keys: HashSet<&str> = e.keys.iter().map(|k| k.name.as_str()).collect();
            if unique != keys {
                c.err(
                    "ENTITY_MAPPING_INCOMPLETE",
                    "SEM-CONS-001",
                    format!("{base}/entity_inputs"),
                    "Consumer must map every entity key, including tenant keys",
                    &[&cons.id, &e.id],
                );
            }
            if let Some(label) = &cons.label {
                let lp = format!("{base}/label");
                let lkeys: Vec<&str> = label
                    .entity_keys
                    .iter()
                    .map(|k| k.entity_key.as_str())
                    .collect();
                let lunique: HashSet<&str> = lkeys.iter().copied().collect();
                if lunique.len() != lkeys.len() {
                    c.err(
                        "DUPLICATE_ENTITY_MAPPING",
                        "SEM-LABEL-001",
                        format!("{lp}/entity_keys"),
                        "Label repeats an entity key",
                        &[&cons.id],
                    );
                }
                if lunique != keys {
                    c.err(
                        "ENTITY_MAPPING_INCOMPLETE",
                        "SEM-LABEL-001",
                        format!("{lp}/entity_keys"),
                        "Label must map every entity key, including tenant keys",
                        &[&cons.id],
                    );
                }
                match p.source(&label.source) {
                    None => c.err(
                        "UNKNOWN_SOURCE",
                        "SEM-ID-002",
                        format!("{lp}/source"),
                        format!("Unknown label source {}", label.source),
                        &[&cons.id],
                    ),
                    Some(s) => {
                        for (name, col) in [
                            ("value_column", &label.value_column),
                            ("event_time_column", &label.event_time_column),
                            ("available_time_column", &label.available_time_column),
                        ] {
                            match s.column(col) {
                                None => c.err(
                                    "UNKNOWN_COLUMN",
                                    "SEM-LABEL-001",
                                    format!("{lp}/{name}"),
                                    format!("Unknown label column {}.{col}", s.id),
                                    &[&cons.id],
                                ),
                                Some(cc)
                                    if name != "value_column"
                                        && (cc.ty != LogicalType::Timestamp || cc.nullable) =>
                                {
                                    c.err(
                                        "INVALID_TIME_COLUMN",
                                        "SEM-LABEL-001",
                                        format!("{lp}/{name}"),
                                        "Label time columns must be non-null timestamps",
                                        &[&cons.id],
                                    )
                                }
                                Some(cc) if name == "value_column" => {
                                    let ok = match label.aggregation {
                                        LabelAggregation::AnyTrue => cc.ty == LogicalType::Boolean,
                                        LabelAggregation::Sum => cc.ty.is_numeric(),
                                        _ => true,
                                    };
                                    if !ok {
                                        c.err("TYPE_MISMATCH", "SEM-LABEL-001", format!("{lp}/value_column"), "Label aggregation is incompatible with the value column type", &[&cons.id]);
                                    }
                                }
                                _ => {}
                            }
                        }
                        if s.time.available_time != label.available_time_column {
                            c.err("LABEL_AVAILABILITY_COLUMN", "SEM-LABEL-001", format!("{lp}/available_time_column"), "Label availability column differs from the source availability column", &[&cons.id]);
                        }
                        for (k, m) in label.entity_keys.iter().enumerate() {
                            match (s.column(&m.column), e.keys.iter().find(|x| x.name == m.entity_key)) {
                                (None, _) => c.err("UNKNOWN_COLUMN", "SEM-LABEL-001", format!("{lp}/entity_keys/{k}/column"), format!("Unknown label column {}.{}", s.id, m.column), &[&cons.id]),
                                (Some(col), Some(key)) if col.ty != key.ty || col.nullable => {
                                    c.err("ENTITY_KEY_TYPE", "SEM-LABEL-001", format!("{lp}/entity_keys/{k}"), format!("Label key column {}.{} must be a non-null {} like entity key {}", s.id, m.column, key.ty.as_str(), key.name), &[&cons.id])
                                }
                                _ => {}
                            }
                        }
                    }
                }
                if cons.time_inputs.dataset_cutoff.is_none() {
                    c.err(
                        "LABEL_CUTOFF_REQUIRED",
                        "SEM-LABEL-001",
                        format!("{base}/time_inputs"),
                        "Label availability requires a dataset_cutoff request field",
                        &[&cons.id],
                    );
                }
                if cons.selection.mode == SelectionMode::HistoricalServingReplay {
                    c.err("LABEL_MODE_CONFLICT", "SEM-LABEL-001", format!("{base}/selection"), "Label dataset construction is defined for source-as-known training consumers only", &[&cons.id]);
                }
            }
        }
    }
}

fn check_deployment(c: &mut Ctx, di: usize, d: &Deployment) {
    let p = c.p;
    let base = format!("/deployments/{di}");
    let did = d.id.as_str();
    let mut adapters: HashMap<&str, &AdapterInstance> = HashMap::new();
    for (k, a) in d.adapters.iter().enumerate() {
        if adapters.insert(a.id.as_str(), a).is_some() {
            c.err(
                "DUPLICATE_ADAPTER",
                "SEM-PORT-002",
                format!("{base}/adapters/{k}/id"),
                format!("Duplicate adapter id {}", a.id),
                &[did],
            );
        }
        if a.roles.len() != a.roles.iter().collect::<HashSet<_>>().len() {
            c.err(
                "DUPLICATE_ADAPTER_ROLE",
                "SEM-PORT-002",
                format!("{base}/adapters/{k}/roles"),
                "Adapter repeats a role",
                &[did, &a.id],
            );
        }
        if contains_secret_like(&serde_json::Value::Object(a.configuration.clone())) {
            c.err("CREDENTIAL_IN_CONFIGURATION", "SEM-DEPLOY-002", format!("{base}/adapters/{k}/configuration"), "Adapter configuration must be credential-free; reference externally managed identities instead", &[did, &a.id]);
        }
    }
    let role_check = |c: &mut Ctx, id: &str, role: Role, path: String| match adapters.get(id) {
        None => c.err(
            "UNKNOWN_ADAPTER",
            "SEM-PORT-002",
            path,
            format!("Unknown adapter {id}"),
            &[did, id],
        ),
        Some(a) if !a.roles.contains(&role) => c.err(
            "ADAPTER_ROLE_MISMATCH",
            "SEM-PORT-002",
            path,
            format!("{id} does not declare role {}", role.as_str()),
            &[did, id],
        ),
        _ => {}
    };
    role_check(
        c,
        &d.execution.compute,
        Role::Compute,
        format!("{base}/execution/compute"),
    );
    role_check(
        c,
        &d.execution.catalog,
        Role::Catalog,
        format!("{base}/execution/catalog"),
    );
    match (&d.execution.orchestrator, &d.schedule) {
        (Some(o), _) => role_check(
            c,
            o,
            Role::Orchestrator,
            format!("{base}/execution/orchestrator"),
        ),
        (None, Schedule::Interval { .. }) => c.err(
            "ORCHESTRATOR_REQUIRED",
            "SEM-PORT-002",
            format!("{base}/execution/orchestrator"),
            "A scheduled deployment requires an explicit orchestrator adapter",
            &[did],
        ),
        _ => {}
    }
    if let Schedule::Interval {
        every_seconds,
        offset_seconds,
    } = d.schedule
    {
        if offset_seconds >= every_seconds {
            c.err(
                "INVALID_SCHEDULE",
                "SEM-DEPLOY-003",
                format!("{base}/schedule/offset_seconds"),
                "offset_seconds must be less than every_seconds",
                &[did],
            );
        }
    }
    // Source bindings.
    let mut bound = HashSet::new();
    for (k, b) in d.source_bindings.iter().enumerate() {
        let bp = format!("{base}/source_bindings/{k}");
        role_check(c, &b.adapter, Role::Catalog, format!("{bp}/adapter"));
        if !bound.insert(b.source.as_str()) {
            c.err(
                "DUPLICATE_SOURCE_BINDING",
                "SEM-DEPLOY-001",
                format!("{bp}/source"),
                format!("Source {} is bound more than once", b.source),
                &[did, &b.source],
            );
        }
        if let Location::Fixture { suite } = &b.location {
            if p.suite(suite).is_none() {
                c.err(
                    "UNKNOWN_SUITE",
                    "SEM-ID-002",
                    format!("{bp}/location/suite"),
                    format!("Unknown suite {suite}"),
                    &[did],
                );
            }
        }
        let Some(s) = p.source(&b.source) else {
            c.err(
                "UNKNOWN_SOURCE",
                "SEM-ID-002",
                format!("{bp}/source"),
                format!("Unknown source {}", b.source),
                &[did],
            );
            continue;
        };
        if s.version != b.version {
            c.err(
                "VERSION_MISMATCH",
                "SEM-PORT-003",
                format!("{bp}/version"),
                format!(
                    "Binding pins {}@{}; project has {}",
                    s.id, b.version, s.version
                ),
                &[did, &s.id],
            );
        }
        let logical: BTreeSet<&str> = s.columns.iter().map(|c| c.name.as_str()).collect();
        let mapped: BTreeSet<&str> = b.column_mapping.keys().map(String::as_str).collect();
        if logical != mapped {
            let missing: Vec<&&str> = logical.difference(&mapped).collect();
            let extra: Vec<&&str> = mapped.difference(&logical).collect();
            c.err("SOURCE_COLUMN_MAPPING_INCOMPLETE", "SEM-PORT-003", format!("{bp}/column_mapping"), format!("Map exactly every declared logical column; missing={missing:?} extra={extra:?}"), &[did, &s.id]);
        }
        physical_unique(
            c,
            b.column_mapping
                .values()
                .filter_map(|v| v.as_str())
                .collect(),
            format!("{bp}/column_mapping"),
            &[did, &s.id],
        );
    }
    // Output targets.
    let mut targets = HashSet::new();
    let mut resources = HashSet::new();
    let mut required_sources = BTreeSet::new();
    for (k, t) in d.output_targets.iter().enumerate() {
        let tp = format!("{base}/output_targets/{k}");
        role_check(c, &t.adapter, t.role, format!("{tp}/adapter"));
        if !targets.insert((t.feature_set.clone(), t.version.clone(), t.role)) {
            c.err(
                "DUPLICATE_OUTPUT_TARGET",
                "SEM-PORT-004",
                tp.clone(),
                "Only one output target per feature-set version and storage role is allowed",
                &[did, &t.feature_set],
            );
        }
        if matches!(t.location, Location::Fixture { .. }) {
            c.err(
                "OUTPUT_LOCATION_READ_ONLY",
                "SEM-PORT-004",
                format!("{tp}/location"),
                "Fixture locations are read-only sources, not output destinations",
                &[did],
            );
        }
        if !resources.insert((t.adapter.clone(), t.location.address())) {
            c.err("DUPLICATE_OUTPUT_RESOURCE", "SEM-PORT-004", format!("{tp}/location"), "Repeated physical output resources (format labels excluded) are outside this profile", &[did]);
        }
        if d.source_bindings
            .iter()
            .any(|b| b.location.address() == t.location.address())
        {
            c.err(
                "OUTPUT_SOURCE_COLLISION",
                "SEM-PORT-004",
                format!("{tp}/location"),
                "An output destination must not also be a bound read-only source",
                &[did],
            );
        }
        let Some(fs) = p.feature_set(&t.feature_set) else {
            c.err(
                "UNKNOWN_FEATURE_SET",
                "SEM-ID-002",
                format!("{tp}/feature_set"),
                format!("Unknown feature set {}", t.feature_set),
                &[did],
            );
            continue;
        };
        if fs.version != t.version {
            c.err(
                "VERSION_MISMATCH",
                "SEM-PORT-004",
                format!("{tp}/version"),
                format!(
                    "Target pins {}@{}; project has {}",
                    fs.id, t.version, fs.version
                ),
                &[did, &fs.id],
            );
        }
        for m in &fs.features {
            required_sources.extend(feature_sources(p, &m.id));
        }
        if let Some(e) = p.entity(&fs.entity) {
            let keys: BTreeSet<&str> = e.keys.iter().map(|k| k.name.as_str()).collect();
            let mapped: BTreeSet<&str> = t.entity_key_mapping.keys().map(String::as_str).collect();
            if keys != mapped {
                c.err(
                    "OUTPUT_ENTITY_MAPPING_INCOMPLETE",
                    "SEM-PORT-004",
                    format!("{tp}/entity_key_mapping"),
                    "Map exactly every logical entity key",
                    &[did, &fs.id],
                );
            }
        }
        let members: HashMap<&str, &str> = fs
            .features
            .iter()
            .map(|m| (m.id.as_str(), m.version.as_str()))
            .collect();
        let mut mapped = HashSet::new();
        let mut physical: Vec<&str> = t
            .entity_key_mapping
            .values()
            .filter_map(|v| v.as_str())
            .collect();
        let mc = &t.metadata_columns;
        physical.extend([
            mc.as_of.as_str(),
            mc.published_at.as_str(),
            mc.publication_sequence.as_str(),
            mc.publication_id.as_str(),
            mc.materialization_id.as_str(),
        ]);
        for (j, fm) in t.feature_mapping.iter().enumerate() {
            if !mapped.insert(fm.feature.as_str()) {
                c.err(
                    "DUPLICATE_OUTPUT_FEATURE",
                    "SEM-PORT-004",
                    format!("{tp}/feature_mapping/{j}"),
                    format!("{} mapped twice", fm.feature),
                    &[did],
                );
            }
            match members.get(fm.feature.as_str()) {
                Some(v) if *v != fm.version => c.err(
                    "VERSION_MISMATCH",
                    "SEM-PORT-004",
                    format!("{tp}/feature_mapping/{j}/version"),
                    format!("Mapping pins {}@{}; set pins {v}", fm.feature, fm.version),
                    &[did],
                ),
                None => c.err(
                    "OUTPUT_FEATURE_NOT_MEMBER",
                    "SEM-PORT-004",
                    format!("{tp}/feature_mapping/{j}/feature"),
                    format!("{} is not a member of {}", fm.feature, fs.id),
                    &[did],
                ),
                _ => {}
            }
            physical.push(&fm.column);
            physical.push(&fm.state_column);
        }
        if mapped != members.keys().copied().collect::<HashSet<_>>() {
            c.err(
                "OUTPUT_FEATURE_MAPPING_INCOMPLETE",
                "SEM-PORT-004",
                format!("{tp}/feature_mapping"),
                "Map exactly every feature-set member with its value and state",
                &[did, &fs.id],
            );
        }
        physical_unique(c, physical, tp.clone(), &[did, &fs.id]);
    }
    for s in &required_sources {
        if !bound.contains(s.as_str()) {
            c.err(
                "MISSING_SOURCE_BINDING",
                "SEM-DEPLOY-001",
                format!("{base}/source_bindings"),
                format!("Required source {s} (direct, transitive or population) is not bound"),
                &[did, s],
            );
        }
    }
    let surface_role = d.publication.surface.role();
    let sets: BTreeSet<(String, String)> = d
        .output_targets
        .iter()
        .map(|t| (t.feature_set.clone(), t.version.clone()))
        .collect();
    for (fs, v) in &sets {
        if !targets.contains(&(fs.clone(), v.clone(), surface_role)) {
            c.err(
                "PUBLICATION_SURFACE_UNBOUND",
                "SEM-PORT-005",
                format!("{base}/publication/surface"),
                format!(
                    "{fs}@{v} has no {} target for the declared surface",
                    surface_role.as_str()
                ),
                &[did, fs],
            );
        }
    }
    // Consumer bindings.
    let mut consumers = HashSet::new();
    for (k, b) in d.consumer_bindings.iter().enumerate() {
        let bp = format!("{base}/consumer_bindings/{k}");
        if !consumers.insert(b.consumer.as_str()) {
            c.err(
                "DUPLICATE_CONSUMER_BINDING",
                "SEM-PORT-005",
                bp.clone(),
                format!("Consumer {} bound twice", b.consumer),
                &[did],
            );
        }
        role_check(c, &b.adapter, b.role, format!("{bp}/adapter"));
        if b.role != surface_role {
            c.err("CONSUMER_SURFACE_MISMATCH", "SEM-PORT-005", format!("{bp}/role"), "A consumer must use this deployment's guaranteed publication surface; use separate deployments for other surfaces", &[did, &b.consumer]);
        }
        let Some(cons) = p.consumer(&b.consumer) else {
            c.err(
                "UNKNOWN_CONSUMER",
                "SEM-ID-002",
                format!("{bp}/consumer"),
                format!("Unknown consumer {}", b.consumer),
                &[did],
            );
            continue;
        };
        if cons.version != b.version {
            c.err(
                "VERSION_MISMATCH",
                "SEM-PORT-005",
                format!("{bp}/version"),
                format!(
                    "Binding pins {}@{}; project has {}",
                    cons.id, b.version, cons.version
                ),
                &[did],
            );
        }
        let r = &cons.feature_set;
        if !d.output_targets.iter().any(|t| {
            t.feature_set == r.feature_set
                && t.version == r.version
                && t.adapter == b.adapter
                && t.role == b.role
        }) {
            c.err("CONSUMER_TARGET_MISSING", "SEM-PORT-005", bp.clone(), "Consumer requires a matching feature-set output target on its selected adapter and role", &[did, &cons.id]);
        }
    }
    // Explicit requirements.
    let mut reqs = HashSet::new();
    for (k, r) in d.runtime.required_capabilities.iter().enumerate() {
        let rp = format!("{base}/runtime/required_capabilities/{k}");
        role_check(c, &r.adapter, r.role, rp.clone());
        if !reqs.insert((r.adapter.clone(), r.role, r.capability.clone())) {
            c.err(
                "DUPLICATE_CAPABILITY_REQUIREMENT",
                "SEM-PORT-006",
                rp,
                format!(
                    "Duplicate requirement {}/{}/{}",
                    r.adapter,
                    r.role.as_str(),
                    r.capability
                ),
                &[did],
            );
        }
    }
    // Native policy.
    let native = &d.portability.native_bindings;
    if !native.is_empty() && d.portability.mode == PortabilityMode::Portable {
        c.err(
            "NATIVE_MODE_REQUIRED",
            "SEM-PORT-007",
            format!("{base}/portability/mode"),
            "Native bindings require portability.mode allow_native",
            &[did],
        );
    }
    let mut native_keys = HashSet::new();
    for (k, n) in native.iter().enumerate() {
        let np = format!("{base}/portability/native_bindings/{k}");
        role_check(c, &n.adapter, n.role, format!("{np}/adapter"));
        match p.feature(&n.feature.id) {
            None => c.err(
                "UNKNOWN_FEATURE",
                "SEM-ID-002",
                format!("{np}/feature"),
                format!("Unknown feature {}", n.feature.id),
                &[did],
            ),
            Some(f) if f.version != n.feature.version => c.err(
                "VERSION_MISMATCH",
                "SEM-PORT-007",
                format!("{np}/feature/version"),
                format!("Native binding pins {}@{}", f.id, n.feature.version),
                &[did],
            ),
            _ => {}
        }
        if !native_keys.insert((
            n.feature.id.clone(),
            n.feature.version.clone(),
            n.adapter.clone(),
            n.role,
        )) {
            c.err(
                "DUPLICATE_NATIVE_BINDING",
                "SEM-PORT-007",
                np.clone(),
                "Duplicate native binding",
                &[did],
            );
        }
        match p.suite(&n.conformance_suite) {
            None => c.err(
                "UNKNOWN_SUITE",
                "SEM-PORT-007",
                format!("{np}/conformance_suite"),
                format!("Unknown suite {}", n.conformance_suite),
                &[did],
            ),
            Some(s) if !s.cases.iter().any(|cs| cs.feature == n.feature) => c.err(
                "NATIVE_SUITE_FEATURE_MISMATCH",
                "SEM-PORT-007",
                format!("{np}/conformance_suite"),
                "Native conformance suite must contain cases for the pinned feature",
                &[did],
            ),
            _ => {}
        }
        for cap in &n.required_capabilities {
            if !reqs.contains(&(n.adapter.clone(), n.role, cap.clone())) {
                c.err(
                    "NATIVE_CAPABILITY_UNDECLARED",
                    "SEM-PORT-007",
                    format!("{np}/required_capabilities"),
                    format!(
                        "Native capability {cap} is not a declared requirement for {}/{}",
                        n.adapter,
                        n.role.as_str()
                    ),
                    &[did],
                );
            }
        }
        if n.migration_notes.trim().is_empty() {
            c.err(
                "NATIVE_MIGRATION_NOTES",
                "SEM-PORT-007",
                format!("{np}/migration_notes"),
                "Native bindings must state migration requirements",
                &[did],
            );
        }
    }
    if let Backfill::Allowed { start, end, .. } = &d.backfill {
        let s = c.ts(start, format!("{base}/backfill/start"), &[did]);
        let e = c.ts(end, format!("{base}/backfill/end"), &[did]);
        if let (Some(s), Some(e)) = (s, e) {
            if s >= e {
                c.err(
                    "INVALID_BACKFILL_RANGE",
                    "SEM-DEPLOY-003",
                    format!("{base}/backfill"),
                    "Backfill start must precede end",
                    &[did],
                );
            }
            for src in &required_sources {
                if let Some(earliest) = p
                    .source(src)
                    .and_then(|x| Instant::parse(&x.retention.earliest_reproducible_at).ok())
                {
                    if s < earliest {
                        c.err(
                            "BACKFILL_BEFORE_REPRODUCIBLE",
                            "SEM-REPLAY-003",
                            format!("{base}/backfill/start"),
                            format!("Backfill starts before {src}'s earliest_reproducible_at"),
                            &[did, src],
                        );
                    }
                }
            }
        }
    }
    let max_lookback = d
        .output_targets
        .iter()
        .filter_map(|t| p.feature_set(&t.feature_set))
        .flat_map(|fs| fs.features.iter().filter_map(|m| p.feature(&m.id)))
        .map(|f| f.time.window.lookback_seconds)
        .max()
        .unwrap_or(0);
    if d.output_retention_seconds < max_lookback {
        c.warn("OUTPUT_RETENTION_SHORT", "SEM-DEPLOY-002", format!("{base}/output_retention_seconds"), "Output retention is shorter than the longest feature window; replay beyond it is unsupported", &[did]);
    }
}

fn physical_unique(c: &mut Ctx, names: Vec<&str>, path: String, related: &[&str]) {
    let mut seen = HashSet::new();
    for n in names {
        if !seen.insert(n) {
            c.err("PHYSICAL_COLUMN_COLLISION", "SEM-PORT-004", path.clone(), format!("Physical field {n} is mapped more than once; keys, values, states and metadata need distinct fields"), related);
        }
    }
}

fn contains_secret_like(v: &serde_json::Value) -> bool {
    match v {
        serde_json::Value::Object(m) => m.iter().any(|(k, v)| {
            let k = k.to_ascii_lowercase();
            [
                "password",
                "secret",
                "token",
                "api_key",
                "apikey",
                "client_secret",
                "private_key",
                "access_key",
            ]
            .iter()
            .any(|s| k.contains(s))
                || contains_secret_like(v)
        }),
        serde_json::Value::Array(a) => a.iter().any(contains_secret_like),
        serde_json::Value::String(s) => s.starts_with("dapi") && s.len() > 30,
        _ => false,
    }
}

fn check_suites(c: &mut Ctx) {
    let p = c.p;
    for (si, s) in p.acceptance_suites.iter().enumerate() {
        let mut seen = HashSet::new();
        for (ci, cs) in s.cases.iter().enumerate() {
            let base = format!("/acceptance_suites/{si}/cases/{ci}");
            let cid = cs.id.as_str();
            if !seen.insert(cid) {
                c.err(
                    "DUPLICATE_CASE_ID",
                    "SEM-ID-001",
                    format!("{base}/id"),
                    format!("Duplicate case {cid}"),
                    &[&s.id],
                );
            }
            c.ts(&cs.as_of, format!("{base}/as_of"), &[&s.id, cid]);
            let feature = match p.feature(&cs.feature.id) {
                None => {
                    c.err(
                        "UNKNOWN_FEATURE",
                        "SEM-ID-002",
                        format!("{base}/feature/id"),
                        format!("Unknown feature {}", cs.feature.id),
                        &[&s.id, cid],
                    );
                    None
                }
                Some(f) if f.version != cs.feature.version => {
                    c.err(
                        "VERSION_MISMATCH",
                        "SEM-ID-002",
                        format!("{base}/feature/version"),
                        format!(
                            "Case pins {}@{}; project has {}",
                            f.id, cs.feature.version, f.version
                        ),
                        &[&s.id, cid],
                    );
                    None
                }
                Some(f) => Some(f),
            };
            if let Some(e) = feature.and_then(|f| p.entity(&f.entity)) {
                let keys: BTreeSet<&str> = e.keys.iter().map(|k| k.name.as_str()).collect();
                let mut fps = HashSet::new();
                for (ri, row) in cs.population_rows.iter().enumerate() {
                    if row.keys().map(String::as_str).collect::<BTreeSet<_>>() != keys {
                        c.err(
                            "POPULATION_KEYS",
                            "SEM-ENT-001",
                            format!("{base}/population_rows/{ri}"),
                            "Fixture population rows must contain exactly the entity keys",
                            &[&s.id, cid],
                        );
                    }
                    for k in &e.keys {
                        match row.get(&k.name).map(|v| V::from_json(v, k.ty)) {
                            Some(Ok(V::Null)) | Some(Err(_)) => c.err(
                                "FIXTURE_TYPE_MISMATCH",
                                "SEM-ENT-001",
                                format!("{base}/population_rows/{ri}/{}", k.name),
                                format!("Invalid entity key {}", k.name),
                                &[&s.id, cid],
                            ),
                            _ => {}
                        }
                    }
                    if !fps.insert(serde_json::to_string(row).unwrap_or_default()) {
                        c.err(
                            "DUPLICATE_POPULATION",
                            "SEM-ENT-001",
                            format!("{base}/population_rows/{ri}"),
                            "Duplicate fixture population row",
                            &[&s.id, cid],
                        );
                    }
                }
            }
            for (sid, rows) in &cs.source_rows {
                let Some(src) = p.source(sid) else {
                    c.err(
                        "UNKNOWN_SOURCE",
                        "SEM-ID-002",
                        format!("{base}/source_rows/{sid}"),
                        format!("Unknown fixture source {sid}"),
                        &[&s.id, cid],
                    );
                    continue;
                };
                let cols: BTreeSet<&str> = src.columns.iter().map(|c| c.name.as_str()).collect();
                for (ri, row) in rows
                    .as_array()
                    .map(Vec::as_slice)
                    .unwrap_or(&[])
                    .iter()
                    .enumerate()
                {
                    let rp = format!("{base}/source_rows/{sid}/{ri}");
                    let Some(obj) = row.as_object() else { continue };
                    if obj.keys().map(String::as_str).collect::<BTreeSet<_>>() != cols {
                        c.err(
                            "FIXTURE_COLUMNS",
                            "SEM-SRC-001",
                            rp.clone(),
                            "Fixture row columns must exactly match the source contract",
                            &[&s.id, cid, sid],
                        );
                    }
                    for col in &src.columns {
                        if let Some(v) = obj.get(&col.name) {
                            match V::from_json(v, col.ty) {
                                Ok(V::Null) if !col.nullable => c.err(
                                    "FIXTURE_TYPE_MISMATCH",
                                    "SEM-SRC-001",
                                    format!("{rp}/{}", col.name),
                                    format!("{} is not nullable", col.name),
                                    &[&s.id, cid],
                                ),
                                Err(m) => c.err(
                                    "FIXTURE_TYPE_MISMATCH",
                                    "SEM-SRC-001",
                                    format!("{rp}/{}", col.name),
                                    format!("Expected {}: {m}", col.ty.as_str()),
                                    &[&s.id, cid],
                                ),
                                _ => {}
                            }
                        }
                    }
                }
            }
            let mut status_sources = HashSet::new();
            for (k, st) in cs.context.source_status.iter().enumerate() {
                let sp = format!("{base}/context/source_status/{k}");
                if p.source(st.source()).is_none() {
                    c.err(
                        "UNKNOWN_SOURCE",
                        "SEM-ID-002",
                        format!("{sp}/source"),
                        format!("Unknown status source {}", st.source()),
                        &[&s.id, cid],
                    );
                }
                if !status_sources.insert(st.source().to_string()) {
                    c.err(
                        "DUPLICATE_SOURCE_STATUS",
                        "SEM-MISS-003",
                        sp.clone(),
                        format!("Duplicate status for {}", st.source()),
                        &[&s.id, cid],
                    );
                }
                if let SourceStatus::Available {
                    coverage_start,
                    coverage_end,
                    ..
                } = st
                {
                    let a = c.ts(coverage_start, format!("{sp}/coverage_start"), &[cid]);
                    let b = c.ts(coverage_end, format!("{sp}/coverage_end"), &[cid]);
                    if let (Some(a), Some(b)) = (a, b) {
                        if a >= b {
                            c.err(
                                "INVALID_COVERAGE_INTERVAL",
                                "SEM-MISS-003",
                                sp,
                                "coverage_start must precede coverage_end",
                                &[&s.id, cid],
                            );
                        }
                    }
                }
            }
            for t in [
                &cs.context.requested_at,
                &cs.context.evaluation_completed_at,
            ]
            .into_iter()
            .flatten()
            {
                c.ts(t, format!("{base}/context"), &[cid]);
            }
            if cs.context.mode == SelectionMode::HistoricalServingReplay {
                match (
                    &cs.context.published_values,
                    &cs.context.requested_at,
                    &cs.context.consumer,
                ) {
                    (Some(_), Some(_), Some(cid2)) => match p.consumer(cid2) {
                        None => c.err(
                            "UNKNOWN_CONSUMER",
                            "SEM-ID-002",
                            format!("{base}/context/consumer"),
                            format!("Unknown consumer {cid2}"),
                            &[&s.id, cid],
                        ),
                        Some(cons) => {
                            if cons.selection.mode != cs.context.mode {
                                c.err(
                                    "CONSUMER_MODE_MISMATCH",
                                    "SEM-REPLAY-001",
                                    format!("{base}/context/consumer"),
                                    "Consumer and fixture selection modes disagree",
                                    &[&s.id, cid],
                                );
                            }
                            if let Some(fs) = p.feature_set(&cons.feature_set.feature_set) {
                                if !fs.features.contains(&cs.feature) {
                                    c.err("CONSUMER_FEATURE_MISMATCH", "SEM-REPLAY-002", format!("{base}/context/consumer"), "Feature/version is not in the consumer's pinned feature set", &[&s.id, cid]);
                                }
                            }
                        }
                    },
                    _ => c.err(
                        "REPLAY_CONTEXT_REQUIRED",
                        "SEM-REPLAY-001",
                        format!("{base}/context"),
                        "Replay requires observed publications, requested_at and a consumer",
                        &[&s.id, cid],
                    ),
                }
            }
            for (k, pv) in cs.context.published_values.iter().flatten().enumerate() {
                let pp = format!("{base}/context/published_values/{k}");
                match p.feature(&pv.feature) {
                    None => c.err(
                        "UNKNOWN_FEATURE",
                        "SEM-ID-002",
                        format!("{pp}/feature"),
                        format!("Unknown feature {}", pv.feature),
                        &[cid],
                    ),
                    Some(f) => {
                        if f.version != pv.version {
                            c.err(
                                "VERSION_MISMATCH",
                                "SEM-ID-002",
                                format!("{pp}/version"),
                                "Publication version differs from the project",
                                &[cid],
                            );
                        }
                        match V::from_json(&pv.value, f.output.ty) {
                            Ok(V::Null) if !f.output.nullable => c.err(
                                "FIXTURE_TYPE_MISMATCH",
                                "SEM-OUT-001",
                                format!("{pp}/value"),
                                "Publication value is null for a non-null output",
                                &[cid],
                            ),
                            Err(m) => c.err(
                                "FIXTURE_TYPE_MISMATCH",
                                "SEM-OUT-001",
                                format!("{pp}/value"),
                                m,
                                &[cid],
                            ),
                            _ => {}
                        }
                    }
                }
                let a = c.ts(&pv.as_of, format!("{pp}/as_of"), &[cid]);
                let b = c.ts(&pv.published_at, format!("{pp}/published_at"), &[cid]);
                if let (Some(a), Some(b)) = (a, b) {
                    if b < a {
                        c.err(
                            "PUBLICATION_BEFORE_AS_OF",
                            "SEM-REPLAY-002",
                            pp,
                            "A publication cannot precede its feature as-of",
                            &[cid],
                        );
                    }
                }
            }
        }
    }
}
