//! Databricks adapter `dev.featlas:databricks@0.1.0` (Azure Databricks is the first target).
//!
//! Roles: compute (Spark SQL lowering of the canonical plan), catalog (Unity
//! Catalog source reads), offline_store (Unity Catalog Delta feature tables),
//! orchestrator (declared; job integration unverified). There is no
//! online_store role in this version. Capability assessment lives in
//! `databricks_caps.rs` and is driven by `databricks_lock.json`.
//!
//! This module only *generates* SQL text and statements. It performs no I/O,
//! holds no credentials and imports no SDK. Execution happens in the thin
//! Python host (`python/featlas_databricks`), which runs these statements in a
//! Spark session it is explicitly authorized to use.
//!
//! Lowering invariants (each mirrors a canonical evaluator stage):
//! * session: `SET TIME ZONE 'UTC'`, `spark.sql.ansi.enabled=true` (checked overflow);
//! * every source is read once through `fl_raw_<source>` with logical names;
//! * availability filter → primary-key duplicate checks → logical-key
//!   partition → `ROW_NUMBER()` latest revision → tie checks → tombstones;
//! * event-window filtering happens on selected revisions only (a
//!   non-partition predicate is never pushed below the window by Spark);
//! * joins on complete key pairs (tenant keys included) and half-open valid
//!   intervals; cardinality, null-key, unmatched and interval checks are
//!   separate zero-row assertion queries;
//! * `and`/`or`/`if_else`/`coalesce` lower to ordered `CASE`/`COALESCE`
//!   so errors in unselected branches are not raised; division guards zero
//!   explicitly; number results are checked finite;
//! * population left-joins aggregates so zero-event entities survive.

use featlas_core::capability::{AdapterDescriptor, Assessment, AssessmentContext};
use featlas_core::diag::{Diagnostic, Stage};
use featlas_core::model::*;
use featlas_core::plan::{FeaturePlan, FeatureSetPlan, Plan, SourceNode, Step};
use serde::Serialize;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};

pub const IMPLEMENTATION: &str = "dev.featlas:databricks";
pub const VERSION: &str = "0.1.0";
const ROLES: &[Role] = &[
    Role::Compute,
    Role::Catalog,
    Role::OfflineStore,
    Role::Orchestrator,
];

pub struct Databricks;

impl AdapterDescriptor for Databricks {
    fn implementation(&self) -> &str {
        IMPLEMENTATION
    }
    fn version(&self) -> &str {
        VERSION
    }
    fn roles(&self) -> &[Role] {
        ROLES
    }
    fn external(&self) -> bool {
        true
    }
    fn configuration_schema(&self) -> Value {
        json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "urn:featlas:adapter:dev.featlas:databricks:0.1.0:configuration",
            "type": "object",
            "additionalProperties": false,
            "required": ["connection_profile", "feature_api"],
            "properties": {
                "connection_profile": {"type": "string", "pattern": "^[A-Za-z0-9_.-]{1,64}$", "description": "Name of an externally managed Databricks unified-auth profile; never a credential."},
                "feature_api": {"const": "uc_feature_tables", "description": "Unity Catalog feature tables via generated Spark SQL; Feature Views are not used by this version."},
                "execution": {"enum": ["databricks_connect", "spark_session"], "description": "How the host obtains a Spark session."},
                "source_status_table": {
                    "type": "object", "additionalProperties": false, "required": ["namespace", "name"],
                    "properties": {"namespace": {"type": "array", "items": {"type": "string", "minLength": 1}, "minItems": 2, "maxItems": 2}, "name": {"type": "string", "minLength": 1}}
                }
            }
        })
    }
    fn assess(&self, role: Role, capability: &str, ctx: &AssessmentContext) -> Assessment {
        crate::databricks_caps::assess(role, capability, ctx)
    }
    fn implementation_sha256(&self) -> Option<String> {
        Some(crate::databricks_caps::implementation_sha256())
    }
    fn runtime_versions(&self) -> Map<String, Value> {
        crate::databricks_caps::runtime_versions()
    }
    fn check_bindings(&self, p: &Project, d: &Deployment, adapter_id: &str) -> Vec<Diagnostic> {
        let mut out = Vec::new();
        let di = p.deployments.iter().position(|x| x.id == d.id).unwrap_or(0);
        let check_location = |loc: &Location, path: String, out: &mut Vec<Diagnostic>| match loc {
            Location::Table {
                namespace,
                name,
                format,
            } => {
                if namespace.len() != 2 {
                    out.push(diag("DATABRICKS_NAMESPACE", path.clone(), format!("Unity Catalog tables need namespace [catalog, schema]; got {} components", namespace.len())));
                }
                for part in namespace.iter().chain(std::iter::once(name)) {
                    if !valid_uc_identifier(part) {
                        out.push(diag("DATABRICKS_IDENTIFIER", path.clone(), format!("{part:?} is not a supported Unity Catalog identifier (letters, digits, underscore; max 255)")));
                    }
                }
                if format != "delta" {
                    out.push(diag("DATABRICKS_FORMAT", path, format!("Format {format} is unsupported; this version reads and writes Delta tables")));
                }
            }
            other => {
                let kind = match other {
                    Location::File { .. } => "file",
                    Location::Resource { .. } => "resource",
                    Location::Fixture { .. } => "fixture",
                    Location::Table { .. } => "table",
                };
                out.push(diag("DATABRICKS_LOCATION_KIND", path, format!("Location kind {kind} is not supported by {IMPLEMENTATION}@{VERSION}; bind a Unity Catalog table")))
            }
        };
        for (k, b) in d.source_bindings.iter().enumerate() {
            if b.adapter == adapter_id {
                check_location(
                    &b.location,
                    format!("/deployments/{di}/source_bindings/{k}/location"),
                    &mut out,
                );
                case_collisions(
                    b.column_mapping.values().filter_map(Value::as_str),
                    format!("/deployments/{di}/source_bindings/{k}/column_mapping"),
                    &mut out,
                );
            }
        }
        for (k, t) in d.output_targets.iter().enumerate() {
            if t.adapter != adapter_id {
                continue;
            }
            check_location(
                &t.location,
                format!("/deployments/{di}/output_targets/{k}/location"),
                &mut out,
            );
            let mut cols: Vec<&str> = t
                .entity_key_mapping
                .values()
                .filter_map(Value::as_str)
                .collect();
            let m = &t.metadata_columns;
            cols.extend([
                m.as_of.as_str(),
                m.published_at.as_str(),
                m.publication_sequence.as_str(),
                m.publication_id.as_str(),
                m.materialization_id.as_str(),
            ]);
            for f in &t.feature_mapping {
                cols.push(&f.column);
                cols.push(&f.state_column);
            }
            case_collisions(
                cols.iter().copied(),
                format!("/deployments/{di}/output_targets/{k}"),
                &mut out,
            );
            for c in cols {
                if !valid_column(c) {
                    out.push(diag(
                        "DATABRICKS_IDENTIFIER",
                        format!("/deployments/{di}/output_targets/{k}"),
                        format!("Column {c:?} is not a supported Delta column name"),
                    ));
                }
            }
        }
        out
    }
}

fn diag(code: &str, path: String, msg: String) -> Diagnostic {
    Diagnostic::new(code, Stage::Planning, "SEM-PORT-004", path, msg)
}

fn valid_uc_identifier(s: &str) -> bool {
    !s.is_empty() && s.len() <= 255 && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn valid_column(s: &str) -> bool {
    !s.is_empty() && s.len() <= 255 && !s.chars().any(|c| " ,;{}()\n\t=".contains(c))
}

/// Delta/Spark resolve column names case-insensitively by default.
fn case_collisions<'a>(
    names: impl Iterator<Item = &'a str>,
    path: String,
    out: &mut Vec<Diagnostic>,
) {
    let mut seen = BTreeMap::new();
    for n in names {
        if let Some(prev) = seen.insert(n.to_ascii_lowercase(), n) {
            if prev != n {
                out.push(diag(
                    "PHYSICAL_COLUMN_CASE_COLLISION",
                    path.clone(),
                    format!(
                        "{prev:?} and {n:?} collide under Databricks case-insensitive resolution"
                    ),
                ));
            }
        }
    }
}

pub fn quote(id: &str) -> String {
    format!("`{}`", id.replace('`', "``"))
}

pub fn table_name(loc: &Location) -> Result<String, String> {
    match loc {
        Location::Table {
            namespace, name, ..
        } if namespace.len() == 2 => Ok(format!(
            "{}.{}.{}",
            quote(&namespace[0]),
            quote(&namespace[1]),
            quote(name)
        )),
        _ => Err("Databricks lowering requires a [catalog, schema] table location".into()),
    }
}

pub fn spark_type(t: LogicalType) -> &'static str {
    match t {
        LogicalType::Integer => "BIGINT",
        LogicalType::Number => "DOUBLE",
        LogicalType::Boolean => "BOOLEAN",
        LogicalType::String => "STRING",
        LogicalType::Timestamp => "TIMESTAMP",
        LogicalType::Date => "DATE",
    }
}

fn sql_string(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}

fn raise(code: &str, msg: &str) -> String {
    format!(
        "raise_error({})",
        sql_string(&format!("FEATLAS:{code}: {msg}"))
    )
}

/// A zero-row assertion: any returned row is a contract violation with `code`.
#[derive(Debug, Clone, Serialize)]
pub struct Check {
    pub name: String,
    pub code: String,
    pub rule: String,
    pub sql: String,
    /// Logical sources this assertion reads (skipped when such a source is unavailable).
    pub reads: Vec<String>,
    /// Set for transform-stage assertions of one feature; skipped when that feature runs its
    /// degraded (coverage-not-established) variant, exactly as the core skips the transform.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub feature: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Statement {
    pub name: String,
    pub purpose: String,
    /// For `view` statements: a parameterized SELECT whose bound result the host registers as a
    /// temporary view named `view` (Spark forbids parameter markers inside CREATE VIEW).
    /// Otherwise a complete statement.
    pub sql: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub view: Option<String>,
    /// Logical sources this statement reads.
    pub reads: Vec<String>,
}

/// Everything the host needs to run one feature-set snapshot on Databricks.
#[derive(Debug, Clone, Serialize)]
pub struct Artifacts {
    pub generator: String,
    pub plan_digest: String,
    pub logical_plan_digest: String,
    pub feature_set: String,
    pub feature_set_version: String,
    pub session: Vec<Statement>,
    /// Source binding views (`fl_raw_<source>`), one per bound source.
    pub bind_sources: Vec<Statement>,
    /// Reconstruction and computation views in canonical stage order.
    pub stages: Vec<Statement>,
    /// Zero-row assertions; run after the views they reference and before publication.
    pub checks: Vec<Check>,
    /// Per feature: computed and degraded (coverage-not-established) variants.
    pub features: Vec<FeatureStatements>,
    pub snapshot: Statement,
    pub create_table: Statement,
    pub publish: Statement,
    pub table_properties: Statement,
    pub expected_table_properties: BTreeMap<String, String>,
    pub evidence: Vec<Statement>,
    pub consumer_read: Statement,
    pub parameters: BTreeMap<String, String>,
    /// Physical schema each bound source must have (verified by host preflight before reading).
    pub expected_source_schemas: BTreeMap<String, Value>,
    pub output_table: String,
    /// Physical names of the five publication metadata fields of the output target.
    pub metadata_columns: PublicationColumns,
    /// Deployment resource limits the host enforces where measurable (SEM-BUDGET-001).
    pub budget: Budget,
    /// Retention contracts the host verifies against the physical tables (SEM-DEPLOY-002).
    pub retention: RetentionContract,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RetentionContract {
    /// Per bound source: physical table, availability column and the declared earliest
    /// reproducible instant.
    pub sources: BTreeMap<String, SourceRetentionCheck>,
    pub output_retention_seconds: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceRetentionCheck {
    pub table: String,
    pub available_time: String,
    pub earliest_reproducible_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct FeatureStatements {
    pub feature: String,
    pub version: String,
    pub view: String,
    pub computed: Statement,
    pub degraded: Statement,
    pub missingness: Missingness,
    /// Output and quality rules in the core's rank order (SEM-OUT-001, SEM-QUAL-001).
    pub quality: Vec<QualityStatement>,
}

/// One output/quality rule over a feature view; `sql` returns the violating rows.
#[derive(Debug, Clone, Serialize)]
pub struct QualityStatement {
    pub name: String,
    /// 0 type/nullability, 1 minimum, 2 maximum, 3 enumeration, 4+i declared check i.
    pub rank: usize,
    pub rule: String,
    pub code: String,
    /// `error` fails the run; `warning` is recorded, values unchanged; `quarantine` blocks publication.
    pub outcome: String,
    pub check: String,
    pub sql: String,
}

/// A numeric bound as a Spark literal that compares exactly with BIGINT/DOUBLE values.
fn bound_literal(x: f64) -> String {
    if x.fract() == 0.0 && x.abs() < 1e15 {
        format!("{}", x as i64)
    } else {
        format!("CAST('{x:?}' AS DOUBLE)")
    }
}

/// Output constraints and declared quality checks of one feature, lowered over its view.
fn quality_statements(f: &Feature, view: &str, keys: &str) -> Vec<QualityStatement> {
    let o = &f.output;
    let v = "fl_value";
    let numeric = matches!(o.ty, LogicalType::Integer | LogicalType::Number);
    let mut rules: Vec<(usize, &str, String, &str, &str, String)> = Vec::new();
    if !o.nullable {
        rules.push((0, "SEM-OUT-001", "OUTPUT_TYPE_MISMATCH".into(), "error", "nullability", format!("{v} IS NULL")));
    }
    if let Some(m) = o.constraints.minimum.filter(|_| numeric) {
        rules.push((1, "SEM-OUT-001", "QUALITY_VIOLATION".into(), "error", "minimum", format!("{v} < {}", bound_literal(m))));
    }
    if let Some(m) = o.constraints.maximum.filter(|_| numeric) {
        rules.push((2, "SEM-OUT-001", "QUALITY_VIOLATION".into(), "error", "maximum", format!("{v} > {}", bound_literal(m))));
    }
    if let Some(allowed) = &o.constraints.enumeration {
        let lits: Vec<String> = allowed.iter().map(|a| literal_json(a, o.ty)).collect();
        rules.push((3, "SEM-OUT-001", "QUALITY_VIOLATION".into(), "error", "enumeration", if lits.is_empty() { format!("{v} IS NOT NULL") } else { format!("{v} IS NOT NULL AND NOT ({v} IN ({}))", lits.join(", ")) }));
    }
    for (i, q) in f.quality.checks.iter().enumerate() {
        let predicate = match q {
            QualityCheck::NotNull { .. } => format!("fl_state IN ('ok', 'empty') AND {v} IS NULL"),
            QualityCheck::Range { minimum, maximum, .. } => {
                let mut parts = Vec::new();
                if let Some(m) = minimum { parts.push(format!("{v} < {}", bound_literal(*m))); }
                if let Some(m) = maximum { parts.push(format!("{v} > {}", bound_literal(*m))); }
                if parts.is_empty() || !numeric { "false".into() } else { format!("({})", parts.join(" OR ")) }
            }
            QualityCheck::Finite { .. } => match o.ty {
                LogicalType::Number => format!("(isnan({v}) OR abs({v}) = double('Infinity'))"),
                LogicalType::Integer => "false".into(),
                _ => format!("{v} IS NOT NULL"),
            },
        };
        let outcome = if q.severity() == "warn" { "warning" } else if f.quality.on_violation == "quarantine" { "quarantine" } else { "error" };
        let code = match outcome { "warning" => "QUALITY_WARNING", "quarantine" => "QUALITY_QUARANTINED", _ => "QUALITY_VIOLATION" };
        rules.push((4 + i, "SEM-QUAL-001", code.into(), outcome, q.kind(), predicate));
    }
    rules
        .into_iter()
        .map(|(rank, rule, code, outcome, check, predicate)| QualityStatement {
            name: format!("{view}_quality_{rank}_{check}"),
            rank,
            rule: rule.into(),
            code,
            outcome: outcome.into(),
            check: check.into(),
            sql: format!("SELECT {keys}, {v} AS value, fl_state AS state FROM {view} WHERE {predicate}"),
        })
        .collect()
}

struct Lowering<'a> {
    plan: &'a Plan,
    fs: &'a FeatureSetPlan,
    stages: Vec<Statement>,
    checks: Vec<Check>,
    reconstructed: BTreeSet<String>,
}

fn boundary_op(b: Boundary, inclusive: &str, exclusive: &str) -> String {
    if b == Boundary::Inclusive {
        inclusive.into()
    } else {
        exclusive.into()
    }
}

fn view_suffix(b: Boundary) -> &'static str {
    if b == Boundary::Inclusive {
        "ai"
    } else {
        "ax"
    }
}

fn safe(id: &str) -> String {
    id.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

impl<'a> Lowering<'a> {
    fn source(&self, id: &str) -> &'a SourceNode {
        &self.plan.logical.sources[id]
    }

    /// Stages 2–5 as views; returns the selected-revision view name.
    fn reconstruct(&mut self, sid: &str, upper: Boundary) -> String {
        let s = self.source(sid);
        let base = format!("fl_{}_{}", safe(sid), view_suffix(upper));
        let selected = format!("{base}_selected");
        if !self.reconstructed.insert(selected.clone()) {
            return selected;
        }
        let (s0, c0) = (self.stages.len(), self.checks.len());
        let raw = format!("fl_raw_{}", safe(sid));
        let avail = format!("{base}_available");
        let cmp = boundary_op(upper, "<=", "<");
        self.stages.push(Statement {
            name: avail.clone(),
            purpose: format!("{sid}: availability cutoff {} as_of (stage 2)", cmp),
            sql: format!(
                "CREATE OR REPLACE TEMP VIEW {avail} AS\nSELECT * FROM {raw} WHERE {} {cmp} :as_of",
                quote(&s.available_time)
            ),
            reads: vec![],
            view: None,
        });
        let pk = s
            .primary_key
            .iter()
            .map(|c| quote(c))
            .collect::<Vec<_>>()
            .join(", ");
        let all_cols = s
            .columns
            .iter()
            .map(|c| quote(&c.name))
            .collect::<Vec<_>>()
            .join(", ");
        let dup_sql = match s.duplicate_policy {
            DuplicatePolicy::Reject => format!("SELECT {pk}, count(*) AS n FROM {avail} GROUP BY {pk} HAVING count(*) > 1"),
            DuplicatePolicy::CollapseIdentical => format!("SELECT {pk}, count(*) AS n FROM (SELECT DISTINCT {all_cols} FROM {avail}) GROUP BY {pk} HAVING count(*) > 1"),
        };
        self.checks.push(Check {
            name: format!("{base}_primary_key_duplicates"),
            code: "DUPLICATE_CONFLICT".into(),
            rule: "SEM-REV-002".into(),
            sql: dup_sql,
            reads: vec![],
            feature: None,
        });
        let deduped = format!("{base}_deduped");
        self.stages.push(Statement {
            name: deduped.clone(),
            purpose: format!("{sid}: collapse identical physical duplicates (stage 3)"),
            sql: format!(
                "CREATE OR REPLACE TEMP VIEW {deduped} AS\nSELECT DISTINCT {all_cols} FROM {avail}"
            ),
            reads: vec![],
            view: None,
        });
        let lk = s
            .logical_key
            .iter()
            .map(|c| quote(c))
            .collect::<Vec<_>>()
            .join(", ");
        if s.record_kind == "append_only" {
            self.checks.push(Check {
                name: format!("{base}_logical_conflicts"),
                code: "DUPLICATE_CONFLICT".into(),
                rule: "SEM-REV-002".into(),
                sql: format!(
                    "SELECT {lk}, count(*) AS n FROM {deduped} GROUP BY {lk} HAVING count(*) > 1"
                ),
                reads: vec![],
                feature: None,
            });
            self.stages.push(Statement {
                name: selected.clone(),
                purpose: format!("{sid}: immutable logical events (stage 4)"),
                sql: format!("CREATE OR REPLACE TEMP VIEW {selected} AS\nSELECT * FROM {deduped}"),
                reads: vec![],
                view: None,
            });
        } else {
            let rev = s
                .revision_order
                .iter()
                .map(|o| quote(&o.column))
                .collect::<Vec<_>>()
                .join(", ");
            let order = s
                .revision_order
                .iter()
                .map(|o| {
                    format!(
                        "{} {}",
                        quote(&o.column),
                        if o.direction == "desc" { "DESC" } else { "ASC" }
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            self.checks.push(Check {
                name: format!("{base}_revision_ties"),
                code: "DUPLICATE_CONFLICT".into(),
                rule: "SEM-REV-001".into(),
                sql: format!("SELECT {lk}, {rev}, count(*) AS n FROM {deduped} GROUP BY {lk}, {rev} HAVING count(*) > 1"), reads: vec![], feature: None });
            let tomb = match &s.tombstone {
                Some((c, v)) => format!(
                    "\nWHERE NOT ({} <=> {})",
                    quote(c),
                    if *v { "true" } else { "false" }
                ),
                None => String::new(),
            };
            self.stages.push(Statement {
                name: selected.clone(),
                purpose: format!("{sid}: latest eligible revision per logical key, then tombstones (stages 4-5)"),
                sql: format!(
                    "CREATE OR REPLACE TEMP VIEW {selected} AS\nSELECT {all_cols} FROM (\n  SELECT *, ROW_NUMBER() OVER (PARTITION BY {lk} ORDER BY {order}) AS fl_rn FROM {deduped}\n) WHERE fl_rn = 1{}",
                    tomb.replace("\nWHERE", " AND")
                ),
                reads: vec![], view: None });
        }
        self.tag(s0, c0, &[sid.to_string()]);
        selected
    }

    fn tag(&mut self, s0: usize, c0: usize, reads: &[String]) {
        for st in &mut self.stages[s0..] {
            st.reads = reads.to_vec();
        }
        for c in &mut self.checks[c0..] {
            c.reads = reads.to_vec();
        }
    }

    fn population(&mut self) -> String {
        let pop = &self.fs.population;
        let sel = self.reconstruct(&pop.source, pop.availability_upper);
        let (s0, c0) = (self.stages.len(), self.checks.len());
        let s = self.source(&pop.source);
        let view = format!("fl_population_{}", safe(&self.fs.id));
        let keys = pop
            .keys
            .iter()
            .map(|k| format!("{} AS {}", quote(&k.source_column), quote(&k.name)))
            .collect::<Vec<_>>()
            .join(", ");
        let key_names = pop
            .keys
            .iter()
            .map(|k| quote(&k.name))
            .collect::<Vec<_>>()
            .join(", ");
        self.stages.push(Statement {
            name: view.clone(),
            purpose: "Eligible population at as_of (stage 6); zero-event entities are retained by later left joins".into(),
            sql: format!("CREATE OR REPLACE TEMP VIEW {view} AS\nSELECT {keys} FROM {sel} WHERE {} {} :as_of", quote(&s.event_time), boundary_op(pop.event_time_upper, "<=", "<")), reads: vec![], view: None });
        self.checks.push(Check { name: format!("{view}_duplicates"), code: "DUPLICATE_POPULATION".into(), rule: "SEM-ENT-001".into(), sql: format!("SELECT {key_names}, count(*) AS n FROM {view} GROUP BY {key_names} HAVING count(*) > 1"), reads: vec![], feature: None });
        let src = pop.source.clone();
        self.tag(s0, c0, &[src]);
        view
    }
}

/// Lower an expression. `col` maps (alias, column) to a SQL reference.
fn lower_expr(
    e: &Expr,
    col: &dyn Fn(&str, &str) -> String,
    ty_of: &dyn Fn(&Expr) -> Option<LogicalType>,
    depth: usize,
) -> Result<String, String> {
    if depth > 24 {
        return Err("Expression nesting exceeds the lowering limit (24)".into());
    }
    let l = |x: &Expr| lower_expr(x, col, ty_of, depth + 1);
    Ok(match e {
        Expr::Literal { ty, value } => match (ty.as_str(), value) {
            ("null", _) => "NULL".into(),
            ("integer", v) => format!(
                "CAST({} AS BIGINT)",
                v.as_i64().ok_or("bad integer literal")?
            ),
            ("number", v) => format!(
                "CAST({:?} AS DOUBLE)",
                v.as_f64().ok_or("bad number literal")?
            ),
            ("boolean", Value::Bool(b)) => (if *b { "true" } else { "false" }).into(),
            ("string", Value::String(s)) => sql_string(s),
            ("timestamp", Value::String(s)) => format!("TIMESTAMP{}", sql_string(s)),
            ("date", Value::String(s)) => format!("DATE{}", sql_string(s)),
            _ => return Err(format!("Unsupported literal {ty}")),
        },
        Expr::Column { input, column } => col(input, column),
        Expr::Feature { feature, .. } => col("__feature__", feature),
        Expr::Call { operator, args } => {
            let a: Vec<String> = args.iter().map(l).collect::<Result<_, _>>()?;
            let finite = |x: String| {
                format!("(CASE WHEN ({x}) IS NULL THEN NULL WHEN isnan({x}) OR abs({x}) = double('Infinity') THEN {} ELSE ({x}) END)", raise("NON_FINITE_RESULT", "non-finite number"))
            };
            match operator {
                Operator::Add | Operator::Subtract | Operator::Multiply => {
                    let op = match operator {
                        Operator::Add => "+",
                        Operator::Subtract => "-",
                        _ => "*",
                    };
                    // BIGINT overflow raises under ANSI mode; DOUBLE results are checked finite.
                    let x = format!("({} {op} {})", a[0], a[1]);
                    if ty_of(e) == Some(LogicalType::Number) { finite(x) } else { x }
                }
                // Null operands yield null before the zero test (left first), matching the core.
                Operator::Divide => finite(format!("(CASE WHEN ({x}) IS NULL OR ({b}) IS NULL THEN NULL WHEN ({b}) = 0 THEN {} ELSE CAST({x} AS DOUBLE) / CAST({b} AS DOUBLE) END)", raise("DIVISION_BY_ZERO", "division by zero"), x = a[0], b = a[1])),
                Operator::SafeDivide => finite(format!("(CASE WHEN ({x}) IS NULL OR ({b}) IS NULL THEN NULL WHEN ({b}) = 0 THEN NULL ELSE CAST({x} AS DOUBLE) / CAST({b} AS DOUBLE) END)", x = a[0], b = a[1])),
                Operator::Absolute => format!("abs({})", a[0]),
                Operator::Equal => format!("({} = {})", a[0], a[1]),
                Operator::NotEqual => format!("({} <> {})", a[0], a[1]),
                Operator::LessThan => format!("({} < {})", a[0], a[1]),
                Operator::LessOrEqual => format!("({} <= {})", a[0], a[1]),
                Operator::GreaterThan => format!("({} > {})", a[0], a[1]),
                Operator::GreaterOrEqual => format!("({} >= {})", a[0], a[1]),
                Operator::Not => format!("(NOT {})", a[0]),
                Operator::IsNull => format!("({} IS NULL)", a[0]),
                Operator::And | Operator::Or => {
                    // Ordered CASE keeps short-circuit semantics; optimizers do not reorder CASE branches.
                    let (stop, stop_v, all_v) = if *operator == Operator::And { ("false", "false", "true") } else { ("true", "true", "false") };
                    let mut s = String::from("(CASE");
                    for x in &a {
                        s.push_str(&format!(" WHEN ({x}) = {stop} THEN {stop_v}"));
                    }
                    let any_null = a.iter().map(|x| format!("({x}) IS NULL")).collect::<Vec<_>>().join(" OR ");
                    s.push_str(&format!(" WHEN {any_null} THEN NULL ELSE {all_v} END)"));
                    s
                }
                Operator::Coalesce => format!("coalesce({})", a.join(", ")),
                Operator::IfElse => format!("(CASE WHEN ({}) = true THEN {} ELSE {} END)", a[0], a[1], a[2]),
            }
        }
    })
}

fn empty_sql(f: &FeaturePlan) -> (String, &'static str) {
    match &f.missingness.empty {
        EmptyPolicy::Value { value } => (literal_json(value, f.output.ty), "'empty'"),
        EmptyPolicy::Null => (
            format!("CAST(NULL AS {})", spark_type(f.output.ty)),
            "'empty'",
        ),
        EmptyPolicy::Error => (
            raise("EMPTY_FEATURE", &format!("{} has no eligible rows", f.id)),
            "'empty'",
        ),
    }
}

fn literal_json(v: &Value, ty: LogicalType) -> String {
    match v {
        Value::Null => format!("CAST(NULL AS {})", spark_type(ty)),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => format!("CAST({n} AS {})", spark_type(ty)),
        Value::String(s) => format!("CAST({} AS {})", sql_string(s), spark_type(ty)),
        _ => "NULL".into(),
    }
}

/// Lower one feature set of a plan into Spark SQL artifacts.
pub fn lower(plan: &Plan, feature_set: &str, p: &Project) -> Result<Artifacts, String> {
    let fs = plan
        .logical
        .feature_sets
        .iter()
        .find(|f| f.id == feature_set)
        .ok_or_else(|| format!("Feature set {feature_set} is not in the plan"))?;
    let target = plan
        .bindings
        .outputs
        .iter()
        .find(|t| t.feature_set == fs.id && t.version == fs.version && t.role == Role::OfflineStore)
        .ok_or("No offline_store target for this feature set")?;
    let out_adapter = plan
        .bindings
        .adapters
        .iter()
        .find(|a| a.id == target.adapter)
        .ok_or("Unknown output adapter")?;
    if out_adapter.implementation != IMPLEMENTATION {
        return Err(format!(
            "Offline target uses {}; this lowering publishes only to {IMPLEMENTATION}",
            out_adapter.implementation
        ));
    }
    let output_table = table_name(&target.location)?;
    let mut safe_names = BTreeMap::new();
    for id in plan
        .logical
        .sources
        .keys()
        .map(|s| ("source", s.clone()))
        .chain(fs.features.iter().map(|f| ("feature", f.id.clone())))
    {
        if let Some(prev) = safe_names.insert((id.0, safe(&id.1)), id.1.clone()) {
            return Err(format!(
                "{} ids {prev:?} and {:?} map to the same Spark view name; rename one",
                id.0, id.1
            ));
        }
    }
    let mut lw = Lowering {
        plan,
        fs,
        stages: Vec::new(),
        checks: Vec::new(),
        reconstructed: BTreeSet::new(),
    };
    let mut bind_sources = Vec::new();
    let mut expected_schemas = BTreeMap::new();
    let mut retention_sources = BTreeMap::new();
    for sid in plan.logical.sources.keys() {
        let b = plan
            .bindings
            .sources
            .iter()
            .find(|b| &b.source == sid)
            .ok_or_else(|| format!("Source {sid} is not bound"))?;
        let s = lw.source(sid);
        let cols = s
            .columns
            .iter()
            .map(|c| {
                let phys = b
                    .column_mapping
                    .get(&c.name)
                    .and_then(Value::as_str)
                    .ok_or(format!("{sid}.{} unmapped", c.name))?;
                // Rename only (SEM-PORT-003): no cast, unit conversion or default. Physical types are verified by preflight.
                Ok(format!("{} AS {}", quote(phys), quote(&c.name)))
            })
            .collect::<Result<Vec<_>, String>>()?
            .join(",\n  ");
        bind_sources.push(Statement {
            name: format!("fl_raw_{}", safe(sid)),
            purpose: format!(
                "Bind logical source {sid}@{} read-only through its complete column mapping",
                s.version
            ),
            sql: format!(
                "CREATE OR REPLACE TEMP VIEW fl_raw_{} AS\nSELECT\n  {cols}\nFROM {}",
                safe(sid),
                table_name(&b.location)?
            ),
            reads: vec![sid.clone()],
            view: None,
        });
        let phys_avail = b
            .column_mapping
            .get(&s.available_time)
            .and_then(Value::as_str)
            .unwrap_or(&s.available_time)
            .to_string();
        retention_sources.insert(
            sid.clone(),
            SourceRetentionCheck {
                table: table_name(&b.location)?,
                available_time: phys_avail,
                earliest_reproducible_at: s.earliest_reproducible_at.clone(),
            },
        );
        expected_schemas.insert(
            sid.clone(),
            json!({
                "table": table_name(&b.location)?,
                "columns": s.columns.iter().map(|c| json!({"logical": c.name, "physical": b.column_mapping.get(&c.name), "type": spark_type(c.ty), "nullable": c.nullable})).collect::<Vec<_>>(),
            }),
        );
        // Non-nullable columns must not contain nulls (invalid_row_policy: reject).
        let not_null: Vec<String> = s
            .columns
            .iter()
            .filter(|c| !c.nullable)
            .map(|c| format!("{} IS NULL", quote(&c.name)))
            .collect();
        if !not_null.is_empty() {
            lw.checks.push(Check {
                name: format!("fl_raw_{}_invalid_rows", safe(sid)),
                code: "INVALID_SOURCE_ROW".into(),
                rule: "SEM-SRC-001".into(),
                sql: format!(
                    "SELECT * FROM fl_raw_{} WHERE {} LIMIT 1",
                    safe(sid),
                    not_null.join(" OR ")
                ),
                reads: vec![sid.clone()],
                feature: None,
            });
        }
    }
    let population = lw.population();
    let key_names: Vec<String> = fs.population.keys.iter().map(|k| k.name.clone()).collect();
    let mut features = Vec::new();
    for f in &fs.features {
        let view = format!("fl_feature_{}", safe(&f.id));
        let c0 = lw.checks.len();
        let mut computed = lower_feature(&mut lw, f, &population, &key_names, &view, p)?;
        computed.reads = f.coverage.sources.clone();
        for c in &mut lw.checks[c0..] {
            if c.reads.is_empty() {
                c.reads = f.coverage.sources.clone();
            }
            if c.name.starts_with(&view) {
                c.feature = Some(f.id.clone());
            }
        }
        let degraded_state = ":coverage_state";
        let policy_error = f.missingness.insufficient_history == NullOrError::Error
            || f.missingness.source_unavailable == NullOrError::Error;
        let value = if policy_error {
            format!(
                "CASE WHEN {degraded_state} = 'insufficient_history' AND {ih} THEN {} WHEN {degraded_state} = 'source_unavailable' AND {su} THEN {} ELSE CAST(NULL AS {}) END",
                raise("INSUFFICIENT_HISTORY", "coverage not established"),
                raise("SOURCE_UNAVAILABLE", "source unavailable"),
                spark_type(f.output.ty),
                ih = f.missingness.insufficient_history == NullOrError::Error,
                su = f.missingness.source_unavailable == NullOrError::Error,
            )
        } else {
            format!("CAST(NULL AS {})", spark_type(f.output.ty))
        };
        let keys = key_names
            .iter()
            .map(|k| quote(k))
            .collect::<Vec<_>>()
            .join(", ");
        let degraded = Statement {
            name: view.clone(),
            purpose: format!("{}: coverage not established; typed null with the core-computed state and no source reads", f.id),
            sql: format!("CREATE OR REPLACE TEMP VIEW {view} AS\nSELECT {keys}, {value} AS fl_value, CAST({degraded_state} AS STRING) AS fl_state FROM {population}"),
            reads: vec![fs.population.source.clone()], view: None };
        let model = p.feature(&f.id).ok_or_else(|| format!("{} is not in the project", f.id))?;
        let quality = quality_statements(model, &view, &keys);
        features.push(FeatureStatements {
            feature: f.id.clone(),
            version: f.version.clone(),
            view,
            computed,
            degraded,
            missingness: f.missingness.clone(),
            quality,
        });
    }
    // Snapshot assembly with physical names.
    let mut select = Vec::new();
    for k in &fs.population.keys {
        let phys = target
            .entity_key_mapping
            .get(&k.name)
            .and_then(Value::as_str)
            .ok_or("entity key unmapped")?;
        select.push(format!(
            "CAST(p.{} AS {}) AS {}",
            quote(&k.name),
            spark_type(k.ty),
            quote(phys)
        ));
    }
    let mut joins = String::new();
    let mut ddl_cols: Vec<String> = fs
        .population
        .keys
        .iter()
        .map(|k| {
            format!(
                "{} {} NOT NULL",
                quote(target.entity_key_mapping[&k.name].as_str().unwrap()),
                spark_type(k.ty)
            )
        })
        .collect();
    for (i, m) in fs.members.iter().enumerate() {
        let (fid, _) = m.split_once('@').ok_or("bad member")?;
        let fm = target
            .feature_mapping
            .iter()
            .find(|x| x.feature == fid)
            .ok_or("member unmapped")?;
        let fsm = features
            .iter()
            .find(|x| x.feature == fid)
            .ok_or("member not lowered")?;
        let fpl = fs
            .features
            .iter()
            .find(|x| x.id == fid)
            .ok_or("member not planned")?;
        let on = key_names
            .iter()
            .map(|k| format!("p.{q} <=> m{i}.{q}", q = quote(k)))
            .collect::<Vec<_>>()
            .join(" AND ");
        joins.push_str(&format!("\nJOIN {} m{i} ON {on}", fsm.view));
        select.push(format!(
            "CAST(m{i}.fl_value AS {}) AS {}",
            spark_type(fpl.output.ty),
            quote(&fm.column)
        ));
        select.push(format!("m{i}.fl_state AS {}", quote(&fm.state_column)));
        ddl_cols.push(format!(
            "{} {}",
            quote(&fm.column),
            spark_type(fpl.output.ty)
        ));
        ddl_cols.push(format!("{} STRING NOT NULL", quote(&fm.state_column)));
    }
    let mc = &target.metadata_columns;
    select.push(format!("CAST(:as_of AS TIMESTAMP) AS {}", quote(&mc.as_of)));
    select.push(format!(
        "CAST(NULL AS TIMESTAMP) AS {}",
        quote(&mc.published_at)
    ));
    select.push(format!(
        "CAST(:publication_sequence AS BIGINT) AS {}",
        quote(&mc.publication_sequence)
    ));
    select.push(format!(
        "CAST(:publication_id AS STRING) AS {}",
        quote(&mc.publication_id)
    ));
    select.push(format!(
        "CAST(:materialization_id AS STRING) AS {}",
        quote(&mc.materialization_id)
    ));
    ddl_cols.push(format!("{} TIMESTAMP NOT NULL", quote(&mc.as_of)));
    ddl_cols.push(format!("{} TIMESTAMP", quote(&mc.published_at)));
    ddl_cols.push(format!(
        "{} BIGINT NOT NULL",
        quote(&mc.publication_sequence)
    ));
    ddl_cols.push(format!("{} STRING NOT NULL", quote(&mc.publication_id)));
    ddl_cols.push(format!("{} STRING NOT NULL", quote(&mc.materialization_id)));
    let snapshot_view = format!("fl_snapshot_{}", safe(&fs.id));
    let snapshot = Statement {
        name: snapshot_view.clone(),
        purpose: "Complete feature-set snapshot: every population key exactly once with every member value and state; published_at stays NULL until commit evidence is recorded".into(),
        sql: format!("CREATE OR REPLACE TEMP VIEW {snapshot_view} AS\nSELECT\n  {}\nFROM {population} p{joins}", select.join(",\n  ")), reads: vec![], view: None };
    let phys_keys: Vec<String> = fs
        .population
        .keys
        .iter()
        .map(|k| quote(target.entity_key_mapping[&k.name].as_str().unwrap()))
        .collect();
    let pk_cols = format!(
        "{}, {} TIMESERIES, {}",
        phys_keys.join(", "),
        quote(&mc.as_of),
        quote(&mc.publication_sequence)
    );
    let create_table = Statement {
        name: "create_feature_table".into(),
        purpose: "Create the Unity Catalog feature table (resource creation; run only with explicit authorization)".into(),
        sql: format!(
            "CREATE TABLE IF NOT EXISTS {output_table} (\n  {},\n  CONSTRAINT {} PRIMARY KEY ({pk_cols})\n) USING DELTA\nTBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'featlas.feature_set' = {}, 'featlas.feature_set_fingerprint' = {})",
            ddl_cols.join(",\n  "),
            quote(&format!("{}_pk", safe(&fs.id))),
            sql_string(&format!("{}@{}", fs.id, fs.version)),
            sql_string(&fs.fingerprint)
        ), reads: vec![], view: None };
    let merge_on = phys_keys
        .iter()
        .chain([quote(&mc.as_of), quote(&mc.publication_id)].iter())
        .map(|c| format!("t.{c} = s.{c}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let publish = Statement {
        name: "publish_snapshot".into(),
        purpose: "Single Delta MERGE commit making the complete snapshot visible atomically; idempotent on (keys, as_of, publication_id) for retries".into(),
        sql: format!("MERGE INTO {output_table} t\nUSING {snapshot_view} s\nON {merge_on}\nWHEN NOT MATCHED THEN INSERT *"), reads: vec![], view: None };
    let table_properties = Statement {
        name: "table_properties".into(),
        purpose: "Before publishing, the existing table must carry this feature set's identity and fingerprint".into(),
        sql: format!("SHOW TBLPROPERTIES {output_table}"),
        reads: vec![],
        view: None,
    };
    let expected_table_properties = BTreeMap::from([
        (
            "featlas.feature_set".to_string(),
            format!("{}@{}", fs.id, fs.version),
        ),
        (
            "featlas.feature_set_fingerprint".to_string(),
            fs.fingerprint.clone(),
        ),
    ]);
    let evidence = vec![
        Statement {
            name: "commit_history".into(),
            purpose: "Read, through the table's change data feed over the bounded version range [:from_version, :to_version], the commit that inserted this publication's rows; its timestamp is the observed visibility instant. The range is a retained commit (see commit_candidates), so the read never needs expired history. Independent of session confs such as commit userMetadata.".into(),
            sql: format!(
                "SELECT _commit_version AS version, _commit_timestamp AS timestamp, count(*) AS rows FROM table_changes({}, :from_version, :to_version) WHERE _change_type = 'insert' AND {} = :publication_id GROUP BY _commit_version, _commit_timestamp",
                sql_string(&output_table),
                quote(&mc.publication_id)
            ),
            reads: vec![],
            view: None,
        },
        Statement {
            name: "record_published_at".into(),
            purpose: "Record observed visibility evidence on the rows of this publication only (never fabricated from schedules or job time)".into(),
            sql: format!("UPDATE {output_table} SET {} = CAST(:observed_commit_timestamp AS TIMESTAMP) WHERE {} = :publication_id AND {} IS NULL", quote(&mc.published_at), quote(&mc.publication_id), quote(&mc.published_at)), reads: vec![], view: None },
        Statement {
            name: "commit_candidates".into(),
            purpose: "The table's retained commit history; the host reads commit_history one retained MERGE commit at a time (newest first, after the pre-publication version on a fresh write), so evidence lookup never depends on history that log or change-data retention has removed".into(),
            sql: format!("DESCRIBE HISTORY {output_table}"),
            reads: vec![],
            view: None,
        },
    ];
    let consumer_read = Statement {
        name: "consumer_read".into(),
        purpose: "Physical read for the target-neutral consumer API; the core selects values, applies freshness, null and replay rules".into(),
        sql: format!("SELECT * FROM {output_table} WHERE {} <= CAST(:max_as_of AS TIMESTAMP) AND ({k}) IN (SELECT {k} FROM fl_request_keys)", quote(&mc.as_of), k = phys_keys.join(", ")), reads: vec![], view: None };
    let session = vec![
        Statement {
            name: "time_zone".into(),
            purpose: "Interpret and render timestamps as UTC instants".into(),
            sql: "SET TIME ZONE 'UTC'".into(),
            reads: vec![],
            view: None,
        },
        Statement {
            name: "ansi".into(),
            purpose: "Checked BIGINT overflow and strict casts".into(),
            sql: "SET spark.sql.ansi.enabled = true".into(),
            reads: vec![],
            view: None,
        },
    ];
    let parameters = BTreeMap::from([
        ("as_of".to_string(), "TIMESTAMP (UTC instant, microsecond precision): the requested feature as_of T".to_string()),
        ("coverage_state".to_string(), "STRING per degraded feature: core-computed 'insufficient_history' or 'source_unavailable'".to_string()),
        ("publication_sequence".to_string(), "BIGINT: deterministic original publication order".to_string()),
        ("publication_id".to_string(), "STRING: stable original publication identity".to_string()),
        ("materialization_id".to_string(), "STRING: identity of this write; also the Delta commit userMetadata".to_string()),
        ("observed_commit_timestamp".to_string(), "TIMESTAMP: _commit_timestamp of the commit that inserted the publication (change data feed)".to_string()),
        ("max_as_of".to_string(), "TIMESTAMP: largest as_of among consumer request rows".to_string()),
    ]);
    let notes = vec![
        "Number sums/means use Spark binary64 accumulation; the local evaluator is correctly rounded. Declared numeric tolerances govern comparison; integer results are exact (BIGINT, ANSI overflow).".into(),
        "Integer/number comparisons follow Spark's widening to DOUBLE, which is inexact above 2^53.".into(),
        "String ordering assumes UTF8_BINARY collation (code-point order); preflight must confirm the table/session collation.".into(),
        "Declared primary keys, foreign keys and cardinalities are verified by the generated assertion queries; Unity Catalog constraints are informational.".into(),
        "Coverage/outage states are computed by the core from source-status evidence; this SQL never infers completeness from row counts.".into(),
    ];
    let Lowering {
        mut stages, checks, ..
    } = lw;
    for st in stages.iter_mut() {
        as_view(st);
    }
    for f in features.iter_mut() {
        as_view(&mut f.computed);
        as_view(&mut f.degraded);
    }
    let mut bind_sources = bind_sources;
    for st in bind_sources.iter_mut() {
        as_view(st);
    }
    let mut snapshot = snapshot;
    as_view(&mut snapshot);
    Ok(Artifacts {
        generator: format!("{IMPLEMENTATION}@{VERSION}"),
        plan_digest: plan.plan_digest.clone(),
        logical_plan_digest: plan.logical_plan_digest.clone(),
        feature_set: fs.id.clone(),
        feature_set_version: fs.version.clone(),
        session,
        bind_sources,
        stages,
        checks,
        features,
        snapshot,
        create_table,
        publish,
        table_properties,
        expected_table_properties,
        evidence,
        consumer_read,
        parameters,
        expected_source_schemas: expected_schemas,
        output_table,
        metadata_columns: target.metadata_columns.clone(),
        budget: plan.bindings.budget.clone(),
        retention: RetentionContract {
            sources: retention_sources,
            output_retention_seconds: p.deployment(&plan.bindings.deployment).map(|d| d.output_retention_seconds).unwrap_or(0),
        },
        notes,
    })
}

struct PlanEnv<'a> {
    p: &'a Project,
    f: &'a Feature,
}

impl featlas_core::expr::TypeEnv for PlanEnv<'_> {
    fn column(
        &self,
        input: &str,
        column: &str,
    ) -> Result<featlas_core::expr::Ty, (String, String)> {
        let src = self
            .f
            .input(input)
            .and_then(|i| self.p.source(&i.source))
            .ok_or(("UNKNOWN_INPUT".to_string(), input.to_string()))?;
        let c = src
            .column(column)
            .ok_or(("UNKNOWN_COLUMN".to_string(), column.to_string()))?;
        Ok(featlas_core::expr::Ty::of(c.ty, c.nullable))
    }
    fn feature(&self, feature: &str, _: &str) -> Result<featlas_core::expr::Ty, (String, String)> {
        let t = self
            .p
            .feature(feature)
            .ok_or(("UNKNOWN_FEATURE".to_string(), feature.to_string()))?;
        Ok(featlas_core::expr::Ty::of(t.output.ty, t.output.nullable))
    }
}

fn lower_feature(
    lw: &mut Lowering,
    f: &FeaturePlan,
    population: &str,
    keys: &[String],
    view: &str,
    p: &Project,
) -> Result<Statement, String> {
    let model_feature = p.feature(&f.id).ok_or("feature not in project")?;
    let env = PlanEnv {
        p,
        f: model_feature,
    };
    let ty_of =
        |e: &Expr| featlas_core::expr::check(e, &env, "", &mut Vec::new()).and_then(|t| t.ty);
    let qkeys = keys.iter().map(|k| quote(k)).collect::<Vec<_>>().join(", ");
    let pkeys = keys
        .iter()
        .map(|k| format!("p.{}", quote(k)))
        .collect::<Vec<_>>()
        .join(", ");
    let join_keys = |a: &str, b: &str| {
        keys.iter()
            .map(|k| format!("{a}.{q} <=> {b}.{q}", q = quote(k)))
            .collect::<Vec<_>>()
            .join(" AND ")
    };
    let rows_view = format!("{view}_rows");
    let mut alias_cols: BTreeMap<String, String> = BTreeMap::new(); // alias -> source
    let mut primary_identity: Vec<String> = Vec::new();
    let mut from = String::new();
    let mut window_where = String::new();
    let mut agg_input_where = String::new();
    for step in &f.steps {
        match step {
            Step::Reconstruct {
                alias,
                source,
                availability_upper,
            } => {
                let sel = lw.reconstruct(source, *availability_upper);
                alias_cols.insert(alias.clone(), source.clone());
                if from.is_empty() {
                    let s = lw.source(source);
                    primary_identity = s
                        .logical_key
                        .iter()
                        .map(|c| format!("{}.{}", quote(alias), quote(c)))
                        .collect();
                    from = format!("{sel} {}", quote(alias));
                }
            }
            Step::EventWindow {
                alias,
                column,
                lookback_seconds,
                lower,
                upper,
            } => {
                let c = format!("unix_micros({}.{})", quote(alias), quote(column));
                let start = format!(
                    "(unix_micros(:as_of) - {})",
                    (*lookback_seconds as i128) * 1_000_000
                );
                window_where = format!(
                    "{c} {} {start} AND {c} {} unix_micros(:as_of)",
                    boundary_op(*lower, ">=", ">"),
                    boundary_op(*upper, "<=", "<")
                );
            }
            Step::Join {
                relationship,
                left_alias,
                right_alias,
                key_pairs,
                cardinality,
                unmatched,
                null_keys,
                temporal,
                left_event_time,
                join_type,
                ..
            } => {
                let jc0 = lw.checks.len();
                let right_source = alias_cols
                    .get(right_alias)
                    .cloned()
                    .ok_or("join alias not reconstructed")?;
                let right_sel = lw.reconstruct(
                    &right_source,
                    f.steps
                        .iter()
                        .find_map(|s| {
                            if let Step::Reconstruct {
                                alias,
                                availability_upper,
                                ..
                            } = s
                            {
                                (alias == right_alias).then_some(*availability_upper)
                            } else {
                                None
                            }
                        })
                        .unwrap_or(Boundary::Inclusive),
                );
                let (la, ra) = (quote(left_alias), quote(right_alias));
                let mut on: Vec<String> = key_pairs
                    .iter()
                    .map(|(l, r)| format!("{la}.{} = {ra}.{}", quote(l), quote(r)))
                    .collect();
                if let RelationshipTemporal::AsOf {
                    valid_from,
                    valid_to,
                    lower,
                    upper,
                    ..
                } = temporal
                {
                    let ev = format!("{la}.{}", quote(left_event_time));
                    on.push(format!(
                        "{ev} {} {ra}.{}",
                        boundary_op(*lower, ">=", ">"),
                        quote(valid_from)
                    ));
                    on.push(format!(
                        "({ra}.{vt} IS NULL OR {ev} {} {ra}.{vt})",
                        boundary_op(*upper, "<=", "<"),
                        vt = quote(valid_to)
                    ));
                    lw.checks.push(Check {
                        name: format!("{view}_{}_invalid_intervals", safe(relationship)),
                        code: "INVALID_INTERVAL".into(),
                        rule: "SEM-REV-003".into(),
                        sql: format!("SELECT * FROM {right_sel} WHERE {vt} IS NOT NULL AND {vt} <= {} LIMIT 1", quote(valid_from), vt = quote(valid_to)), reads: vec![], feature: None });
                }
                let kind = if *join_type == JoinType::Inner || *unmatched == Unmatched::Drop {
                    "JOIN"
                } else {
                    "LEFT JOIN"
                };
                let base_from = from.clone();
                from = format!("{from}\n{kind} {right_sel} {ra} ON {}", on.join(" AND "));
                let ident = primary_identity.join(", ");
                let base_where = if window_where.is_empty() {
                    String::new()
                } else {
                    format!(" WHERE {window_where}")
                };
                lw.checks.push(Check {
                    name: format!("{view}_{}_multiple_matches", safe(relationship)),
                    code: "MULTIPLE_MATCHES".into(),
                    rule: "SEM-REL-002".into(),
                    sql: format!("SELECT {ident}, count(*) AS n FROM {base_from}\nJOIN {right_sel} {ra} ON {}{base_where} GROUP BY {ident} HAVING count(*) > 1", on.join(" AND ")), reads: vec![], feature: None });
                let left_null = key_pairs
                    .iter()
                    .map(|(l, _)| format!("{la}.{} IS NULL", quote(l)))
                    .collect::<Vec<_>>()
                    .join(" OR ");
                if *null_keys == NullKeys::Error {
                    lw.checks.push(Check {
                        name: format!("{view}_{}_null_keys", safe(relationship)),
                        code: "NULL_JOIN_KEY".into(),
                        rule: "SEM-REL-003".into(),
                        sql: format!(
                            "SELECT {ident} FROM {base_from} WHERE ({left_null}){} LIMIT 1",
                            if window_where.is_empty() {
                                String::new()
                            } else {
                                format!(" AND {window_where}")
                            }
                        ),
                        reads: vec![],
                        feature: None,
                    });
                }
                if *unmatched == Unmatched::Error {
                    let r0 = format!("{ra}.{}", quote(&key_pairs[0].1));
                    lw.checks.push(Check { name: format!("{view}_{}_unmatched", safe(relationship)), code: "UNMATCHED_JOIN".into(), rule: "SEM-REL-003".into(), sql: format!("SELECT {ident} FROM {base_from}\nLEFT JOIN {right_sel} {ra} ON {}{} AND {r0} IS NULL LIMIT 1", on.join(" AND "), if base_where.is_empty() { " WHERE true".to_string() } else { base_where.clone() }), reads: vec![], feature: None });
                }
                if *cardinality == Cardinality::OneToOne {
                    let lk = key_pairs
                        .iter()
                        .map(|(l, _)| format!("{la}.{}", quote(l)))
                        .collect::<Vec<_>>()
                        .join(", ");
                    lw.checks.push(Check { name: format!("{view}_{}_one_to_one", safe(relationship)), code: "CARDINALITY_VIOLATION".into(), rule: "SEM-REL-002".into(), sql: format!("SELECT {lk}, count(*) AS n FROM {base_from}{base_where} GROUP BY {lk} HAVING count(*) > 1 AND NOT ({})", left_null.replace(&format!("{la}."), "")), reads: vec![], feature: None });
                }
                // Canonical precedence, identical to the core evaluator: reconstruction, invalid
                // intervals, null join keys, cardinality, multiple matches, unmatched rows.
                let rank = |c: &Check| match c.code.as_str() {
                    "INVALID_INTERVAL" => 1,
                    "NULL_JOIN_KEY" => 2,
                    "CARDINALITY_VIOLATION" => 3,
                    "MULTIPLE_MATCHES" => 4,
                    "UNMATCHED_JOIN" => 5,
                    _ => 0,
                };
                lw.checks[jc0..].sort_by_key(rank);
            }
            Step::Filter { predicate } => {
                let c = |a: &str, col: &str| format!("{}.{}", quote(a), quote(col));
                agg_input_where =
                    format!("coalesce({}, false)", lower_expr(predicate, &c, &ty_of, 0)?);
            }
            _ => {}
        }
    }
    let col = |a: &str, c: &str| format!("{}.{}", quote(a), quote(c));
    let mut wheres: Vec<String> = Vec::new();
    if !window_where.is_empty() {
        wheres.push(window_where.clone());
    }
    if !agg_input_where.is_empty() {
        wheres.push(agg_input_where.clone());
    }
    let where_sql = if wheres.is_empty() {
        String::new()
    } else {
        format!("\nWHERE {}", wheres.join("\n  AND "))
    };
    let group_select = |gkeys: &[GroupKey]| -> String {
        keys.iter()
            .map(|k| {
                let g = gkeys.iter().find(|g| &g.entity_key == k).unwrap();
                format!("{}.{} AS {}", quote(&g.input), quote(&g.column), quote(k))
            })
            .collect::<Vec<_>>()
            .join(", ")
    };
    let group_not_null = |gkeys: &[GroupKey]| -> String {
        gkeys
            .iter()
            .map(|g| format!("{}.{} IS NOT NULL", quote(&g.input), quote(&g.column)))
            .collect::<Vec<_>>()
            .join(" AND ")
    };
    let (empty_value, empty_state) = empty_sql(f);
    let out_ty = spark_type(f.output.ty);
    let statement = |sql: String, purpose: String| Statement {
        name: view.to_string(),
        purpose,
        sql,
        reads: vec![],
        view: None,
    };
    let gkeys: Vec<GroupKey> = f
        .steps
        .iter()
        .find_map(|s| {
            if let Step::Group { keys } = s {
                Some(keys.clone())
            } else {
                None
            }
        })
        .unwrap_or_default();
    let value_cols = |e: &Expr| lower_expr(e, &col, &ty_of, 0);
    // Reduce-stage assertions apply only to population entities, as the core reduces only those.
    let pop_filter = |gkeys: &[GroupKey]| -> String {
        let cols = keys
            .iter()
            .map(|k| {
                let g = gkeys.iter().find(|g| &g.entity_key == k).unwrap();
                format!("{}.{}", quote(&g.input), quote(&g.column))
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            " AND ({cols}) IN (SELECT {} FROM {population})",
            keys.iter().map(|k| quote(k)).collect::<Vec<_>>().join(", ")
        )
    };
    for step in &f.steps {
        match step {
            Step::Aggregate {
                operation,
                value,
                distinct_by,
                nulls,
            } => {
                let gsel = group_select(&gkeys);
                let gnn = group_not_null(&gkeys);
                let wh = if where_sql.is_empty() {
                    format!("\nWHERE {gnn}")
                } else {
                    format!("{where_sql}\n  AND {gnn}")
                };
                let mut extra = String::new();
                let agg = match operation {
                    AggregateOp::Count => "count(*)".to_string(),
                    AggregateOp::CountDistinct => {
                        let parts: Vec<String> = distinct_by
                            .iter()
                            .map(value_cols)
                            .collect::<Result<_, _>>()?;
                        let any_null = parts
                            .iter()
                            .map(|x| format!("({x}) IS NULL"))
                            .collect::<Vec<_>>()
                            .join(" OR ");
                        if *nulls == NullPolicy::Error {
                            lw.checks.push(Check {
                                name: format!("{view}_null_input"),
                                code: "NULL_INPUT".into(),
                                rule: "SEM-TRANS-004".into(),
                                sql: format!(
                                    "SELECT * FROM {from}{wh} AND ({any_null}){} LIMIT 1",
                                    pop_filter(&gkeys)
                                ),
                                reads: vec![],
                                feature: None,
                            });
                        }
                        extra =
                            format!(", max(CASE WHEN {any_null} THEN 1 ELSE 0 END) AS fl_has_null");
                        format!("count(DISTINCT {})", parts.join(", "))
                    }
                    op => {
                        let v = value_cols(value.as_ref().ok_or("aggregate value missing")?)?;
                        if *nulls == NullPolicy::Error {
                            lw.checks.push(Check {
                                name: format!("{view}_null_input"),
                                code: "NULL_INPUT".into(),
                                rule: "SEM-TRANS-004".into(),
                                sql: format!(
                                    "SELECT * FROM {from}{wh} AND ({v}) IS NULL{} LIMIT 1",
                                    pop_filter(&gkeys)
                                ),
                                reads: vec![],
                                feature: None,
                            });
                        }
                        extra = format!(
                            ", max(CASE WHEN ({v}) IS NULL THEN 1 ELSE 0 END) AS fl_has_null"
                        );
                        let func = match op {
                            AggregateOp::Sum => "sum",
                            AggregateOp::Mean => "avg",
                            AggregateOp::Min => "min",
                            _ => "max",
                        };
                        let finite_agg = |x: String| {
                            format!("(CASE WHEN ({x}) IS NULL THEN NULL WHEN isnan({x}) OR abs({x}) = double('Infinity') THEN {} ELSE ({x}) END)", raise("NON_FINITE_RESULT", "non-finite aggregate"))
                        };
                        let vty = ty_of(value.as_ref().unwrap());
                        match op {
                            AggregateOp::Mean if vty == Some(LogicalType::Integer) => {
                                finite_agg(format!(
                                    "CAST(sum(CAST({v} AS DECIMAL(38,0))) AS DOUBLE) / count({v})"
                                ))
                            }
                            AggregateOp::Mean => finite_agg(format!("avg(CAST({v} AS DOUBLE))")),
                            // Exact integer sum: DECIMAL(38,0) accumulation, then an ANSI-checked BIGINT cast.
                            AggregateOp::Sum if vty == Some(LogicalType::Integer) => {
                                format!("CAST(sum(CAST({v} AS DECIMAL(38,0))) AS BIGINT)")
                            }
                            AggregateOp::Sum => finite_agg(format!("sum({v})")),
                            _ => format!("{func}({v})"),
                        }
                    }
                };
                let value_expr =
                    if *nulls == NullPolicy::Propagate && *operation != AggregateOp::Count {
                        "CASE WHEN a.fl_has_null = 1 THEN NULL ELSE a.fl_agg END".to_string()
                    } else {
                        "a.fl_agg".to_string()
                    };
                let sql = format!(
                    "CREATE OR REPLACE TEMP VIEW {view} AS\nWITH fl_agg_rows AS (\n  SELECT {gsel}, {agg} AS fl_agg{extra}\n  FROM {from}{wh}\n  GROUP BY {}\n)\nSELECT {pkeys},\n  CASE WHEN a.fl_present IS NULL THEN {empty_value} ELSE CAST({value_expr} AS {out_ty}) END AS fl_value,\n  CASE WHEN a.fl_present IS NULL THEN {empty_state} ELSE 'ok' END AS fl_state\nFROM {population} p\nLEFT JOIN (SELECT *, 1 AS fl_present FROM fl_agg_rows) a ON {}",
                    gkeys_in_entity_order(&gkeys, keys),
                    join_keys("p", "a")
                );
                let _ = &rows_view;
                return Ok(statement(sql, format!("{}: exact {:?} per complete entity key; population left join keeps zero-event entities", f.id, operation)));
            }
            Step::Recency { alias, column, .. } => {
                let gsel = group_select(&gkeys);
                let gnn = group_not_null(&gkeys);
                let wh = if where_sql.is_empty() {
                    format!("\nWHERE {gnn}")
                } else {
                    format!("{where_sql}\n  AND {gnn}")
                };
                let sql = format!(
                    "CREATE OR REPLACE TEMP VIEW {view} AS\nWITH fl_latest AS (\n  SELECT {gsel}, max({}.{}) AS fl_latest\n  FROM {from}{wh}\n  GROUP BY {}\n)\nSELECT {pkeys},\n  CASE WHEN a.fl_latest IS NULL THEN {empty_value} ELSE CAST(div(unix_micros(:as_of) - unix_micros(a.fl_latest), 1000000) AS {out_ty}) END AS fl_value,\n  CASE WHEN a.fl_latest IS NULL THEN {empty_state} ELSE 'ok' END AS fl_state\nFROM {population} p\nLEFT JOIN fl_latest a ON {}",
                    quote(alias),
                    quote(column),
                    gkeys_in_entity_order(&gkeys, keys),
                    join_keys("p", "a")
                );
                return Ok(statement(
                    sql,
                    format!(
                        "{}: floor elapsed seconds since the latest eligible event",
                        f.id
                    ),
                ));
            }
            Step::Lookup { value, order_by } => {
                let gsel = group_select(&gkeys);
                let gnn = group_not_null(&gkeys);
                let wh = if where_sql.is_empty() {
                    format!("\nWHERE {gnn}")
                } else {
                    format!("{where_sql}\n  AND {gnn}")
                };
                let order = order_by
                    .iter()
                    .map(|o| {
                        format!(
                            "{}.{} {}",
                            quote(&o.input),
                            quote(&o.column),
                            if o.direction == "desc" { "DESC" } else { "ASC" }
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                let null_order = order_by
                    .iter()
                    .map(|o| format!("{}.{} IS NULL", quote(&o.input), quote(&o.column)))
                    .collect::<Vec<_>>()
                    .join(" OR ");
                lw.checks.push(Check {
                    name: format!("{view}_null_order_key"),
                    code: "NULL_ORDER_KEY".into(),
                    rule: "SEM-TRANS-005".into(),
                    sql: format!(
                        "SELECT * FROM {from}{wh} AND ({null_order}){} LIMIT 1",
                        pop_filter(&gkeys)
                    ),
                    reads: vec![],
                    feature: None,
                });
                let part_q: Vec<String> = gkeys
                    .iter()
                    .map(|g| format!("{}.{}", quote(&g.input), quote(&g.column)))
                    .collect();
                let v = value_cols(value)?;
                lw.checks.push(Check {
                    name: format!("{view}_ties"),
                    code: "LOOKUP_TIE".into(),
                    rule: "SEM-TRANS-005".into(),
                    sql: format!("SELECT fl_k, count(*) AS n FROM (SELECT to_json(named_struct({})) AS fl_k, RANK() OVER (PARTITION BY {} ORDER BY {order}) AS fl_rank FROM {from}{wh}{}) WHERE fl_rank = 1 GROUP BY fl_k HAVING count(*) > 1", gkeys.iter().map(|g| format!("{}, {}.{}", sql_string(&g.entity_key), quote(&g.input), quote(&g.column))).collect::<Vec<_>>().join(", "), part_q.join(", "), pop_filter(&gkeys)), reads: vec![], feature: None });
                let sql = format!(
                    "CREATE OR REPLACE TEMP VIEW {view} AS\nWITH fl_first AS (\n  SELECT * FROM (\n    SELECT {gsel}, {v} AS fl_v, ROW_NUMBER() OVER (PARTITION BY {} ORDER BY {order}) AS fl_rn\n    FROM {from}{wh}\n  ) WHERE fl_rn = 1\n)\nSELECT {pkeys},\n  CASE WHEN a.fl_rn IS NULL THEN {empty_value} ELSE CAST(a.fl_v AS {out_ty}) END AS fl_value,\n  CASE WHEN a.fl_rn IS NULL THEN {empty_state} ELSE 'ok' END AS fl_state\nFROM {population} p\nLEFT JOIN fl_first a ON {}",
                    part_q.join(", "),
                    join_keys("p", "a")
                );
                return Ok(statement(sql, format!("{}: first row by declared ordering; ties are asserted separately; a selected null stays null", f.id)));
            }
            Step::Expression {
                expression,
                dependencies,
            } => {
                let mut joins = String::new();
                let mut dep_alias = BTreeMap::new();
                for (i, d) in dependencies.iter().enumerate() {
                    let (fid, _) = d.split_once('@').ok_or("bad dependency")?;
                    let dv = format!("fl_feature_{}", safe(fid));
                    joins.push_str(&format!(
                        "\nJOIN {dv} d{i} ON {}",
                        join_keys("p", &format!("d{i}"))
                    ));
                    dep_alias.insert(fid.to_string(), format!("d{i}"));
                }
                let dcol = |_: &str, fid: &str| {
                    format!(
                        "{}.fl_value",
                        dep_alias
                            .get(fid)
                            .cloned()
                            .unwrap_or_else(|| "missing".into())
                    )
                };
                let ex = lower_expr(expression, &dcol, &ty_of, 0)?;
                let unavailable = dep_alias
                    .values()
                    .map(|a| format!("{a}.fl_state = 'source_unavailable'"))
                    .collect::<Vec<_>>()
                    .join(" OR ");
                let insufficient = dep_alias
                    .values()
                    .map(|a| format!("{a}.fl_state = 'insufficient_history'"))
                    .collect::<Vec<_>>()
                    .join(" OR ");
                let su = if f.missingness.source_unavailable == NullOrError::Error {
                    raise("SOURCE_UNAVAILABLE", "dependency source unavailable")
                } else {
                    format!("CAST(NULL AS {out_ty})")
                };
                let ih = if f.missingness.insufficient_history == NullOrError::Error {
                    raise("INSUFFICIENT_HISTORY", "dependency history insufficient")
                } else {
                    format!("CAST(NULL AS {out_ty})")
                };
                let sql = format!(
                    "CREATE OR REPLACE TEMP VIEW {view} AS\nSELECT {pkeys},\n  CASE WHEN {unavailable} THEN {su} WHEN {insufficient} THEN {ih} ELSE CAST({ex} AS {out_ty}) END AS fl_value,\n  CASE WHEN {unavailable} THEN 'source_unavailable' WHEN {insufficient} THEN 'insufficient_history' ELSE 'ok' END AS fl_state\nFROM {population} p{joins}"
                );
                let _ = p;
                return Ok(statement(sql, format!("{}: typed expression over same-entity as-of dependencies; dependency outage/history states propagate", f.id)));
            }
            Step::RequiresCapability {
                kind, capabilities, ..
            } => {
                return Err(format!("{} is a {kind} transform requiring {capabilities:?}; {IMPLEMENTATION}@{VERSION} does not execute it", f.id));
            }
            _ => {}
        }
    }
    let _ = qkeys;
    Err(format!("{} has no lowerable transform", f.id))
}

/// Split `CREATE OR REPLACE TEMP VIEW name AS\n<select>` into a view name and its SELECT body.
fn as_view(st: &mut Statement) {
    if let Some(rest) = st.sql.strip_prefix("CREATE OR REPLACE TEMP VIEW ") {
        if let Some((name, body)) = rest.split_once(" AS\n") {
            st.view = Some(name.to_string());
            st.sql = body.to_string();
        }
    }
}

fn render_statement(x: &Statement) -> String {
    match &x.view {
        Some(v) => format!("CREATE OR REPLACE TEMP VIEW {v} AS\n{}", x.sql),
        None => x.sql.clone(),
    }
}

fn gkeys_in_entity_order(gkeys: &[GroupKey], keys: &[String]) -> String {
    keys.iter()
        .map(|k| {
            let g = gkeys.iter().find(|g| &g.entity_key == k).unwrap();
            format!("{}.{}", quote(&g.input), quote(&g.column))
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Render artifacts as an ordered, reviewable SQL script (for inspection).
pub fn render_script(a: &Artifacts) -> String {
    let mut s = format!("-- Generated by {} from plan {}\n-- Logical plan {}; feature set {}@{}\n-- Review artifact: the host executes statements individually with named parameters.\n\n", a.generator, a.plan_digest, a.logical_plan_digest, a.feature_set, a.feature_set_version);
    let mut section = |title: &str, items: Vec<(&str, &str, String)>| {
        s.push_str(&format!("-- ==== {title} ====\n"));
        for (name, purpose, sql) in items {
            s.push_str(&format!("-- [{name}] {purpose}\n{sql};\n\n"));
        }
    };
    let r = |x: &Statement| render_statement(x);
    section(
        "session",
        a.session
            .iter()
            .map(|x| (x.name.as_str(), x.purpose.as_str(), r(x)))
            .collect(),
    );
    section(
        "bind sources (read-only)",
        a.bind_sources
            .iter()
            .map(|x| (x.name.as_str(), x.purpose.as_str(), r(x)))
            .collect(),
    );
    section(
        "reconstruction and population",
        a.stages
            .iter()
            .map(|x| (x.name.as_str(), x.purpose.as_str(), r(x)))
            .collect(),
    );
    section(
        "features (computed variant)",
        a.features
            .iter()
            .map(|x| {
                (
                    x.computed.name.as_str(),
                    x.computed.purpose.as_str(),
                    r(&x.computed),
                )
            })
            .collect(),
    );
    section(
        "features (degraded variant, used when the core reports coverage not established)",
        a.features
            .iter()
            .map(|x| {
                (
                    x.degraded.name.as_str(),
                    x.degraded.purpose.as_str(),
                    r(&x.degraded),
                )
            })
            .collect(),
    );
    section(
        "zero-row assertions (any row is a contract violation)",
        a.checks
            .iter()
            .map(|x| (x.name.as_str(), x.code.as_str(), x.sql.clone()))
            .collect(),
    );
    section(
        "snapshot",
        vec![(
            a.snapshot.name.as_str(),
            a.snapshot.purpose.as_str(),
            r(&a.snapshot),
        )],
    );
    section(
        "publication (requires authorization)",
        vec![
            (
                a.create_table.name.as_str(),
                a.create_table.purpose.as_str(),
                r(&a.create_table),
            ),
            (
                a.publish.name.as_str(),
                a.publish.purpose.as_str(),
                r(&a.publish),
            ),
        ],
    );
    section(
        "evidence",
        a.evidence
            .iter()
            .map(|x| (x.name.as_str(), x.purpose.as_str(), r(x)))
            .collect(),
    );
    section(
        "consumer read",
        vec![(
            a.consumer_read.name.as_str(),
            a.consumer_read.purpose.as_str(),
            r(&a.consumer_read),
        )],
    );
    s
}

/// Spark SQL for one label consumer (SEM-LABEL-001/002).
///
/// The core decides maturity and completeness per request (`featlas_core::label::label_requests`)
/// without reading data; the host registers the eligible requests as `fl_label_requests`
/// (`fl_request_index`, entity keys, `fl_as_of`, `fl_dataset_cutoff`, `fl_horizon_end`) and runs,
/// once per distinct dataset cutoff bound to `:as_of`, the label source reconstruction
/// (availability <= cutoff, duplicates, revisions, tombstones) and the `values` query. Row-level
/// failures are returned as data (`fl_error`) so the host reports them in request order, as the
/// core does.
#[derive(Debug, Clone, Serialize)]
pub struct LabelArtifacts {
    pub consumer: String,
    pub consumer_version: String,
    pub label_source: String,
    pub requests_view: String,
    pub request_columns: Vec<(String, String)>,
    pub session: Vec<Statement>,
    pub bind_sources: Vec<Statement>,
    pub stages: Vec<Statement>,
    pub checks: Vec<Check>,
    pub values: Statement,
    pub value_type: String,
    pub expected_source_schemas: BTreeMap<String, Value>,
    pub notes: Vec<String>,
}

pub fn lower_labels(plan: &Plan, p: &Project, consumer: &str) -> Result<LabelArtifacts, String> {
    let c = p.consumer(consumer).ok_or_else(|| format!("Unknown consumer {consumer}"))?;
    let label = c.label.as_ref().ok_or_else(|| format!("{consumer} declares no label"))?;
    let fs = plan
        .logical
        .feature_sets
        .iter()
        .find(|f| f.id == c.feature_set.feature_set)
        .ok_or_else(|| format!("Feature set {} is not in the plan", c.feature_set.feature_set))?;
    if !plan.logical.sources.contains_key(&label.source) {
        return Err(format!("Label source {} is not bound by this deployment", label.source));
    }
    let base = lower(plan, &fs.id, p)?;
    let session = base.session.clone();
    let bind_sources: Vec<Statement> = base
        .bind_sources
        .into_iter()
        .filter(|b| b.name == format!("fl_raw_{}", safe(&label.source)))
        .collect();
    let expected_source_schemas = base
        .expected_source_schemas
        .into_iter()
        .filter(|(k, _)| k == &label.source)
        .collect();
    let mut lw = Lowering { plan, fs, stages: Vec::new(), checks: Vec::new(), reconstructed: BTreeSet::new() };
    let selected = lw.reconstruct(&label.source, Boundary::Inclusive);
    let src = lw.source(&label.source);
    let col_ty = |name: &str| src.columns.iter().find(|c| c.name == name).map(|c| c.ty);
    let vty = col_ty(&label.value_column).ok_or_else(|| format!("Unknown label column {}", label.value_column))?;
    let out_ty = match label.aggregation {
        LabelAggregation::Count => LogicalType::Integer,
        LabelAggregation::AnyTrue => LogicalType::Boolean,
        _ => vty,
    };
    let entity = p.entity(&fs.population.entity).ok_or("Unknown entity")?;
    let mut request_columns = vec![("fl_request_index".to_string(), "BIGINT".to_string())];
    let mut on = Vec::new();
    for k in &entity.keys {
        let m = label.entity_keys.iter().find(|m| m.entity_key == k.name).ok_or_else(|| format!("Label maps no column to entity key {}", k.name))?;
        request_columns.push((k.name.clone(), spark_type(k.ty).to_string()));
        on.push(format!("s.{} = r.{}", quote(&m.column), quote(&k.name)));
    }
    for t in ["fl_as_of", "fl_dataset_cutoff", "fl_horizon_end"] {
        request_columns.push((t.into(), "TIMESTAMP".into()));
    }
    let et = quote(&label.event_time_column);
    let at = quote(&label.available_time_column);
    let lower_cmp = boundary_op(label.lower, ">=", ">");
    let upper_cmp = boundary_op(label.upper, "<=", "<");
    let v = "e.fl_v";
    let n = "count(e.fl_present)";
    let nulls = format!("{n} > count({v})");
    let (empty_value, empty_error) = match &label.empty {
        EmptyPolicy::Error => ("NULL".to_string(), "'LABEL_EMPTY'"),
        EmptyPolicy::Null => ("NULL".to_string(), "NULL"),
        EmptyPolicy::Value { value } => (literal_json(value, out_ty), "NULL"),
    };
    let (value, error) = match label.aggregation {
        LabelAggregation::Count => (n.to_string(), "NULL".to_string()),
        LabelAggregation::AnyTrue => (
            format!("CASE WHEN max(CASE WHEN {v} THEN 1 ELSE 0 END) = 1 THEN true WHEN {nulls} THEN NULL ELSE false END"),
            "NULL".into(),
        ),
        LabelAggregation::Max => (format!("CASE WHEN {nulls} THEN NULL ELSE max({v}) END"), "NULL".into()),
        LabelAggregation::Sum if vty == LogicalType::Integer => {
            // Exact: DECIMAL(38,0) accumulation; overflow is an error (as data), never a wrapped value.
            let dec = format!("sum(CAST({v} AS DECIMAL(38,0)))");
            let fits = format!("{dec} BETWEEN -9223372036854775808 AND 9223372036854775807");
            (
                format!("CASE WHEN {nulls} THEN NULL WHEN {fits} THEN CAST({dec} AS BIGINT) END"),
                format!("CASE WHEN {nulls} THEN NULL WHEN NOT ({fits}) THEN 'NUMERIC_OVERFLOW' END"),
            )
        }
        LabelAggregation::Sum => (
            format!("CASE WHEN {nulls} THEN NULL ELSE sum(CAST({v} AS DOUBLE)) END"),
            format!("CASE WHEN {nulls} THEN NULL WHEN isnan(sum(CAST({v} AS DOUBLE))) OR abs(sum(CAST({v} AS DOUBLE))) = double('Infinity') THEN 'NON_FINITE_RESULT' END"),
        ),
        LabelAggregation::Latest => (
            "max(CASE WHEN e.fl_rank = 1 THEN e.fl_v END)".into(),
            "CASE WHEN count(CASE WHEN e.fl_rank = 1 THEN 1 END) > 1 THEN 'LABEL_TIE' END".into(),
        ),
    };
    let rank = if label.aggregation == LabelAggregation::Latest {
        format!(", rank() OVER (PARTITION BY r.fl_request_index ORDER BY s.{et} DESC, s.{at} DESC) AS fl_rank")
    } else {
        String::new()
    };
    let sql = format!(
        "WITH req AS (SELECT * FROM fl_label_requests WHERE fl_dataset_cutoff = :as_of),\n\
         e AS (\n  SELECT r.fl_request_index AS fl_ri, s.{val} AS fl_v, 1 AS fl_present{rank}\n  FROM req r JOIN {selected} s ON {on}\n   AND s.{et} {lower_cmp} r.fl_as_of AND s.{et} {upper_cmp} r.fl_horizon_end\n)\n\
         SELECT r.fl_request_index AS request_index, {n} AS eligible_rows,\n\
           CASE WHEN {n} = 0 THEN 'empty' ELSE 'ok' END AS fl_state,\n\
           CAST(CASE WHEN {n} = 0 THEN {empty_value} ELSE {value} END AS {sty}) AS fl_value,\n\
           CASE WHEN {n} = 0 THEN {empty_error} ELSE {error} END AS fl_error\n\
         FROM req r LEFT JOIN e ON e.fl_ri = r.fl_request_index\n\
         GROUP BY r.fl_request_index ORDER BY r.fl_request_index",
        val = quote(&label.value_column),
        on = on.join(" AND "),
        sty = spark_type(out_ty),
    );
    // Spark forbids parameter markers in CREATE VIEW: register bound SELECTs instead.
    for st in lw.stages.iter_mut() {
        as_view(st);
    }
    Ok(LabelArtifacts {
        consumer: c.id.clone(),
        consumer_version: c.version.clone(),
        label_source: label.source.clone(),
        requests_view: "fl_label_requests".into(),
        request_columns,
        session,
        bind_sources,
        stages: lw.stages,
        checks: lw.checks,
        values: Statement {
            name: format!("fl_label_values_{}", safe(&c.id)),
            purpose: format!("{}: {:?} over [as_of, as_of + {}s) of {} as known at the dataset cutoff (:as_of)", c.id, label.aggregation, label.horizon_seconds, label.source),
            sql,
            reads: vec![label.source.clone()],
            view: None,
        },
        value_type: spark_type(out_ty).into(),
        expected_source_schemas,
        notes: vec![
            "Maturity, completeness and exclusion are decided by the core (label_requests); this SQL computes values for eligible requests only.".into(),
            "Feature filters are never applied to labels; the label source is reconstructed independently at each dataset cutoff.".into(),
        ],
    })
}

/// A Databricks Asset Bundle job (`resources.jobs`) running each feature set of a scheduled
/// deployment (SEM-DEPLOY-003, `core.scheduling`). JSON is valid bundle YAML.
///
/// * The trigger is a Quartz cron in UTC on the deployment's interval grid
///   (`every_seconds`, `offset_seconds`); intervals that are not whole minutes dividing an hour,
///   whole hours dividing a day, or exactly one day are refused.
/// * Each task derives `as_of` from the trigger time floored to that grid, and its publication
///   identity from `as_of` (`publication_id = <deployment>/<feature set>/<as_of>`, sequence =
///   epoch seconds), so a retried run republishes nothing (the host's retry path).
/// * Retries, `max_concurrent_runs` (budget) and `timeout_seconds` (SLO) are explicit.
/// * The job is created **paused**: deploying the bundle authorizes nothing; unpausing it is the
///   explicit publication approval for `environment`.
pub fn job_bundle(plan: &Plan, d: &Deployment, project_id: &str) -> Result<Value, String> {
    let Schedule::Interval { every_seconds, offset_seconds } = d.schedule else {
        return Err("Deployment schedule is manual; no job is generated".into());
    };
    if d.execution.orchestrator.is_none() {
        return Err("A scheduled deployment must select an orchestrator (SEM-PORT-002)".into());
    }
    let (e, o) = (every_seconds, offset_seconds.rem_euclid(every_seconds.max(1)));
    let cron = if e == 86400 {
        format!("{} {} {} * * ?", o % 60, (o / 60) % 60, o / 3600)
    } else if e % 3600 == 0 && 86400 % e == 0 {
        format!("{} {} {}/{} * * ?", o % 60, (o / 60) % 60, o / 3600, e / 3600)
    } else if e % 60 == 0 && 3600 % e == 0 {
        format!("{} {}/{} * * * ?", o % 60, o / 60, e / 60)
    } else {
        return Err(format!("every_seconds {e} is not expressible as a UTC Quartz schedule (whole minutes dividing an hour, whole hours dividing a day, or one day)"));
    };
    let job_key = format!("featlas_{}", safe(&d.id));
    let tasks: Vec<Value> = plan
        .logical
        .feature_sets
        .iter()
        .map(|fs| {
            json!({
                "task_key": safe(&fs.id),
                "environment_key": "featlas",
                "max_retries": 2,
                "min_retry_interval_millis": 60000,
                "retry_on_timeout": false,
                "python_wheel_task": {
                    "package_name": "featlas",
                    "entry_point": "featlas-databricks",
                    "parameters": [
                        "${var.project_path}", "--deployment", d.id, "--feature-set", fs.id,
                        "--as-of-from-trigger", "{{job.trigger.time.iso_datetime}}",
                        "--grid-seconds", e.to_string(), "--offset-seconds", o.to_string(),
                        "--status-table", "${var.source_status_table}",
                        "--publish", "--environment", d.environment,
                        "--approved-by", format!("databricks-job:{job_key}"),
                        "--materialization-id", "{{job.run_id}}-{{task.run_id}}",
                    ],
                },
            })
        })
        .collect();
    Ok(json!({
        "variables": {
            "project_path": {"description": "Workspace path of the exact project file this job runs (its sha256 must equal the plan's)."},
            "featlas_wheel": {"description": "Workspace/Volume path of the featlas wheel built from the verified candidate."},
            "source_status_table": {"description": "Three-part name of the source-status evidence table (source, state, coverage_start, coverage_end, observed_at)."},
        },
        "resources": {"jobs": {job_key.clone(): {
            "name": format!("featlas {} ({project_id})", d.id),
            "description": format!("Generated by {IMPLEMENTATION}@{VERSION} from plan {}. Paused on creation: unpausing is the publication approval for environment {}.", plan.plan_digest, d.environment),
            "max_concurrent_runs": d.budget.max_concurrent_runs,
            "timeout_seconds": d.slo.max_compute_seconds,
            "schedule": {"quartz_cron_expression": cron, "timezone_id": "UTC", "pause_status": "PAUSED"},
            "tags": {"featlas_deployment": d.id, "featlas_deployment_identity": plan.bindings.deployment_identity, "featlas_plan_digest": plan.plan_digest},
            "environments": [{"environment_key": "featlas", "spec": {"client": "2", "dependencies": ["${var.featlas_wheel}"]}}],
            "tasks": tasks,
        }}},
    }))
}
