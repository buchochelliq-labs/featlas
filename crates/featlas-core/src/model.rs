//! Typed canonical model mirroring `project.schema.json` (0.2.0).
//!
//! Deserialization happens only after schema validation. Every struct denies
//! unknown fields, so a disagreement between this model and the schema is
//! reported as `SCHEMA_MODEL_CONFLICT` rather than silently ignored. No field
//! has a serde default: omitted contract fields are never invented.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

pub type Extensions = Option<Map<String, Value>>;
pub type Row = Map<String, Value>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogicalType {
    Integer,
    Number,
    Boolean,
    String,
    Timestamp,
    Date,
}

impl LogicalType {
    pub fn as_str(self) -> &'static str {
        match self {
            LogicalType::Integer => "integer",
            LogicalType::Number => "number",
            LogicalType::Boolean => "boolean",
            LogicalType::String => "string",
            LogicalType::Timestamp => "timestamp",
            LogicalType::Date => "date",
        }
    }
    pub fn is_numeric(self) -> bool {
        matches!(self, LogicalType::Integer | LogicalType::Number)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Boundary {
    Inclusive,
    Exclusive,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeatureRef {
    pub feature: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamedFeatureRef {
    pub id: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeatureSetRef {
    pub feature_set: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodeProvenance {
    pub uri: String,
    pub revision: String,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Metadata {
    pub id: String,
    pub name: String,
    pub version: String,
    pub description: String,
    pub owners: Vec<String>,
    pub status: String,
    pub tags: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extensions: Extensions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: LogicalType,
    pub description: String,
    pub nullable: bool,
    pub unit: Option<String>,
    pub classification: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extensions: Extensions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RevisionOrder {
    pub column: String,
    pub direction: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Revision {
    pub order_by: Vec<RevisionOrder>,
    pub selection: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tombstone_column: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tombstone_value: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DuplicatePolicy {
    Reject,
    CollapseIdentical,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RecordSemantics {
    AppendOnly {
        logical_key: Vec<String>,
        duplicate_policy: DuplicatePolicy,
    },
    Versioned {
        logical_key: Vec<String>,
        duplicate_policy: DuplicatePolicy,
        revision: Revision,
    },
}

impl RecordSemantics {
    pub fn logical_key(&self) -> &[String] {
        match self {
            RecordSemantics::AppendOnly { logical_key, .. }
            | RecordSemantics::Versioned { logical_key, .. } => logical_key,
        }
    }
    pub fn duplicate_policy(&self) -> DuplicatePolicy {
        match self {
            RecordSemantics::AppendOnly {
                duplicate_policy, ..
            }
            | RecordSemantics::Versioned {
                duplicate_policy, ..
            } => *duplicate_policy,
        }
    }
    pub fn revision(&self) -> Option<&Revision> {
        match self {
            RecordSemantics::Versioned { revision, .. } => Some(revision),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceTime {
    pub event_time: String,
    pub available_time: String,
    pub timezone: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Completeness {
    pub mode: String,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceQuality {
    pub null_key_policy: String,
    pub invalid_row_policy: String,
    pub max_lateness_seconds: i64,
    pub completeness: Completeness,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extensions: Extensions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceRetention {
    pub raw_seconds: i64,
    pub revision_history_seconds: i64,
    pub earliest_reproducible_at: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpstreamColumn {
    pub column: String,
    pub origin: String,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceLineage {
    pub description: String,
    pub upstream_columns: Vec<UpstreamColumn>,
    pub code: Option<CodeProvenance>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Source {
    pub id: String,
    pub version: String,
    pub name: String,
    pub description: String,
    pub owner: String,
    pub columns: Vec<Column>,
    pub primary_key: Vec<String>,
    pub record_semantics: RecordSemantics,
    pub time: SourceTime,
    pub quality: SourceQuality,
    pub retention: SourceRetention,
    pub lineage: SourceLineage,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extensions: Extensions,
}

impl Source {
    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EntityKey {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: LogicalType,
    pub description: String,
    pub source_column: String,
    pub is_tenant: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Population {
    pub source: String,
    pub mode: String,
    pub event_time_upper: Boundary,
    pub availability_upper: Boundary,
    pub deletion_policy: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Entity {
    pub id: String,
    pub version: String,
    pub name: String,
    pub description: String,
    pub owner: String,
    pub keys: Vec<EntityKey>,
    pub population: Population,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extensions: Extensions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Endpoint {
    pub source: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum RelationshipTemporal {
    None,
    AsOf {
        reference: String,
        valid_from: String,
        valid_to: String,
        lower: Boundary,
        upper: Boundary,
        available_time_cutoff: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Cardinality {
    OneToOne,
    ManyToOne,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinType {
    Left,
    Inner,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NullKeys {
    Unmatched,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Unmatched {
    PreserveNull,
    Drop,
    Error,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Relationship {
    pub id: String,
    pub version: String,
    pub name: String,
    pub description: String,
    pub owner: String,
    pub from: Endpoint,
    pub to: Endpoint,
    pub cardinality: Cardinality,
    pub join_type: JoinType,
    pub null_keys: NullKeys,
    pub unmatched: Unmatched,
    pub multiple_matches: String,
    pub temporal: RelationshipTemporal,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extensions: Extensions,
}

/// Expression AST. Literal values are kept as JSON and typed during checking.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Expr {
    Literal {
        #[serde(rename = "type")]
        ty: String,
        value: Value,
    },
    Column {
        input: String,
        column: String,
    },
    Feature {
        feature: String,
        version: String,
    },
    Call {
        operator: Operator,
        args: Vec<Expr>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operator {
    Add,
    Subtract,
    Multiply,
    Divide,
    SafeDivide,
    Equal,
    NotEqual,
    LessThan,
    LessOrEqual,
    GreaterThan,
    GreaterOrEqual,
    And,
    Or,
    Not,
    IsNull,
    Coalesce,
    IfElse,
    Absolute,
}

impl Operator {
    pub fn as_str(self) -> &'static str {
        match self {
            Operator::Add => "add",
            Operator::Subtract => "subtract",
            Operator::Multiply => "multiply",
            Operator::Divide => "divide",
            Operator::SafeDivide => "safe_divide",
            Operator::Equal => "equal",
            Operator::NotEqual => "not_equal",
            Operator::LessThan => "less_than",
            Operator::LessOrEqual => "less_or_equal",
            Operator::GreaterThan => "greater_than",
            Operator::GreaterOrEqual => "greater_or_equal",
            Operator::And => "and",
            Operator::Or => "or",
            Operator::Not => "not",
            Operator::IsNull => "is_null",
            Operator::Coalesce => "coalesce",
            Operator::IfElse => "if_else",
            Operator::Absolute => "absolute",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Input {
    pub alias: String,
    pub source: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Dependency {
    pub feature: String,
    pub version: String,
    pub alignment: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Window {
    pub lookback_seconds: i64,
    pub lower: Boundary,
    pub upper: Boundary,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Availability {
    pub cutoff: String,
    pub upper: Boundary,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationDelay {
    pub delay_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeatureTime {
    pub timezone: String,
    pub window: Window,
    pub availability: Availability,
    pub knowledge_mode: String,
    pub publication: PublicationDelay,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Join {
    pub relationship: String,
    pub version: String,
    pub left_alias: String,
    pub right_alias: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupKey {
    pub entity_key: String,
    pub input: String,
    pub column: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregateOp {
    Count,
    CountDistinct,
    Sum,
    Mean,
    Min,
    Max,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NullPolicy {
    Ignore,
    Propagate,
    Error,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderKey {
    pub input: String,
    pub column: String,
    pub direction: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    pub id: String,
    pub version: String,
    pub uri: String,
    pub sha256: String,
    pub format: String,
    pub created_at: String,
    pub training_cutoff: String,
    pub training_available_cutoff: String,
    pub split_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fitting {
    pub policy: String,
    pub split_id: String,
    pub training_start: String,
    pub training_end: String,
    pub label_availability_cutoff: String,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContractInput {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: LogicalType,
    pub nullable: bool,
    pub expression: Expr,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Contract {
    pub entrypoint: String,
    pub inputs: Vec<ContractInput>,
    pub output_type: LogicalType,
    pub output_nullable: bool,
    pub deterministic: bool,
    pub side_effects: String,
    pub network_access: String,
    pub resource_limits: Value,
    pub null_policy: String,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Transform {
    Aggregate {
        input: String,
        joins: Vec<Join>,
        group_by: Vec<GroupKey>,
        filter: Option<Expr>,
        operation: AggregateOp,
        value: Option<Expr>,
        distinct_by: Vec<Expr>,
        nulls: NullPolicy,
    },
    Recency {
        input: String,
        joins: Vec<Join>,
        group_by: Vec<GroupKey>,
        filter: Option<Expr>,
        event_time: String,
        unit: String,
        rounding: String,
    },
    Lookup {
        input: String,
        joins: Vec<Join>,
        group_by: Vec<GroupKey>,
        filter: Option<Expr>,
        value: Expr,
        order_by: Vec<OrderKey>,
        on_tie: String,
    },
    Expression {
        expression: Expr,
    },
    Learned {
        artifact: Artifact,
        fitting: Fitting,
        contract: Contract,
        required_capabilities: Vec<String>,
        conformance_suite: String,
    },
    Custom {
        implementation: CodeProvenance,
        contract: Contract,
        required_capabilities: Vec<String>,
        conformance_suite: String,
    },
}

impl Transform {
    pub fn kind(&self) -> &'static str {
        match self {
            Transform::Aggregate { .. } => "aggregate",
            Transform::Recency { .. } => "recency",
            Transform::Lookup { .. } => "lookup",
            Transform::Expression { .. } => "expression",
            Transform::Learned { .. } => "learned",
            Transform::Custom { .. } => "custom",
        }
    }
    /// The row-level shape shared by aggregate, recency and lookup.
    pub fn row_shape(&self) -> Option<(&str, &[Join], &[GroupKey], Option<&Expr>)> {
        match self {
            Transform::Aggregate {
                input,
                joins,
                group_by,
                filter,
                ..
            }
            | Transform::Recency {
                input,
                joins,
                group_by,
                filter,
                ..
            }
            | Transform::Lookup {
                input,
                joins,
                group_by,
                filter,
                ..
            } => Some((input, joins, group_by, filter.as_ref())),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NumericTolerance {
    pub absolute: f64,
    pub relative: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Constraints {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub minimum: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub maximum: Option<f64>,
    #[serde(rename = "enum", skip_serializing_if = "Option::is_none")]
    pub enumeration: Option<Vec<Value>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Output {
    #[serde(rename = "type")]
    pub ty: LogicalType,
    pub nullable: bool,
    pub unit: Option<String>,
    pub description: String,
    pub constraints: Constraints,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub numeric_tolerance: Option<NumericTolerance>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LineageEntry {
    pub source: String,
    pub column: String,
    pub roles: Vec<String>,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DependencyLineage {
    pub feature: String,
    pub version: String,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeatureLineage {
    pub sources: Vec<LineageEntry>,
    pub dependencies: Vec<DependencyLineage>,
    pub code: Option<CodeProvenance>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum EmptyPolicy {
    Value { value: Value },
    Null,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NullOrError {
    Null,
    Error,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Missingness {
    pub empty: EmptyPolicy,
    pub null_input: NullPolicy,
    pub insufficient_history: NullOrError,
    pub source_unavailable: NullOrError,
    pub execution_failure: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum QualityCheck {
    Range {
        #[serde(skip_serializing_if = "Option::is_none")]
        minimum: Option<f64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        maximum: Option<f64>,
        severity: String,
    },
    NotNull {
        severity: String,
    },
    Finite {
        severity: String,
    },
}

impl QualityCheck {
    pub fn kind(&self) -> &'static str {
        match self {
            QualityCheck::Range { .. } => "range",
            QualityCheck::NotNull { .. } => "not_null",
            QualityCheck::Finite { .. } => "finite",
        }
    }
    pub fn severity(&self) -> &str {
        match self {
            QualityCheck::Range { severity, .. }
            | QualityCheck::NotNull { severity }
            | QualityCheck::Finite { severity } => severity,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeatureQuality {
    pub checks: Vec<QualityCheck>,
    pub on_violation: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extensions: Extensions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct History {
    pub required_seconds: i64,
    pub coverage: String,
    pub earliest_as_of: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeatureAcceptance {
    pub suites: Vec<String>,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Feature {
    pub id: String,
    pub version: String,
    pub name: String,
    pub description: String,
    pub owner: String,
    pub entity: String,
    pub output: Output,
    pub inputs: Vec<Input>,
    pub dependencies: Vec<Dependency>,
    pub time: FeatureTime,
    pub transform: Transform,
    pub lineage: FeatureLineage,
    pub missingness: Missingness,
    pub quality: FeatureQuality,
    pub history: History,
    pub acceptance: FeatureAcceptance,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extensions: Extensions,
}

impl Feature {
    pub fn input(&self, alias: &str) -> Option<&Input> {
        self.inputs.iter().find(|i| i.alias == alias)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeatureSet {
    pub id: String,
    pub version: String,
    pub name: String,
    pub description: String,
    pub owner: String,
    pub entity: String,
    pub features: Vec<NamedFeatureRef>,
    pub publication: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extensions: Extensions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EntityInput {
    pub entity_key: String,
    pub input_field: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimeInputs {
    pub as_of: String,
    pub requested_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dataset_cutoff: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectionMode {
    SourceAsKnown,
    HistoricalServingReplay,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Selection {
    pub mode: SelectionMode,
    pub tie_breaker: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LabelKey {
    pub entity_key: String,
    pub column: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LabelAggregation {
    AnyTrue,
    Count,
    Sum,
    Max,
    Latest,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Label {
    pub source: String,
    pub value_column: String,
    pub entity_keys: Vec<LabelKey>,
    pub event_time_column: String,
    pub available_time_column: String,
    pub horizon_seconds: i64,
    pub lower: Boundary,
    pub upper: Boundary,
    pub availability_policy: String,
    pub description: String,
    pub aggregation: LabelAggregation,
    pub empty: EmptyPolicy,
    pub incomplete_horizon: String,
    pub full_horizon_required: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissingValuePolicy {
    PreserveNull,
    Reject,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Consumer {
    pub id: String,
    pub version: String,
    pub name: String,
    pub description: String,
    pub owner: String,
    pub feature_set: FeatureSetRef,
    pub entity_inputs: Vec<EntityInput>,
    pub time_inputs: TimeInputs,
    pub selection: Selection,
    pub max_staleness_seconds: i64,
    pub missing_value_policy: MissingValuePolicy,
    pub label: Option<Label>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extensions: Extensions,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Location {
    File {
        path: String,
        format: String,
    },
    Table {
        namespace: Vec<String>,
        name: String,
        format: String,
    },
    Resource {
        uri: String,
        format: String,
    },
    Fixture {
        suite: String,
    },
}

impl Location {
    /// Resource identity excluding the format label (SEM-PORT-004).
    pub fn address(&self) -> String {
        match self {
            Location::File { path, .. } => format!("file:{path}"),
            Location::Table {
                namespace, name, ..
            } => {
                let parts: Vec<String> = namespace
                    .iter()
                    .chain(std::iter::once(name))
                    .map(|p| serde_json::to_string(p).unwrap())
                    .collect();
                format!("table:[{}]", parts.join(","))
            }
            Location::Resource { uri, .. } => format!("resource:{uri}"),
            Location::Fixture { suite } => format!("fixture:{suite}"),
        }
    }
}

pub type ColumnMap = Map<String, Value>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceBinding {
    pub source: String,
    pub read_only: bool,
    pub location: Location,
    pub version: String,
    pub adapter: String,
    pub column_mapping: ColumnMap,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputFeatureMapping {
    pub feature: String,
    pub version: String,
    pub column: String,
    pub state_column: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationColumns {
    pub as_of: String,
    pub published_at: String,
    pub publication_sequence: String,
    pub publication_id: String,
    pub materialization_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Compute,
    Catalog,
    OfflineStore,
    OnlineStore,
    Orchestrator,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Compute => "compute",
            Role::Catalog => "catalog",
            Role::OfflineStore => "offline_store",
            Role::OnlineStore => "online_store",
            Role::Orchestrator => "orchestrator",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputTarget {
    pub feature_set: String,
    pub version: String,
    pub location: Location,
    pub write_mode: String,
    pub adapter: String,
    pub role: Role,
    pub entity_key_mapping: ColumnMap,
    pub feature_mapping: Vec<OutputFeatureMapping>,
    pub metadata_columns: PublicationColumns,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum Schedule {
    Manual,
    Interval {
        every_seconds: i64,
        offset_seconds: i64,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Refresh {
    pub mode: String,
    pub lookback_seconds: i64,
    pub late_arrival_policy: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Surface {
    OfflineSnapshot,
    OnlineStore,
}

impl Surface {
    pub fn role(self) -> Role {
        match self {
            Surface::OfflineSnapshot => Role::OfflineStore,
            Surface::OnlineStore => Role::OnlineStore,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Surface::OfflineSnapshot => "offline_snapshot",
            Surface::OnlineStore => "online_store",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeploymentPublication {
    pub atomicity: String,
    pub visibility: String,
    pub observed_timestamp_required: bool,
    pub surface: Surface,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Backfill {
    Allowed {
        allowed: bool,
        start: String,
        end: String,
        mode: String,
        publish: String,
    },
    Disabled {
        allowed: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityRequirement {
    pub adapter: String,
    pub role: Role,
    pub capability: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Runtime {
    pub sdk_policy: String,
    pub required_capabilities: Vec<CapabilityRequirement>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Slo {
    pub max_compute_seconds: i64,
    pub max_staleness_seconds: i64,
    pub max_error_rate: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Budget {
    pub currency: String,
    pub max_daily_cost: f64,
    pub max_scan_bytes: i64,
    pub max_output_rows: i64,
    pub max_memory_mib: i64,
    pub max_concurrent_runs: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Monitoring {
    pub metrics: Vec<String>,
    pub alert_owner: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Permission {
    pub resource: String,
    pub privilege: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Access {
    pub read_principal: String,
    pub write_principal: String,
    pub required_permissions: Vec<Permission>,
    pub purpose: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterInstance {
    pub id: String,
    pub implementation: String,
    pub version: String,
    pub roles: Vec<Role>,
    pub configuration: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Execution {
    pub compute: String,
    pub catalog: String,
    pub orchestrator: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerBinding {
    pub consumer: String,
    pub version: String,
    pub adapter: String,
    pub role: Role,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeBinding {
    pub feature: NamedFeatureRef,
    pub adapter: String,
    pub role: Role,
    pub implementation: CodeProvenance,
    pub required_capabilities: Vec<String>,
    pub conformance_suite: String,
    pub migration_notes: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PortabilityMode {
    Portable,
    AllowNative,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Portability {
    pub mode: PortabilityMode,
    pub native_bindings: Vec<NativeBinding>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Deployment {
    pub id: String,
    pub version: String,
    pub name: String,
    pub description: String,
    pub owner: String,
    pub environment: String,
    pub source_bindings: Vec<SourceBinding>,
    pub output_targets: Vec<OutputTarget>,
    pub schedule: Schedule,
    pub refresh: Refresh,
    pub publication: DeploymentPublication,
    pub backfill: Backfill,
    pub runtime: Runtime,
    pub slo: Slo,
    pub budget: Budget,
    pub monitoring: Monitoring,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extensions: Extensions,
    pub access: Access,
    pub output_retention_seconds: i64,
    pub adapters: Vec<AdapterInstance>,
    pub execution: Execution,
    pub consumer_bindings: Vec<ConsumerBinding>,
    pub portability: Portability,
}

impl Deployment {
    pub fn adapter(&self, id: &str) -> Option<&AdapterInstance> {
        self.adapters.iter().find(|a| a.id == id)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExpectedRow {
    pub entity_keys: Row,
    pub value: Value,
    pub state: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishedValue {
    pub feature: String,
    pub version: String,
    pub entity_keys: Row,
    pub as_of: String,
    pub published_at: String,
    pub run_id: String,
    pub sequence: i64,
    pub value: Value,
    pub state: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceStatus {
    Available {
        source: String,
        coverage_start: String,
        coverage_end: String,
    },
    Unavailable {
        source: String,
    },
}

impl SourceStatus {
    pub fn source(&self) -> &str {
        match self {
            SourceStatus::Available { source, .. } | SourceStatus::Unavailable { source } => source,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaseContext {
    pub mode: SelectionMode,
    pub source_status: Vec<SourceStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requested_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evaluation_completed_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub published_values: Option<Vec<PublishedValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumer: Option<String>,
    pub capabilities: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub split_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExpectedDiagnostic {
    pub code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_contains: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Expectation {
    Rows { rows: Vec<ExpectedRow> },
    Diagnostic { diagnostic: ExpectedDiagnostic },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptanceCase {
    pub id: String,
    pub name: String,
    pub description: String,
    pub feature: NamedFeatureRef,
    pub as_of: String,
    pub population_rows: Vec<Row>,
    pub source_rows: Map<String, Value>,
    pub context: CaseContext,
    pub expected: Expectation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extensions: Extensions,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Runner {
    Reference,
    AdapterRequired,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptanceSuite {
    pub id: String,
    pub version: String,
    pub name: String,
    pub description: String,
    pub runner: Runner,
    pub cases: Vec<AcceptanceCase>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extensions: Extensions,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Project {
    pub spec_version: String,
    pub kind: String,
    pub metadata: Metadata,
    pub entities: Vec<Entity>,
    pub sources: Vec<Source>,
    pub relationships: Vec<Relationship>,
    pub features: Vec<Feature>,
    pub feature_sets: Vec<FeatureSet>,
    pub consumers: Vec<Consumer>,
    pub deployments: Vec<Deployment>,
    pub acceptance_suites: Vec<AcceptanceSuite>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extensions: Extensions,
}

impl Project {
    pub fn entity(&self, id: &str) -> Option<&Entity> {
        self.entities.iter().find(|x| x.id == id)
    }
    pub fn source(&self, id: &str) -> Option<&Source> {
        self.sources.iter().find(|x| x.id == id)
    }
    pub fn relationship(&self, id: &str) -> Option<&Relationship> {
        self.relationships.iter().find(|x| x.id == id)
    }
    pub fn feature(&self, id: &str) -> Option<&Feature> {
        self.features.iter().find(|x| x.id == id)
    }
    pub fn feature_set(&self, id: &str) -> Option<&FeatureSet> {
        self.feature_sets.iter().find(|x| x.id == id)
    }
    pub fn consumer(&self, id: &str) -> Option<&Consumer> {
        self.consumers.iter().find(|x| x.id == id)
    }
    pub fn deployment(&self, id: &str) -> Option<&Deployment> {
        self.deployments.iter().find(|x| x.id == id)
    }
    pub fn suite(&self, id: &str) -> Option<&AcceptanceSuite> {
        self.acceptance_suites.iter().find(|x| x.id == id)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportEvidence {
    pub uri: String,
    pub sha256: String,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterAssessment {
    pub id: String,
    pub implementation: String,
    pub version: String,
    pub implementation_sha256: Option<String>,
    pub runtime_versions: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityCheck {
    pub adapter: String,
    pub role: Role,
    pub capability: String,
    pub status: String,
    pub evidence_scope: String,
    pub evidence: Vec<ReportEvidence>,
    pub limitations: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeploymentIdentity {
    pub id: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityReport {
    pub spec_version: String,
    pub kind: String,
    pub project_id: String,
    pub project_version: String,
    pub project_file_sha256: String,
    pub deployment: DeploymentIdentity,
    pub assessed_at: String,
    pub execution_scope: String,
    pub adapters: Vec<AdapterAssessment>,
    pub checks: Vec<CapabilityCheck>,
    pub execution_ready: bool,
    pub limitations: Vec<String>,
}
