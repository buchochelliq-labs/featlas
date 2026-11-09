//! Structured diagnostics shared by every stage.
//!
//! A diagnostic identifies a stable code, severity, validation stage, the
//! normative rule (SEM-*) it enforces, the JSON-pointer instance path, an
//! optional source location, related object IDs/versions and an actionable
//! message. "Invalid input" without location or reason is never emitted.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    Parse,
    Schema,
    References,
    Planning,
    Data,
    Runtime,
}

impl Stage {
    pub fn as_str(self) -> &'static str {
        match self {
            Stage::Parse => "parse",
            Stage::Schema => "schema",
            Stage::References => "references",
            Stage::Planning => "planning",
            Stage::Data => "data",
            Stage::Runtime => "runtime",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Error,
    Warning,
    Info,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceLocation {
    pub line: usize,
    pub column: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Diagnostic {
    pub code: String,
    pub severity: Severity,
    pub stage: Stage,
    pub rule: String,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<SourceLocation>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub related: Vec<String>,
    pub message: String,
}

impl Diagnostic {
    pub fn new(
        code: &str,
        stage: Stage,
        rule: &str,
        path: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Diagnostic {
            code: code.to_string(),
            severity: Severity::Error,
            stage,
            rule: rule.to_string(),
            path: path.into(),
            location: None,
            related: Vec::new(),
            message: message.into(),
        }
    }
    pub fn warning(mut self) -> Self {
        self.severity = Severity::Warning;
        self
    }
    pub fn info(mut self) -> Self {
        self.severity = Severity::Info;
        self
    }
    pub fn related(mut self, items: impl IntoIterator<Item = String>) -> Self {
        self.related.extend(items);
        self
    }
    pub fn at(mut self, location: Option<SourceLocation>) -> Self {
        self.location = location;
        self
    }
    pub fn is_error(&self) -> bool {
        self.severity == Severity::Error
    }
}

impl std::fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} [{}:{}] {}",
            self.code,
            self.stage.as_str(),
            self.rule,
            self.path
        )?;
        if let Some(loc) = &self.location {
            write!(f, " (line {}, column {})", loc.line, loc.column)?;
        }
        write!(f, ": {}", self.message)
    }
}

/// An evaluation failure that stops a computation with a stable code.
#[derive(Debug, Clone, PartialEq)]
pub struct EvalError {
    pub code: String,
    pub rule: String,
    pub message: String,
}

impl EvalError {
    pub fn new(code: &str, rule: &str, message: impl Into<String>) -> Self {
        EvalError {
            code: code.into(),
            rule: rule.into(),
            message: message.into(),
        }
    }
    pub fn to_diagnostic(&self, path: &str) -> Diagnostic {
        Diagnostic::new(
            &self.code,
            Stage::Data,
            &self.rule,
            path,
            self.message.clone(),
        )
    }
}

impl std::fmt::Display for EvalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} [{}]: {}", self.code, self.rule, self.message)
    }
}

pub fn has_errors(diags: &[Diagnostic]) -> bool {
    diags.iter().any(|d| d.is_error())
}
