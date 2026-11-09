//! Staged loading: parse → schema → typed model → references/types.
//!
//! Each stage runs only when its prerequisite passed (SEM-CONF-002), and the
//! report records which stages were attempted, passed, failed or not evaluated
//! (SEM-CONF-001).

use crate::diag::{has_errors, Diagnostic, Stage};
use crate::model::Project;
use crate::parse::{parse_bytes, Format, ParsedDocument};
use crate::schema::validate_project;
use crate::validate::semantic_diagnostics;
use serde::Serialize;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StageStatus {
    Passed,
    Failed,
    NotEvaluated,
}

#[derive(Debug, Clone, Serialize)]
pub struct StageReport {
    pub stage: Stage,
    pub status: StageStatus,
}

#[derive(Debug, Clone)]
pub struct LoadedProject {
    pub document: ParsedDocument,
    pub project: Project,
    pub file_sha256: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct LoadReport {
    pub file_sha256: String,
    pub stages: Vec<StageReport>,
    pub diagnostics: Vec<Diagnostic>,
}

impl LoadReport {
    pub fn passed(&self) -> bool {
        self.stages.iter().all(|s| s.status == StageStatus::Passed)
    }
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn stages(statuses: [StageStatus; 3]) -> Vec<StageReport> {
    [Stage::Parse, Stage::Schema, Stage::References]
        .into_iter()
        .zip(statuses)
        .map(|(stage, status)| StageReport { stage, status })
        .collect()
}

/// Load and validate a project through the reference/type stage.
pub fn load_project(bytes: &[u8], format: Format) -> (Option<LoadedProject>, LoadReport) {
    use StageStatus::*;
    let file_sha256 = sha256_hex(bytes);
    let document = match parse_bytes(bytes, format) {
        Ok(d) => d,
        Err(diagnostics) => return (None, LoadReport { file_sha256, stages: stages([Failed, NotEvaluated, NotEvaluated]), diagnostics }),
    };
    let mut diagnostics = validate_project(&document);
    if has_errors(&diagnostics) {
        return (None, LoadReport { file_sha256, stages: stages([Passed, Failed, NotEvaluated]), diagnostics });
    }
    let project: Project = match serde_json::from_value(document.value.clone()) {
        Ok(p) => p,
        Err(e) => {
            diagnostics.push(Diagnostic::new(
                "SCHEMA_MODEL_CONFLICT",
                Stage::Schema,
                "SEM-CONF-001",
                "",
                format!("Schema-valid document does not match the canonical model; report this schema/model conflict: {e}"),
            ));
            return (None, LoadReport { file_sha256, stages: stages([Passed, Failed, NotEvaluated]), diagnostics });
        }
    };
    let mut semantic = semantic_diagnostics(&project);
    for d in semantic.iter_mut() {
        if d.location.is_none() {
            d.location = document.locate(&d.path);
        }
    }
    let failed = has_errors(&semantic);
    diagnostics.extend(semantic);
    let loaded = LoadedProject { document, project, file_sha256: file_sha256.clone() };
    let status = if failed { Failed } else { Passed };
    (Some(loaded), LoadReport { file_sha256, stages: stages([Passed, Passed, status]), diagnostics })
}

pub fn load_path(path: &std::path::Path) -> Result<(Option<LoadedProject>, LoadReport), String> {
    let bytes = std::fs::read(path).map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    if bytes.len() > crate::parse::MAX_DOCUMENT_BYTES {
        return Err(format!("{} exceeds the {}-byte document limit", path.display(), crate::parse::MAX_DOCUMENT_BYTES));
    }
    Ok(load_project(&bytes, Format::from_path(&path.to_string_lossy())))
}
