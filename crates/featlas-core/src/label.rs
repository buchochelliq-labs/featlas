//! Forward-horizon label construction (SEM-LABEL-001/002).
//!
//! Labels are independent of the feature DAG: the label source is
//! reconstructed as known at the dataset cutoff (availability ≤ cutoff),
//! revision rules applied, then the horizon `[T, T + H]` with declared bounds
//! is applied to event time. A row is labelled only when its horizon has
//! matured by the cutoff *and* positive completeness evidence covers the
//! horizon; otherwise `incomplete_horizon` decides exclusion or error. Only a
//! known-complete empty horizon can use `label.empty`. Feature filters are
//! never applied to labels.

use crate::diag::EvalError;
use crate::eval::{fsum, reconstruct_rows, Limits};
use crate::model::*;
use crate::time::Instant;
use crate::value::{compare, key_string, V};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as Json};
use std::cmp::Ordering;

fn err(code: &str, msg: impl Into<String>) -> EvalError {
    EvalError::new(code, "SEM-LABEL-001", msg)
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LabelInput {
    /// Consumer request rows (entity input fields, as_of, requested_at, dataset_cutoff).
    pub rows: Vec<Map<String, Json>>,
    /// Rows of the label source (exact source columns).
    pub source_rows: Vec<Json>,
    /// Completeness evidence for the label source as known at the dataset cutoff.
    pub source_status: SourceStatus,
}

#[derive(Debug, Clone, Serialize)]
pub struct LabelRow {
    pub request_index: usize,
    pub entity_keys: Map<String, Json>,
    pub as_of: String,
    pub dataset_cutoff: String,
    pub value: Json,
    pub state: String,
    pub eligible_rows: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct Excluded {
    pub request_index: usize,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct LabelDataset {
    pub consumer: String,
    pub consumer_version: String,
    pub label_source: String,
    pub rows: Vec<LabelRow>,
    pub excluded: Vec<Excluded>,
}

pub fn build_labels(
    p: &Project,
    consumer_id: &str,
    input: &LabelInput,
    limits: &Limits,
) -> Result<LabelDataset, EvalError> {
    let c = p
        .consumer(consumer_id)
        .ok_or_else(|| err("UNKNOWN_CONSUMER", consumer_id))?;
    let label = c.label.as_ref().ok_or_else(|| {
        err(
            "LABEL_NOT_DECLARED",
            format!("{consumer_id} declares no label"),
        )
    })?;
    let cutoff_field = c.time_inputs.dataset_cutoff.as_ref().ok_or_else(|| {
        err(
            "LABEL_CUTOFF_REQUIRED",
            "Consumer has no dataset_cutoff input",
        )
    })?;
    let fs = p
        .feature_set(&c.feature_set.feature_set)
        .ok_or_else(|| err("UNKNOWN_FEATURE_SET", &c.feature_set.feature_set))?;
    let entity = p
        .entity(&fs.entity)
        .ok_or_else(|| err("UNKNOWN_ENTITY", &fs.entity))?;
    let source = p
        .source(&label.source)
        .ok_or_else(|| err("UNKNOWN_SOURCE", &label.source))?;
    if input.source_status.source() != source.id {
        return Err(err(
            "MISSING_SOURCE_STATUS",
            format!("Status evidence must describe label source {}", source.id),
        ));
    }
    let value_col = source
        .column(&label.value_column)
        .ok_or_else(|| err("UNKNOWN_COLUMN", &label.value_column))?;
    let mut rows_map = Map::new();
    rows_map.insert(source.id.clone(), Json::Array(input.source_rows.clone()));
    let mut out = Vec::new();
    let mut excluded = Vec::new();
    let mut cache: std::collections::HashMap<i64, Vec<crate::eval::TRow>> = Default::default();
    for (i, row) in input.rows.iter().enumerate() {
        let ts = |field: &str| -> Result<Instant, EvalError> {
            let raw = row
                .get(field)
                .and_then(Json::as_str)
                .ok_or_else(|| err("REQUEST_TIME_MISSING", format!("Row {i} lacks {field}")))?;
            Instant::parse(raw).map_err(|m| err("INVALID_TIMESTAMP", m))
        };
        let t = ts(&c.time_inputs.as_of)?;
        let cutoff = ts(cutoff_field)?;
        let mut key = Vec::new();
        let mut entity_keys = Map::new();
        for k in &entity.keys {
            let m = c
                .entity_inputs
                .iter()
                .find(|m| m.entity_key == k.name)
                .ok_or_else(|| err("ENTITY_MAPPING_INCOMPLETE", &k.name))?;
            let raw = row.get(&m.input_field).ok_or_else(|| {
                err(
                    "REQUEST_KEY_MISSING",
                    format!("Row {i} lacks {}", m.input_field),
                )
            })?;
            let v = V::from_json(raw, k.ty).map_err(|m| err("REQUEST_KEY_TYPE", m))?;
            if v.is_null() {
                return Err(err(
                    "REQUEST_KEY_MISSING",
                    format!("Row {i}: entity key {} is null", k.name),
                ));
            }
            key.push(v);
            entity_keys.insert(k.name.clone(), raw.clone());
        }
        let end = t
            .checked_add_seconds(label.horizon_seconds)
            .ok_or_else(|| err("NUMERIC_OVERFLOW", "Horizon overflows"))?;
        // Maturity and completeness of the entire horizon at the dataset cutoff.
        let mut incomplete = None;
        if end > cutoff {
            incomplete = Some(format!(
                "Horizon ends {end}, after dataset cutoff {cutoff}: immature"
            ));
        }
        match &input.source_status {
            SourceStatus::Unavailable { .. } => {
                incomplete = Some("Label source unavailable".into())
            }
            SourceStatus::Available {
                coverage_start,
                coverage_end,
                ..
            } => {
                let cs = Instant::parse(coverage_start).map_err(|m| err("INVALID_TIMESTAMP", m))?;
                let ce = Instant::parse(coverage_end).map_err(|m| err("INVALID_TIMESTAMP", m))?;
                let end_ok = if label.upper == Boundary::Inclusive {
                    ce > end
                } else {
                    ce >= end
                };
                if cs > t || !end_ok {
                    incomplete.get_or_insert_with(|| {
                        "Completeness evidence does not cover the whole horizon (censored)".into()
                    });
                }
            }
        }
        if let Some(reason) = incomplete {
            if label.incomplete_horizon == "error" {
                return Err(err(
                    "LABEL_INCOMPLETE_HORIZON",
                    format!("Row {i}: {reason}"),
                ));
            }
            excluded.push(Excluded {
                request_index: i,
                reason,
            });
            continue;
        }
        if !cache.contains_key(&cutoff.micros) {
            let rec = reconstruct_rows(
                p,
                &source.id,
                &rows_map,
                cutoff,
                Boundary::Inclusive,
                limits,
            )?;
            cache.insert(cutoff.micros, rec);
        }
        let known = &cache[&cutoff.micros];
        let k = key_string(&key);
        let mut eligible = Vec::new();
        for r in known {
            let rk: Vec<V> = entity
                .keys
                .iter()
                .map(|ek| {
                    label
                        .entity_keys
                        .iter()
                        .find(|m| m.entity_key == ek.name)
                        .map(|m| r[&m.column].clone())
                        .unwrap_or(V::Null)
                })
                .collect();
            if key_string(&rk) != k {
                continue;
            }
            let V::Ts(ev) = r[&label.event_time_column] else {
                continue;
            };
            let lower_ok = if label.lower == Boundary::Inclusive {
                ev >= t
            } else {
                ev > t
            };
            let upper_ok = if label.upper == Boundary::Inclusive {
                ev <= end
            } else {
                ev < end
            };
            if lower_ok && upper_ok {
                eligible.push(r);
            }
        }
        let (value, state) = if eligible.is_empty() {
            match &label.empty {
                EmptyPolicy::Error => {
                    return Err(err(
                        "LABEL_EMPTY",
                        format!("Row {i}: known-complete empty horizon with empty action error"),
                    ))
                }
                EmptyPolicy::Null => (V::Null, "empty"),
                EmptyPolicy::Value { value } => {
                    let ty = match label.aggregation {
                        LabelAggregation::Count => LogicalType::Integer,
                        LabelAggregation::AnyTrue => LogicalType::Boolean,
                        _ => value_col.ty,
                    };
                    (
                        V::from_json(value, ty).map_err(|m| err("TYPE_MISMATCH", m))?,
                        "empty",
                    )
                }
            }
        } else {
            let vals: Vec<&V> = eligible.iter().map(|r| &r[&label.value_column]).collect();
            let v = match label.aggregation {
                LabelAggregation::Count => V::Int(eligible.len() as i64),
                LabelAggregation::AnyTrue => {
                    if vals.iter().any(|v| **v == V::Bool(true)) {
                        V::Bool(true)
                    } else if vals.iter().any(|v| v.is_null()) {
                        V::Null
                    } else {
                        V::Bool(false)
                    }
                }
                LabelAggregation::Sum => {
                    if vals.iter().any(|v| v.is_null()) {
                        V::Null
                    } else if vals.iter().all(|v| matches!(v, V::Int(_))) {
                        let s: i128 = vals
                            .iter()
                            .map(|v| if let V::Int(x) = v { i128::from(*x) } else { 0 })
                            .sum();
                        V::Int(
                            i64::try_from(s)
                                .map_err(|_| err("NUMERIC_OVERFLOW", "Label sum overflows"))?,
                        )
                    } else {
                        let f: Vec<f64> = vals
                            .iter()
                            .map(|v| match v {
                                V::Int(x) => *x as f64,
                                V::Num(x) => *x,
                                _ => f64::NAN,
                            })
                            .collect();
                        let total = fsum(&f);
                        if !total.is_finite() {
                            return Err(EvalError::new(
                                "NON_FINITE_RESULT",
                                "SEM-TRANS-006",
                                format!("Row {i}: label sum is not finite"),
                            ));
                        }
                        V::Num(total)
                    }
                }
                LabelAggregation::Max => {
                    if vals.iter().any(|v| v.is_null()) {
                        V::Null
                    } else {
                        let mut best = vals[0].clone();
                        for v in &vals[1..] {
                            if compare(v, &best) == Some(Ordering::Greater) {
                                best = (*v).clone();
                            }
                        }
                        best
                    }
                }
                LabelAggregation::Latest => {
                    let mut sorted: Vec<&&crate::eval::TRow> = eligible.iter().collect();
                    let order = |a: &crate::eval::TRow, b: &crate::eval::TRow| {
                        compare(&b[&label.event_time_column], &a[&label.event_time_column])
                            .unwrap_or(Ordering::Equal)
                            .then(
                                compare(
                                    &b[&label.available_time_column],
                                    &a[&label.available_time_column],
                                )
                                .unwrap_or(Ordering::Equal),
                            )
                    };
                    sorted.sort_by(|a, b| order(a, b));
                    if sorted.len() > 1 && order(sorted[0], sorted[1]) == Ordering::Equal {
                        return Err(err(
                            "LABEL_TIE",
                            format!(
                                "Row {i}: latest label rows tie on event and availability time"
                            ),
                        ));
                    }
                    sorted[0][&label.value_column].clone()
                }
            };
            (v, "ok")
        };
        out.push(LabelRow {
            request_index: i,
            entity_keys,
            as_of: t.to_string(),
            dataset_cutoff: cutoff.to_string(),
            value: value.to_json(),
            state: state.into(),
            eligible_rows: eligible.len(),
        });
    }
    Ok(LabelDataset {
        consumer: c.id.clone(),
        consumer_version: c.version.clone(),
        label_source: source.id.clone(),
        rows: out,
        excluded,
    })
}
