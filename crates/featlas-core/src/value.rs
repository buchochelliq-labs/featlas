//! Runtime scalar values and type-aware comparison.
//!
//! `integer` is signed 64-bit and `number` is finite binary64 (SEM-TRANS-006).
//! Ordering is numeric across integer/number (exactly, without lossy casts),
//! chronological for timestamp/date, false < true, and Unicode code-point
//! order for strings. There is no implicit string/number/boolean coercion.

use crate::model::LogicalType;
use crate::time::{Date, Instant};
use serde_json::{Number, Value as Json};
use std::cmp::Ordering;

#[derive(Debug, Clone, PartialEq)]
pub enum V {
    Null,
    Bool(bool),
    Int(i64),
    Num(f64),
    Str(String),
    Ts(Instant),
    Date(Date),
}

impl V {
    pub fn is_null(&self) -> bool {
        matches!(self, V::Null)
    }

    /// Convert a JSON fixture/literal value to a typed value; `Err` explains the mismatch.
    pub fn from_json(value: &Json, ty: LogicalType) -> Result<V, String> {
        match (value, ty) {
            (Json::Null, _) => Ok(V::Null),
            (Json::Bool(b), LogicalType::Boolean) => Ok(V::Bool(*b)),
            (Json::Number(n), LogicalType::Integer) => n.as_i64().map(V::Int).ok_or_else(|| format!("{n} is not a signed 64-bit integer")),
            (Json::Number(n), LogicalType::Number) => match n.as_f64() {
                Some(f) if f.is_finite() => Ok(V::Num(f)),
                _ => Err(format!("{n} is not a finite number")),
            },
            (Json::String(s), LogicalType::String) => Ok(V::Str(s.clone())),
            (Json::String(s), LogicalType::Timestamp) => Instant::parse(s).map(V::Ts),
            (Json::String(s), LogicalType::Date) => Date::parse(s).map(V::Date),
            (other, ty) => Err(format!("{other} is not a valid {}", ty.as_str())),
        }
    }

    pub fn to_json(&self) -> Json {
        match self {
            V::Null => Json::Null,
            V::Bool(b) => Json::Bool(*b),
            V::Int(i) => Json::Number(Number::from(*i)),
            V::Num(f) => Number::from_f64(*f).map(Json::Number).unwrap_or(Json::Null),
            V::Str(s) => Json::String(s.clone()),
            V::Ts(t) => Json::String(t.to_string()),
            V::Date(d) => Json::String(d.to_string()),
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            V::Null => "null",
            V::Bool(_) => "boolean",
            V::Int(_) => "integer",
            V::Num(_) => "number",
            V::Str(_) => "string",
            V::Ts(_) => "timestamp",
            V::Date(_) => "date",
        }
    }
}

/// Exact comparison of an i64 with a finite f64.
pub fn cmp_int_num(i: i64, f: f64) -> Ordering {
    if f >= 9_223_372_036_854_775_808.0 {
        return Ordering::Less;
    }
    if f < -9_223_372_036_854_775_808.0 {
        return Ordering::Greater;
    }
    let fl = f.floor();
    let fi = fl as i64;
    match i.cmp(&fi) {
        Ordering::Equal if f > fl => Ordering::Less,
        other => other,
    }
}

/// Compare two non-null values of compatible types. `None` for incompatible types.
pub fn compare(a: &V, b: &V) -> Option<Ordering> {
    match (a, b) {
        (V::Int(x), V::Int(y)) => Some(x.cmp(y)),
        (V::Num(x), V::Num(y)) => x.partial_cmp(y),
        (V::Int(x), V::Num(y)) => Some(cmp_int_num(*x, *y)),
        (V::Num(x), V::Int(y)) => Some(cmp_int_num(*y, *x).reverse()),
        (V::Bool(x), V::Bool(y)) => Some(x.cmp(y)),
        (V::Str(x), V::Str(y)) => Some(x.chars().cmp(y.chars())),
        (V::Ts(x), V::Ts(y)) => Some(x.cmp(y)),
        (V::Date(x), V::Date(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

/// Null-aware structural identity used for duplicate detection and grouping
/// (null equals null here; this is *not* expression equality).
pub fn identical(a: &V, b: &V) -> bool {
    match (a, b) {
        (V::Null, V::Null) => true,
        (V::Num(x), V::Num(y)) => x.to_bits() == y.to_bits() || x == y,
        _ => a == b,
    }
}

/// Canonical grouping key for a tuple of values (type-tagged, exact).
pub fn key_string(values: &[V]) -> String {
    let mut out = String::new();
    for v in values {
        let part = match v {
            V::Null => "n".to_string(),
            V::Bool(b) => format!("b{b}"),
            V::Int(i) => format!("i{i}"),
            V::Num(f) => {
                if *f == 0.0 { "f0".to_string() } else { format!("f{:x}", f.to_bits()) }
            }
            V::Str(s) => format!("s{}", serde_json::to_string(s).unwrap()),
            V::Ts(t) => format!("t{}", t.micros),
            V::Date(d) => format!("d{}", d.days),
        };
        out.push_str(&part);
        out.push('\u{1f}');
    }
    out
}

/// Tolerance comparison: `abs(a - e) <= max(absolute, relative * abs(e))`.
/// Non-numeric values and nulls compare by type-aware exact equality.
pub fn equal_with_tolerance(actual: &Json, expected: &Json, ty: LogicalType, absolute: f64, relative: f64) -> bool {
    match (actual, expected) {
        (Json::Null, Json::Null) => true,
        (Json::Null, _) | (_, Json::Null) => false,
        (Json::Number(a), Json::Number(e)) if ty == LogicalType::Integer => match (a.as_i64(), e.as_i64()) {
            (Some(x), Some(y)) => x == y,
            _ => false,
        },
        (Json::Number(a), Json::Number(e)) if ty == LogicalType::Number => match (a.as_f64(), e.as_f64()) {
            (Some(x), Some(y)) => (x - y).abs() <= absolute.max(relative * y.abs()),
            _ => false,
        },
        (Json::Number(_), _) | (_, Json::Number(_)) => false,
        (a, e) => match (V::from_json(a, ty), V::from_json(e, ty)) {
            (Ok(x), Ok(y)) => identical(&x, &y),
            _ => a == e,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_int_number_comparison() {
        assert_eq!(cmp_int_num(9_007_199_254_740_993, 9_007_199_254_740_992.0), Ordering::Greater);
        assert_eq!(cmp_int_num(1, 1.0), Ordering::Equal);
        assert_eq!(cmp_int_num(1, 1.5), Ordering::Less);
        assert_eq!(cmp_int_num(-2, -1.5), Ordering::Less);
        assert_eq!(cmp_int_num(i64::MAX, 9.3e18), Ordering::Less);
    }

    #[test]
    fn strings_use_code_point_order() {
        assert_eq!(compare(&V::Str("Z".into()), &V::Str("a".into())), Some(Ordering::Less));
        assert_eq!(compare(&V::Str("é".into()), &V::Str("z".into())), Some(Ordering::Greater));
        assert_eq!(compare(&V::Str("1".into()), &V::Int(1)), None);
    }

    #[test]
    fn tolerance_rules() {
        use serde_json::json;
        assert!(equal_with_tolerance(&json!(0.25), &json!(0.25), LogicalType::Number, 0.0, 0.0));
        assert!(!equal_with_tolerance(&json!(null), &json!(0), LogicalType::Integer, 0.0, 0.0));
        assert!(!equal_with_tolerance(&json!(false), &json!(null), LogicalType::Boolean, 0.0, 0.0));
        assert!(equal_with_tolerance(&json!(1.0000001), &json!(1.0), LogicalType::Number, 1e-6, 0.0));
        assert!(!equal_with_tolerance(&json!(1.0), &json!(1), LogicalType::Integer, 0.0, 0.0));
    }
}
