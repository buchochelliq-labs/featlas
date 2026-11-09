//! Typed expression AST checking and exact evaluation (SEM-TRANS-002/003/006).
//!
//! Type checking runs before any execution. Evaluation is lazy for `and`,
//! `or`, `coalesce` and `if_else`, uses three-valued logic, checked signed
//! 64-bit integer arithmetic, integer→number widening only, and rejects
//! non-finite results and division by zero.

use crate::diag::EvalError;
use crate::model::{Expr, LogicalType, Operator};
use crate::value::{compare, V};
use std::cmp::Ordering;

/// Static type of an expression: `ty == None` means the null literal type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ty {
    pub ty: Option<LogicalType>,
    pub nullable: bool,
}

impl Ty {
    pub fn of(ty: LogicalType, nullable: bool) -> Ty {
        Ty {
            ty: Some(ty),
            nullable,
        }
    }
    pub const NULL: Ty = Ty {
        ty: None,
        nullable: true,
    };
}

/// Resolves references while type checking.
pub trait TypeEnv {
    fn column(&self, input: &str, column: &str) -> Result<Ty, (String, String)>;
    fn feature(&self, feature: &str, version: &str) -> Result<Ty, (String, String)>;
}

#[derive(Debug, Clone)]
pub struct TypeError {
    pub code: String,
    pub path: String,
    pub message: String,
}

fn terr(code: &str, path: &str, message: impl Into<String>) -> TypeError {
    TypeError {
        code: code.into(),
        path: path.into(),
        message: message.into(),
    }
}

/// Unify result types for coalesce/if_else branches with numeric widening.
fn unify(types: &[Ty], path: &str, what: &str) -> Result<Ty, TypeError> {
    let mut result: Option<LogicalType> = None;
    for t in types {
        if let Some(ty) = t.ty {
            result = match result {
                None => Some(ty),
                Some(r) if r == ty => Some(r),
                Some(r) if r.is_numeric() && ty.is_numeric() => Some(LogicalType::Number),
                Some(r) => {
                    return Err(terr(
                        "TYPE_MISMATCH",
                        path,
                        format!(
                            "{what} have incompatible types {} and {}",
                            r.as_str(),
                            ty.as_str()
                        ),
                    ))
                }
            };
        }
    }
    Ok(Ty {
        ty: result,
        nullable: types.iter().any(|t| t.nullable),
    })
}

pub fn literal_value(ty: &str, value: &serde_json::Value) -> Result<V, String> {
    let logical = match ty {
        "null" => {
            return if value.is_null() {
                Ok(V::Null)
            } else {
                Err("null literal must have value null".into())
            }
        }
        "integer" => LogicalType::Integer,
        "number" => LogicalType::Number,
        "boolean" => LogicalType::Boolean,
        "string" => LogicalType::String,
        "timestamp" => LogicalType::Timestamp,
        "date" => LogicalType::Date,
        other => return Err(format!("unknown literal type {other}")),
    };
    if value.is_null() {
        return Err(format!("{ty} literal must not be null; use a null literal"));
    }
    V::from_json(value, logical)
}

/// Type-check an expression, collecting every error found.
pub fn check(
    expr: &Expr,
    env: &dyn TypeEnv,
    path: &str,
    errors: &mut Vec<TypeError>,
) -> Option<Ty> {
    match expr {
        Expr::Literal { ty, value } => match literal_value(ty, value) {
            Ok(V::Null) => Some(Ty::NULL),
            Ok(v) => Some(Ty::of(
                match v {
                    V::Bool(_) => LogicalType::Boolean,
                    V::Int(_) => LogicalType::Integer,
                    V::Num(_) => LogicalType::Number,
                    V::Str(_) => LogicalType::String,
                    V::Ts(_) => LogicalType::Timestamp,
                    V::Date(_) => LogicalType::Date,
                    V::Null => unreachable!(),
                },
                false,
            )),
            Err(m) => {
                errors.push(terr(
                    "TYPE_MISMATCH",
                    path,
                    format!("Literal value and declared type disagree: {m}"),
                ));
                None
            }
        },
        Expr::Column { input, column } => match env.column(input, column) {
            Ok(t) => Some(t),
            Err((code, msg)) => {
                errors.push(terr(&code, path, msg));
                None
            }
        },
        Expr::Feature { feature, version } => match env.feature(feature, version) {
            Ok(t) => Some(t),
            Err((code, msg)) => {
                errors.push(terr(&code, path, msg));
                None
            }
        },
        Expr::Call { operator, args } => {
            let types: Vec<Option<Ty>> = args
                .iter()
                .enumerate()
                .map(|(i, a)| check(a, env, &format!("{path}/args/{i}"), errors))
                .collect();
            let op = *operator;
            let arity_ok = match op {
                Operator::Absolute | Operator::Not | Operator::IsNull => args.len() == 1,
                Operator::IfElse => args.len() == 3,
                Operator::And | Operator::Or | Operator::Coalesce => !args.is_empty(),
                _ => args.len() == 2,
            };
            if !arity_ok {
                errors.push(terr(
                    "OPERATOR_ARITY",
                    path,
                    format!(
                        "{} has wrong number of arguments ({})",
                        op.as_str(),
                        args.len()
                    ),
                ));
                return None;
            }
            if types.iter().any(Option::is_none) {
                return None;
            }
            let t: Vec<Ty> = types.into_iter().map(Option::unwrap).collect();
            let nullable = t.iter().any(|x| x.nullable);
            let numeric = |x: &Ty| x.ty.is_none_or(|y| y.is_numeric());
            let result = match op {
                Operator::Add | Operator::Subtract | Operator::Multiply => {
                    if !t.iter().all(numeric) {
                        return push(
                            errors,
                            terr(
                                "TYPE_MISMATCH",
                                path,
                                format!("{} requires numeric operands", op.as_str()),
                            ),
                        );
                    }
                    let ty = if t.iter().any(|x| x.ty == Some(LogicalType::Number)) {
                        Some(LogicalType::Number)
                    } else if t.iter().any(|x| x.ty == Some(LogicalType::Integer)) {
                        Some(LogicalType::Integer)
                    } else {
                        None
                    };
                    Ty { ty, nullable }
                }
                Operator::Divide | Operator::SafeDivide => {
                    if !t.iter().all(numeric) {
                        return push(
                            errors,
                            terr(
                                "TYPE_MISMATCH",
                                path,
                                format!("{} requires numeric operands", op.as_str()),
                            ),
                        );
                    }
                    Ty {
                        ty: Some(LogicalType::Number),
                        nullable: nullable || op == Operator::SafeDivide,
                    }
                }
                Operator::Absolute => {
                    if !numeric(&t[0]) {
                        return push(
                            errors,
                            terr("TYPE_MISMATCH", path, "absolute requires a numeric operand"),
                        );
                    }
                    t[0]
                }
                Operator::Equal
                | Operator::NotEqual
                | Operator::LessThan
                | Operator::LessOrEqual
                | Operator::GreaterThan
                | Operator::GreaterOrEqual => {
                    if let (Some(a), Some(b)) = (t[0].ty, t[1].ty) {
                        if a != b && !(a.is_numeric() && b.is_numeric()) {
                            return push(
                                errors,
                                terr(
                                    "TYPE_MISMATCH",
                                    path,
                                    format!(
                                        "Comparison operands have incompatible types {} and {}",
                                        a.as_str(),
                                        b.as_str()
                                    ),
                                ),
                            );
                        }
                    }
                    Ty {
                        ty: Some(LogicalType::Boolean),
                        nullable,
                    }
                }
                Operator::And | Operator::Or | Operator::Not => {
                    if !t
                        .iter()
                        .all(|x| x.ty.is_none_or(|y| y == LogicalType::Boolean))
                    {
                        return push(
                            errors,
                            terr(
                                "TYPE_MISMATCH",
                                path,
                                format!("{} requires boolean operands", op.as_str()),
                            ),
                        );
                    }
                    Ty {
                        ty: Some(LogicalType::Boolean),
                        nullable,
                    }
                }
                Operator::IsNull => Ty {
                    ty: Some(LogicalType::Boolean),
                    nullable: false,
                },
                Operator::Coalesce => {
                    let u = unify(&t, path, "coalesce arguments")
                        .map_err(|e| errors.push(e))
                        .ok()?;
                    Ty {
                        ty: u.ty,
                        nullable: t.iter().all(|x| x.nullable),
                    }
                }
                Operator::IfElse => {
                    if t[0].ty.is_some_and(|y| y != LogicalType::Boolean) {
                        return push(
                            errors,
                            terr("TYPE_MISMATCH", path, "if_else condition must be boolean"),
                        );
                    }
                    unify(&t[1..], path, "if_else branches")
                        .map_err(|e| errors.push(e))
                        .ok()?
                }
            };
            Some(result)
        }
    }
}

fn push(errors: &mut Vec<TypeError>, e: TypeError) -> Option<Ty> {
    errors.push(e);
    None
}

/// Supplies values while evaluating.
pub trait ValueEnv {
    fn column(&self, input: &str, column: &str) -> Result<V, EvalError>;
    fn feature(&self, feature: &str, version: &str) -> Result<V, EvalError>;
    /// Static result type of a sub-expression, used to widen integer values to number where the
    /// type checker unified to number (coalesce, if_else, mixed arithmetic).
    fn static_type(&self, _e: &Expr) -> Option<LogicalType> {
        None
    }
}

fn overflow(op: Operator) -> EvalError {
    EvalError::new(
        "NUMERIC_OVERFLOW",
        "SEM-TRANS-006",
        format!("Signed 64-bit integer overflow in {}", op.as_str()),
    )
}

fn finite(f: f64, op: Operator) -> Result<V, EvalError> {
    if f.is_finite() {
        Ok(V::Num(f))
    } else {
        Err(EvalError::new(
            "NON_FINITE_RESULT",
            "SEM-TRANS-006",
            format!("{} produced a non-finite number", op.as_str()),
        ))
    }
}

fn as_f64(v: &V) -> f64 {
    match v {
        V::Int(i) => *i as f64,
        V::Num(f) => *f,
        _ => f64::NAN,
    }
}

fn mismatch(op: Operator, v: &V) -> EvalError {
    EvalError::new(
        "TYPE_MISMATCH",
        "SEM-TRANS-002",
        format!("{} received {}", op.as_str(), v.type_name()),
    )
}

pub fn eval(expr: &Expr, env: &dyn ValueEnv) -> Result<V, EvalError> {
    match expr {
        Expr::Literal { ty, value } => literal_value(ty, value)
            .map_err(|m| EvalError::new("TYPE_MISMATCH", "SEM-TRANS-002", m)),
        Expr::Column { input, column } => env.column(input, column),
        Expr::Feature { feature, version } => env.feature(feature, version),
        Expr::Call { operator, args } => {
            let op = *operator;
            match op {
                Operator::IfElse => {
                    let c = eval(&args[0], env)?;
                    let v = match c {
                        V::Bool(true) => eval(&args[1], env)?,
                        V::Bool(false) | V::Null => eval(&args[2], env)?,
                        other => return Err(mismatch(op, &other)),
                    };
                    Ok(conform(v, env.static_type(expr)))
                }
                Operator::Coalesce => {
                    for a in args {
                        let v = eval(a, env)?;
                        if !v.is_null() {
                            return Ok(conform(v, env.static_type(expr)));
                        }
                    }
                    Ok(V::Null)
                }
                Operator::And | Operator::Or => {
                    let mut unknown = false;
                    for a in args {
                        match eval(a, env)? {
                            V::Bool(false) if op == Operator::And => return Ok(V::Bool(false)),
                            V::Bool(true) if op == Operator::Or => return Ok(V::Bool(true)),
                            V::Bool(_) => {}
                            V::Null => unknown = true,
                            other => return Err(mismatch(op, &other)),
                        }
                    }
                    Ok(if unknown {
                        V::Null
                    } else {
                        V::Bool(op == Operator::And)
                    })
                }
                Operator::IsNull => Ok(V::Bool(eval(&args[0], env)?.is_null())),
                Operator::Not => match eval(&args[0], env)? {
                    V::Bool(b) => Ok(V::Bool(!b)),
                    V::Null => Ok(V::Null),
                    other => Err(mismatch(op, &other)),
                },
                Operator::Equal
                | Operator::NotEqual
                | Operator::LessThan
                | Operator::LessOrEqual
                | Operator::GreaterThan
                | Operator::GreaterOrEqual => {
                    // Null-intolerant: a null left operand yields null without evaluating the right one.
                    let a = eval(&args[0], env)?;
                    if a.is_null() {
                        return Ok(V::Null);
                    }
                    let b = eval(&args[1], env)?;
                    if b.is_null() {
                        return Ok(V::Null);
                    }
                    let ord = compare(&a, &b).ok_or_else(|| {
                        EvalError::new(
                            "TYPE_MISMATCH",
                            "SEM-TRANS-002",
                            format!("Cannot compare {} with {}", a.type_name(), b.type_name()),
                        )
                    })?;
                    Ok(V::Bool(match op {
                        Operator::Equal => ord == Ordering::Equal,
                        Operator::NotEqual => ord != Ordering::Equal,
                        Operator::LessThan => ord == Ordering::Less,
                        Operator::LessOrEqual => ord != Ordering::Greater,
                        Operator::GreaterThan => ord == Ordering::Greater,
                        _ => ord != Ordering::Less,
                    }))
                }
                Operator::Absolute => match eval(&args[0], env)? {
                    V::Null => Ok(V::Null),
                    V::Int(i) => i.checked_abs().map(V::Int).ok_or_else(|| overflow(op)),
                    V::Num(f) => finite(f.abs(), op),
                    other => Err(mismatch(op, &other)),
                },
                Operator::Add
                | Operator::Subtract
                | Operator::Multiply
                | Operator::Divide
                | Operator::SafeDivide => {
                    // Null-intolerant and left-to-right: a null left operand short-circuits.
                    let a = eval(&args[0], env)?;
                    if !matches!(a, V::Null | V::Int(_) | V::Num(_)) {
                        return Err(mismatch(op, &a));
                    }
                    if a.is_null() {
                        return Ok(V::Null);
                    }
                    let b = eval(&args[1], env)?;
                    if !matches!(b, V::Null | V::Int(_) | V::Num(_)) {
                        return Err(mismatch(op, &b));
                    }
                    if b.is_null() {
                        return Ok(V::Null);
                    }
                    let widen = env.static_type(expr) == Some(LogicalType::Number);
                    let (a, b) = if widen {
                        (
                            conform(a, Some(LogicalType::Number)),
                            conform(b, Some(LogicalType::Number)),
                        )
                    } else {
                        (a, b)
                    };
                    match op {
                        Operator::Divide | Operator::SafeDivide => {
                            if as_f64(&b) == 0.0 {
                                return if op == Operator::SafeDivide {
                                    Ok(V::Null)
                                } else {
                                    Err(EvalError::new(
                                        "DIVISION_BY_ZERO",
                                        "SEM-TRANS-006",
                                        "Division by zero",
                                    ))
                                };
                            }
                            finite(as_f64(&a) / as_f64(&b), op)
                        }
                        _ => match (&a, &b) {
                            (V::Int(x), V::Int(y)) => match op {
                                Operator::Add => x.checked_add(*y),
                                Operator::Subtract => x.checked_sub(*y),
                                _ => x.checked_mul(*y),
                            }
                            .map(V::Int)
                            .ok_or_else(|| overflow(op)),
                            _ => {
                                let (x, y) = (as_f64(&a), as_f64(&b));
                                finite(
                                    match op {
                                        Operator::Add => x + y,
                                        Operator::Subtract => x - y,
                                        _ => x * y,
                                    },
                                    op,
                                )
                            }
                        },
                    }
                }
            }
        }
    }
}

/// Truthiness for filters: only `true` retains a row (SEM-TRANS-003).
pub fn passes(v: &V) -> bool {
    matches!(v, V::Bool(true))
}

/// Widen an integer to number when the static result type is number.
pub fn conform(v: V, ty: Option<LogicalType>) -> V {
    match (v, ty) {
        (V::Int(i), Some(LogicalType::Number)) => V::Num(i as f64),
        (v, _) => v,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct Env;
    impl ValueEnv for Env {
        fn column(&self, _: &str, c: &str) -> Result<V, EvalError> {
            Ok(match c {
                "n" => V::Null,
                "zero" => V::Int(0),
                "max" => V::Int(i64::MAX),
                "t" => V::Bool(true),
                "f" => V::Bool(false),
                _ => V::Int(1),
            })
        }
        fn feature(&self, _: &str, _: &str) -> Result<V, EvalError> {
            Ok(V::Null)
        }
    }

    fn e(v: serde_json::Value) -> Expr {
        serde_json::from_value(v).unwrap()
    }
    fn col(c: &str) -> serde_json::Value {
        json!({"kind":"column","input":"a","column":c})
    }
    fn call(op: &str, args: Vec<serde_json::Value>) -> serde_json::Value {
        json!({"kind":"call","operator":op,"args":args})
    }

    #[test]
    fn three_valued_logic() {
        assert_eq!(
            eval(&e(call("and", vec![col("f"), col("n")])), &Env).unwrap(),
            V::Bool(false)
        );
        assert_eq!(
            eval(&e(call("and", vec![col("t"), col("n")])), &Env).unwrap(),
            V::Null
        );
        assert_eq!(
            eval(&e(call("or", vec![col("t"), col("n")])), &Env).unwrap(),
            V::Bool(true)
        );
        assert_eq!(
            eval(&e(call("or", vec![col("f"), col("n")])), &Env).unwrap(),
            V::Null
        );
        assert_eq!(
            eval(&e(call("not", vec![col("n")])), &Env).unwrap(),
            V::Null
        );
        assert_eq!(
            eval(&e(call("equal", vec![col("n"), col("n")])), &Env).unwrap(),
            V::Null
        );
        assert_eq!(
            eval(&e(call("is_null", vec![col("n")])), &Env).unwrap(),
            V::Bool(true)
        );
    }

    #[test]
    fn laziness_prevents_irrelevant_errors() {
        let div0 = call("divide", vec![col("one"), col("zero")]);
        assert_eq!(
            eval(
                &e(call("if_else", vec![col("t"), col("one"), div0.clone()])),
                &Env
            )
            .unwrap(),
            V::Int(1)
        );
        assert_eq!(
            eval(
                &e(call("if_else", vec![col("n"), div0.clone(), col("one")])),
                &Env
            )
            .unwrap(),
            V::Int(1)
        );
        assert_eq!(
            eval(&e(call("coalesce", vec![col("one"), div0.clone()])), &Env).unwrap(),
            V::Int(1)
        );
        assert_eq!(
            eval(
                &e(call(
                    "and",
                    vec![col("f"), call("equal", vec![div0.clone(), col("one")])]
                )),
                &Env
            )
            .unwrap(),
            V::Bool(false)
        );
        assert_eq!(
            eval(&e(div0), &Env).map_err(|x| x.code),
            Err("DIVISION_BY_ZERO".into())
        );
    }

    fn eval1(v: serde_json::Value) -> Result<V, String> {
        eval(&e(v), &Env).map_err(|x| x.code)
    }

    #[test]
    fn numeric_rules() {
        assert_eq!(
            eval1(call("safe_divide", vec![col("one"), col("zero")])),
            Ok(V::Null)
        );
        assert_eq!(
            eval1(call("divide", vec![col("one"), col("one")])),
            Ok(V::Num(1.0))
        );
        assert_eq!(
            eval1(call("add", vec![col("max"), col("one")])),
            Err("NUMERIC_OVERFLOW".into())
        );
        assert_eq!(eval1(call("add", vec![col("n"), col("one")])), Ok(V::Null));
        assert_eq!(
            eval1(call(
                "absolute",
                vec![json!({"kind":"literal","type":"integer","value":i64::MIN})]
            )),
            Err("NUMERIC_OVERFLOW".into())
        );
        assert_eq!(
            eval1(call(
                "multiply",
                vec![
                    json!({"kind":"literal","type":"number","value":1e308}),
                    json!({"kind":"literal","type":"number","value":10.0})
                ]
            )),
            Err("NON_FINITE_RESULT".into())
        );
        assert_eq!(
            eval1(call(
                "add",
                vec![
                    col("one"),
                    json!({"kind":"literal","type":"number","value":0.5})
                ]
            )),
            Ok(V::Num(1.5))
        );
    }
}

#[cfg(test)]
mod widening_tests {
    use super::*;
    use serde_json::json;

    struct Mixed;
    impl ValueEnv for Mixed {
        fn column(&self, _: &str, c: &str) -> Result<V, EvalError> {
            Ok(if c == "i" { V::Int(1) } else { V::Null })
        }
        fn feature(&self, _: &str, _: &str) -> Result<V, EvalError> {
            Ok(V::Null)
        }
        fn static_type(&self, e: &Expr) -> Option<LogicalType> {
            match e {
                Expr::Call {
                    operator: Operator::Coalesce | Operator::Add,
                    ..
                } => Some(LogicalType::Number),
                _ => None,
            }
        }
    }

    #[test]
    fn coalesce_and_arithmetic_widen_to_unified_number_type() {
        let co: Expr = serde_json::from_value(json!({"kind":"call","operator":"coalesce","args":[{"kind":"column","input":"a","column":"i"},{"kind":"column","input":"a","column":"n"}]})).unwrap();
        assert_eq!(eval(&co, &Mixed).unwrap(), V::Num(1.0));
        let add: Expr = serde_json::from_value(json!({"kind":"call","operator":"add","args":[{"kind":"column","input":"a","column":"i"},{"kind":"literal","type":"integer","value":i64::MAX}]})).unwrap();
        assert!(matches!(eval(&add, &Mixed).unwrap(), V::Num(_)));
    }
}
