//! Label construction goldens (SEM-LABEL-001/002), reasoned by hand.
//!
//! Contract (example 03, consumer spend_training_dataset): label = sum of
//! amount_usd_cents over purchases with purchased_at in [T, T + 7 days),
//! reconstructed as available at the dataset cutoff C; every status counts
//! (the feature's settled-only filter must not leak into the label); a row is
//! kept only if T + 7d <= C and completeness evidence covers [T, T + 7d);
//! known-complete empty horizon → 0 (state empty); incomplete → excluded.

use featlas_core::eval::Limits;
use featlas_core::label::{build_labels, LabelInput};
use featlas_core::load::load_path;
use serde_json::{json, Value};
use std::path::PathBuf;

fn project() -> featlas_core::model::Project {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../spec/feature-engineering-spec-v0.2.0/examples/03_commerce_ratio.yaml");
    load_path(&path).unwrap().0.unwrap().project
}

fn purchase(id: &str, tenant: &str, at: &str, available: &str, amount: i64, status: &str) -> Value {
    json!({"transaction_id": id, "tenant_id": tenant, "customer_id": "customer_1", "purchased_at": at, "available_at": available, "amount_usd_cents": amount, "status": status})
}

fn request(tenant: &str, as_of: &str, cutoff: &str) -> Value {
    json!({"tenant_id": tenant, "customer_id": "customer_1", "as_of": as_of, "requested_at": cutoff, "dataset_cutoff": cutoff})
}

fn build(
    rows: Vec<Value>,
    source_rows: Vec<Value>,
    coverage_end: &str,
) -> Result<featlas_core::label::LabelDataset, String> {
    let input: LabelInput = serde_json::from_value(json!({
        "rows": rows,
        "source_rows": source_rows,
        "source_status": {"source": "purchase_events", "state": "available", "coverage_start": "2026-01-01T00:00:00Z", "coverage_end": coverage_end}
    }))
    .unwrap();
    build_labels(
        &project(),
        "spend_training_dataset",
        &input,
        &Limits::default(),
    )
    .map_err(|e| e.code)
}

const T: &str = "2026-06-01T00:00:00Z";

fn history() -> Vec<Value> {
    vec![
        purchase(
            "at_lower",
            "tenant_a",
            "2026-06-01T00:00:00Z",
            "2026-06-01T00:00:00Z",
            100,
            "settled",
        ),
        purchase(
            "refund",
            "tenant_a",
            "2026-06-02T00:00:00Z",
            "2026-06-02T00:00:00Z",
            -30,
            "cancelled",
        ),
        purchase(
            "at_upper",
            "tenant_a",
            "2026-06-08T00:00:00Z",
            "2026-06-08T00:00:00Z",
            200,
            "settled",
        ),
        purchase(
            "before",
            "tenant_a",
            "2026-05-31T23:59:59.999999Z",
            "2026-05-31T23:59:59.999999Z",
            400,
            "settled",
        ),
        purchase(
            "late",
            "tenant_a",
            "2026-06-03T00:00:00Z",
            "2026-06-11T00:00:00.000001Z",
            1000,
            "settled",
        ),
        purchase(
            "other_tenant",
            "tenant_b",
            "2026-06-02T00:00:00Z",
            "2026-06-02T00:00:00Z",
            7,
            "settled",
        ),
    ]
}

#[test]
fn mature_complete_horizon_sums_every_status_inside_bounds() {
    // Counted: at_lower (100, inclusive lower) + refund (-30, cancelled still counts) = 70.
    // Excluded: at_upper (exclusive upper), before (outside), late (available after C), tenant_b row.
    let ds = build(
        vec![request("tenant_a", T, "2026-06-11T00:00:00Z")],
        history(),
        "2026-06-11T00:00:00Z",
    )
    .unwrap();
    assert_eq!(ds.rows.len(), 1);
    assert_eq!(ds.rows[0].value, json!(70));
    assert_eq!(ds.rows[0].state, "ok");
    assert_eq!(ds.rows[0].eligible_rows, 2);
}

#[test]
fn late_label_availability_changes_the_as_known_label() {
    // With a later cutoff the late purchase (available 2026-06-11T00:00:00.000001Z) becomes knowable: 70 + 1000.
    let ds = build(
        vec![request("tenant_a", T, "2026-06-12T00:00:00Z")],
        history(),
        "2026-06-12T00:00:00Z",
    )
    .unwrap();
    assert_eq!(ds.rows[0].value, json!(1070));
}

#[test]
fn immature_horizon_is_excluded_not_negative() {
    // Horizon ends 2026-06-08; a cutoff of 2026-06-07T23:59:59.999999Z is one microsecond immature.
    let ds = build(
        vec![request("tenant_a", T, "2026-06-07T23:59:59.999999Z")],
        history(),
        "2026-06-11T00:00:00Z",
    )
    .unwrap();
    assert!(ds.rows.is_empty());
    assert!(ds.excluded[0].reason.contains("immature"));
    // Exactly at maturity it is labelled.
    let ds = build(
        vec![request("tenant_a", T, "2026-06-08T00:00:00Z")],
        history(),
        "2026-06-11T00:00:00Z",
    )
    .unwrap();
    assert_eq!(ds.rows.len(), 1);
}

#[test]
fn censored_coverage_is_excluded_and_exact_end_is_enough_for_exclusive_upper() {
    let ds = build(
        vec![request("tenant_a", T, "2026-06-11T00:00:00Z")],
        history(),
        "2026-06-07T23:59:59.999999Z",
    )
    .unwrap();
    assert!(ds.rows.is_empty());
    assert!(ds.excluded[0].reason.contains("censored"));
    let ds = build(
        vec![request("tenant_a", T, "2026-06-11T00:00:00Z")],
        history(),
        "2026-06-08T00:00:00Z",
    )
    .unwrap();
    assert_eq!(ds.rows.len(), 1);
}

#[test]
fn known_complete_empty_horizon_uses_label_empty() {
    let ds = build(
        vec![request(
            "tenant_a",
            "2026-07-01T00:00:00Z",
            "2026-07-09T00:00:00Z",
        )],
        history(),
        "2026-07-09T00:00:00Z",
    )
    .unwrap();
    assert_eq!(ds.rows[0].value, json!(0));
    assert_eq!(ds.rows[0].state, "empty");
}

#[test]
fn tenant_isolation_in_labels() {
    let ds = build(
        vec![request("tenant_b", T, "2026-06-11T00:00:00Z")],
        history(),
        "2026-06-11T00:00:00Z",
    )
    .unwrap();
    assert_eq!(ds.rows[0].value, json!(7));
}

#[test]
fn unavailable_label_source_is_never_a_negative_label() {
    let input: LabelInput = serde_json::from_value(json!({"rows": [request("tenant_a", T, "2026-06-11T00:00:00Z")], "source_rows": history(), "source_status": {"source": "purchase_events", "state": "unavailable"}})).unwrap();
    let ds = build_labels(
        &project(),
        "spend_training_dataset",
        &input,
        &Limits::default(),
    )
    .unwrap();
    assert!(ds.rows.is_empty());
    assert_eq!(ds.excluded.len(), 1);
}

#[test]
fn conflicting_label_revisions_fail() {
    let mut rows = history();
    rows.push(purchase(
        "at_lower",
        "tenant_a",
        "2026-06-01T00:00:00Z",
        "2026-06-01T00:00:00Z",
        999,
        "settled",
    ));
    assert_eq!(
        build(
            vec![request("tenant_a", T, "2026-06-11T00:00:00Z")],
            rows,
            "2026-06-11T00:00:00Z"
        )
        .unwrap_err(),
        "DUPLICATE_CONFLICT"
    );
}
