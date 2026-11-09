//! Decisive negative cases: each mutation of a valid example must be rejected
//! with the intended code at the intended stage (never "any exception").

use featlas_core::eval::{evaluate, Limits, Request};
use featlas_core::load::load_project;
use featlas_core::model::*;
use featlas_core::parse::Format;
use featlas_core::time::Instant;
use serde_json::{json, Value};
use std::path::PathBuf;

fn example_value(name: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../spec/feature-engineering-spec-v0.2.0/examples")
        .join(name);
    let bytes = std::fs::read(path).unwrap();
    featlas_core::parse::parse_bytes(&bytes, Format::Yaml)
        .unwrap()
        .value
}

fn codes_for(v: &Value) -> (Vec<String>, Vec<String>) {
    let text = serde_json::to_string(v).unwrap();
    let (_, report) = load_project(text.as_bytes(), Format::Json);
    let stages = report
        .stages
        .iter()
        .map(|s| format!("{:?}:{:?}", s.stage, s.status))
        .collect();
    (
        report
            .diagnostics
            .iter()
            .filter(|d| d.is_error())
            .map(|d| d.code.clone())
            .collect(),
        stages,
    )
}

fn expect(v: &Value, code: &str) {
    let (codes, stages) = codes_for(v);
    assert!(
        codes.iter().any(|c| c == code),
        "expected {code}, got {codes:?} (stages {stages:?})"
    );
}

fn mutate(name: &str, f: impl FnOnce(&mut Value)) -> Value {
    let mut v = example_value(name);
    f(&mut v);
    v
}

const FC: &str = "01_fault_count.yaml";

#[test]
fn schema_rejects_unknown_fields_and_misspellings() {
    expect(
        &mutate(FC, |v| v["features"][0]["unknown_field"] = json!(1)),
        "SCHEMA_INVALID",
    );
    expect(
        &mutate(FC, |v| {
            v["features"][0]["transform"]["operation"] = json!("sum_distinct")
        }),
        "SCHEMA_INVALID",
    );
    expect(
        &mutate(FC, |v| {
            v["features"][0]["time"]["window"]["lower"] = json!("Inclusive")
        }),
        "SCHEMA_INVALID",
    );
    expect(
        &mutate(FC, |v| {
            v["features"][0]["history"]["earliest_as_of"] = json!("2026-02-01 00:00:00")
        }),
        "SCHEMA_INVALID",
    );
}

#[test]
fn reference_and_identity_errors() {
    expect(
        &mutate(FC, |v| v["features"][0]["entity"] = json!("devices")),
        "UNKNOWN_ENTITY",
    );
    expect(
        &mutate(FC, |v| {
            v["feature_sets"][0]["features"][0]["version"] = json!("1.0.1")
        }),
        "VERSION_MISMATCH",
    );
    expect(
        &mutate(FC, |v| {
            v["consumers"][0]["feature_set"]["version"] = json!("2.0.0")
        }),
        "VERSION_MISMATCH",
    );
    expect(
        &mutate(FC, |v| {
            let s = v["sources"][0].clone();
            v["sources"].as_array_mut().unwrap().push(s);
        }),
        "DUPLICATE_ID",
    );
    expect(
        &mutate(FC, |v| {
            v["features"][0]["inputs"][0]["columns"]
                .as_array_mut()
                .unwrap()
                .push(json!("severity"))
        }),
        "UNKNOWN_COLUMN",
    );
    expect(
        &mutate(FC, |v| {
            v["features"][0]["inputs"][0]["columns"] = json!([
                "event_id",
                "tenant_id",
                "device_id",
                "occurred_at",
                "available_at"
            ]);
        }),
        "UNDECLARED_INPUT_COLUMN",
    );
}

#[test]
fn time_and_identity_column_contracts() {
    expect(
        &mutate(FC, |v| {
            v["sources"][1]["columns"][3]["nullable"] = json!(true)
        }),
        "INVALID_TIME_COLUMN",
    );
    expect(
        &mutate(FC, |v| {
            v["sources"][1]["columns"][0]["nullable"] = json!(true)
        }),
        "NULLABLE_IDENTITY",
    );
    expect(
        &mutate(FC, |v| {
            v["sources"][1]["retention"]["earliest_reproducible_at"] = json!("2026-02-30T00:00:00Z")
        }),
        "INVALID_TIMESTAMP",
    );
    expect(
        &mutate(FC, |v| {
            v["features"][0]["inputs"][0]["columns"] = json!([
                "event_id",
                "tenant_id",
                "device_id",
                "occurred_at",
                "is_fault"
            ])
        }),
        "MISSING_INPUT_COLUMN",
    );
}

#[test]
fn expression_typing_and_arity() {
    let bad_type = json!({"kind":"call","operator":"equal","args":[{"kind":"column","input":"faults","column":"is_fault"},{"kind":"literal","type":"integer","value":1}]});
    expect(
        &mutate(FC, |v| v["features"][0]["transform"]["filter"] = bad_type),
        "TYPE_MISMATCH",
    );
    let bad_arity = json!({"kind":"call","operator":"not","args":[{"kind":"column","input":"faults","column":"is_fault"},{"kind":"column","input":"faults","column":"is_fault"}]});
    expect(
        &mutate(FC, |v| v["features"][0]["transform"]["filter"] = bad_arity),
        "OPERATOR_ARITY",
    );
    let non_bool = json!({"kind":"column","input":"faults","column":"event_id"});
    expect(
        &mutate(FC, |v| v["features"][0]["transform"]["filter"] = non_bool),
        "TYPE_MISMATCH",
    );
    let lit = json!({"kind":"call","operator":"equal","args":[{"kind":"column","input":"faults","column":"is_fault"},{"kind":"literal","type":"boolean","value":"true"}]});
    expect(
        &mutate(FC, |v| v["features"][0]["transform"]["filter"] = lit),
        "SCHEMA_INVALID",
    );
    let feat = json!({"kind":"call","operator":"equal","args":[{"kind":"feature","feature":"device.fault_count_7d","version":"1.0.0"},{"kind":"literal","type":"integer","value":1}]});
    expect(
        &mutate(FC, |v| v["features"][0]["transform"]["filter"] = feat),
        "UNSUPPORTED_ROW_LEVEL_DEPENDENCY",
    );
}

#[test]
fn entity_grouping_must_include_tenant() {
    expect(
        &mutate(FC, |v| {
            v["features"][0]["transform"]["group_by"]
                .as_array_mut()
                .unwrap()
                .remove(0);
        }),
        "ENTITY_MAPPING_INCOMPLETE",
    );
    expect(
        &mutate(FC, |v| {
            v["consumers"][0]["entity_inputs"]
                .as_array_mut()
                .unwrap()
                .truncate(1)
        }),
        "ENTITY_MAPPING_INCOMPLETE",
    );
}

#[test]
fn lineage_must_cover_roles() {
    expect(
        &mutate(FC, |v| {
            v["features"][0]["lineage"]["sources"]
                .as_array_mut()
                .unwrap()
                .retain(|e| e["column"] != "available_at" || e["source"] != "fault_events");
        }),
        "MISSING_LINEAGE",
    );
    expect(
        &mutate(FC, |v| {
            v["features"][0]["lineage"]["sources"][5]["roles"] = json!(["value"])
        }),
        "MISSING_LINEAGE_ROLE",
    );
    expect(
        &mutate(FC, |v| {
            v["features"][0]["lineage"]["sources"][0]["column"] = json!("ghost")
        }),
        "UNKNOWN_COLUMN",
    );
}

#[test]
fn missingness_and_output_contracts() {
    expect(
        &mutate(FC, |v| {
            v["features"][0]["output"]["nullable"] = json!(false)
        }),
        "NULLABILITY_CONFLICT",
    );
    expect(
        &mutate(FC, |v| {
            v["features"][0]["missingness"]["empty"] = json!({"action":"value","value":"zero"})
        }),
        "TYPE_MISMATCH",
    );
    expect(
        &mutate(FC, |v| {
            v["features"][0]["missingness"]["null_input"] = json!("propagate")
        }),
        "NULL_POLICY_CONFLICT",
    );
    expect(
        &mutate(FC, |v| {
            v["features"][0]["history"]["required_seconds"] = json!(86400)
        }),
        "HISTORY_WINDOW_MISMATCH",
    );
    expect(
        &mutate(FC, |v| {
            v["features"][0]["output"]["type"] = json!("boolean")
        }),
        "SCHEMA_INVALID",
    );
}

#[test]
fn dependency_cycles_and_undeclared_dependencies() {
    let v = mutate("03_commerce_ratio.yaml", |v| {
        v["features"][0]["dependencies"] = json!([{"feature":"customer.recent_spend_ratio","version":"1.0.0","alignment":"same_entity_as_of"}]);
    });
    expect(&v, "DEPENDENCY_CYCLE");
    expect(
        &mutate("03_commerce_ratio.yaml", |v| {
            v["features"][2]["dependencies"]
                .as_array_mut()
                .unwrap()
                .truncate(1);
        }),
        "UNDECLARED_DEPENDENCY",
    );
    expect(
        &mutate("03_commerce_ratio.yaml", |v| {
            v["features"][2]["lineage"]["dependencies"]
                .as_array_mut()
                .unwrap()
                .truncate(1);
        }),
        "MISSING_LINEAGE",
    );
}

#[test]
fn relationship_contracts() {
    let t = "02_historical_temperature.yaml";
    expect(
        &mutate(t, |v| v["relationships"][0]["unmatched"] = json!("drop")),
        "JOIN_POLICY_CONFLICT",
    );
    expect(
        &mutate(t, |v| {
            v["relationships"][0]["from"]["columns"] = json!(["device_id"]);
            v["relationships"][0]["to"]["columns"] = json!(["device_id"]);
        }),
        "TENANT_JOIN_MISSING",
    );
    expect(
        &mutate(t, |v| {
            v["relationships"][0]["to"]["columns"] = json!(["tenant_id"])
        }),
        "RELATIONSHIP_ARITY",
    );
    expect(
        &mutate(t, |v| {
            v["sources"][2]["record_semantics"]["logical_key"] = json!(["tenant_id", "device_id"])
        }),
        "DIMENSION_INTERVAL_IDENTITY",
    );
}

#[test]
fn deployment_binding_contracts() {
    // Index 1 is fault_prod.
    expect(
        &mutate(FC, |v| {
            v["deployments"][1]["source_bindings"][1]["column_mapping"]
                .as_object_mut()
                .unwrap()
                .remove("is_fault")
                .map(|_| ())
                .unwrap()
        }),
        "SOURCE_COLUMN_MAPPING_INCOMPLETE",
    );
    expect(
        &mutate(FC, |v| {
            v["deployments"][1]["source_bindings"][1]["column_mapping"]["is_fault"] =
                json!("event_id")
        }),
        "PHYSICAL_COLUMN_COLLISION",
    );
    expect(
        &mutate(FC, |v| {
            v["deployments"][1]["source_bindings"][1]["version"] = json!("2.0.0")
        }),
        "VERSION_MISMATCH",
    );
    expect(
        &mutate(FC, |v| {
            v["deployments"][1]["source_bindings"]
                .as_array_mut()
                .unwrap()
                .truncate(1)
        }),
        "MISSING_SOURCE_BINDING",
    );
    expect(
        &mutate(FC, |v| {
            v["deployments"][1]["output_targets"][0]["feature_mapping"][0]["state_column"] =
                json!("tenant_id")
        }),
        "PHYSICAL_COLUMN_COLLISION",
    );
    expect(
        &mutate(FC, |v| {
            v["deployments"][1]["output_targets"][0]["entity_key_mapping"]
                .as_object_mut()
                .unwrap()
                .remove("tenant_id")
                .map(|_| ())
                .unwrap()
        }),
        "OUTPUT_ENTITY_MAPPING_INCOMPLETE",
    );
    expect(
        &mutate(FC, |v| {
            v["deployments"][1]["output_targets"][0]["location"] =
                json!({"kind":"fixture","suite":"fault_count_conformance"})
        }),
        "OUTPUT_LOCATION_READ_ONLY",
    );
    expect(
        &mutate(FC, |v| {
            v["deployments"][1]["execution"]["orchestrator"] = Value::Null
        }),
        "ORCHESTRATOR_REQUIRED",
    );
    expect(
        &mutate(FC, |v| {
            v["deployments"][1]["adapters"][0]["roles"] =
                json!(["compute", "catalog", "orchestrator"])
        }),
        "ADAPTER_ROLE_MISMATCH",
    );
    expect(
        &mutate(FC, |v| {
            v["deployments"][1]["consumer_bindings"][0]["role"] = json!("online_store")
        }),
        "CONSUMER_SURFACE_MISMATCH",
    );
    expect(
        &mutate(FC, |v| {
            v["deployments"][1]["publication"]["surface"] = json!("online_store")
        }),
        "PUBLICATION_SURFACE_UNBOUND",
    );
    expect(
        &mutate(FC, |v| {
            v["deployments"][1]["adapters"][0]["configuration"]["api_token"] = json!("abc")
        }),
        "CREDENTIAL_IN_CONFIGURATION",
    );
    expect(
        &mutate(FC, |v| {
            let t = v["deployments"][1]["output_targets"][0].clone();
            let mut t2 = t.clone();
            t2["location"]["format"] = json!("parquet");
            t2["role"] = json!("offline_store");
            v["deployments"][1]["output_targets"]
                .as_array_mut()
                .unwrap()
                .push(t2);
        }),
        "DUPLICATE_OUTPUT_TARGET",
    );
}

#[test]
fn native_policy() {
    expect(
        &mutate("04_fitted_and_custom_contracts.yaml", |v| {
            v["deployments"][0]["portability"]["mode"] = json!("portable")
        }),
        "NATIVE_MODE_REQUIRED",
    );
    expect(
        &mutate("04_fitted_and_custom_contracts.yaml", |v| {
            v["deployments"][0]["portability"]["native_bindings"][0]["conformance_suite"] =
                json!("fitted_core_conformance")
        }),
        "NATIVE_SUITE_FEATURE_MISMATCH",
    );
    expect(
        &mutate("04_fitted_and_custom_contracts.yaml", |v| {
            v["deployments"][0]["portability"]["native_bindings"][0]["required_capabilities"] =
                json!(["custom.other.v1"])
        }),
        "NATIVE_CAPABILITY_UNDECLARED",
    );
}

#[test]
fn fixture_contracts() {
    expect(
        &mutate(FC, |v| {
            v["acceptance_suites"][0]["cases"][0]["source_rows"]["fault_events"][0]["is_fault"] =
                json!("yes")
        }),
        "FIXTURE_TYPE_MISMATCH",
    );
    expect(
        &mutate(FC, |v| {
            v["acceptance_suites"][0]["cases"][0]["source_rows"]["fault_events"][0]
                .as_object_mut()
                .unwrap()
                .remove("is_fault")
                .map(|_| ())
                .unwrap()
        }),
        "FIXTURE_COLUMNS",
    );
    expect(
        &mutate(FC, |v| {
            v["acceptance_suites"][0]["cases"][0]["context"]["source_status"][0]["coverage_end"] =
                json!("2025-01-01T00:00:00Z")
        }),
        "INVALID_COVERAGE_INTERVAL",
    );
    expect(
        &mutate(FC, |v| {
            v["acceptance_suites"][0]["cases"][9]["context"]["published_values"][0]
                ["published_at"] = json!("2026-06-07T00:00:00Z")
        }),
        "PUBLICATION_BEFORE_AS_OF",
    );
}

/// Run one mutated fixture case through the evaluator and return the outcome code or rows.
fn eval_case(v: &Value, case_index: usize) -> Result<Vec<(Value, Value, String)>, String> {
    let text = serde_json::to_string(v).unwrap();
    let (loaded, report) = load_project(text.as_bytes(), Format::Json);
    let l = loaded.unwrap_or_else(|| panic!("{:?}", report.diagnostics));
    let p = &l.project;
    let case = &p.acceptance_suites[0].cases[case_index];
    let req = Request {
        as_of: Instant::parse(&case.as_of).unwrap(),
        source_rows: &case.source_rows,
        source_status: &case.context.source_status,
        expected_population: Some(&case.population_rows),
        mode: case.context.mode,
        requested_at: case
            .context
            .requested_at
            .as_deref()
            .map(|t| Instant::parse(t).unwrap()),
        published_values: case.context.published_values.as_deref(),
        consumer: case.context.consumer.as_deref(),
        capabilities: &case.context.capabilities,
    };
    evaluate(p, &case.feature.id, &req, &Limits::default())
        .map(|e| {
            e.rows
                .into_iter()
                .map(|r| (Value::Object(r.entity_keys), r.value, r.state))
                .collect()
        })
        .map_err(|e| e.code)
}

#[test]
fn temporal_boundary_metamorphic_checks() {
    // event_window_edges (case 0): moving the at_upper event one microsecond earlier makes it eligible.
    let v = mutate(FC, |v| {
        v["acceptance_suites"][0]["cases"][0]["source_rows"]["fault_events"][3]["occurred_at"] =
            json!("2026-06-07T23:59:59.999999Z")
    });
    let v = {
        let mut v = v;
        v["acceptance_suites"][0]["cases"][0]["source_rows"]["fault_events"][3]["available_at"] =
            json!("2026-06-07T23:59:59.999999Z");
        v
    };
    assert_eq!(eval_case(&v, 0).unwrap()[0].1, json!(3));
    // A future-available row must not affect an earlier result.
    let v = mutate(FC, |v| {
        v["acceptance_suites"][0]["cases"][0]["source_rows"]["fault_events"].as_array_mut().unwrap().push(json!({"event_id":"future","tenant_id":"tenant_a","device_id":"device_1","occurred_at":"2026-06-07T00:00:00Z","available_at":"2026-06-08T00:00:00.000001Z","is_fault":true}));
    });
    assert_eq!(eval_case(&v, 0).unwrap()[0].1, json!(2));
    // Adding another tenant's rows does not affect the first tenant.
    let v = mutate(FC, |v| {
        v["acceptance_suites"][0]["cases"][0]["source_rows"]["fault_events"].as_array_mut().unwrap().push(json!({"event_id":"other","tenant_id":"tenant_z","device_id":"device_1","occurred_at":"2026-06-07T00:00:00Z","available_at":"2026-06-07T00:00:00Z","is_fault":true}));
    });
    assert_eq!(eval_case(&v, 0).unwrap()[0].1, json!(2));
    // Coverage end exactly at T is insufficient for an exclusive-upper window? No: exclusive permits coverage_end = T.
    let v = mutate(FC, |v| {
        v["acceptance_suites"][0]["cases"][0]["context"]["source_status"][1]["coverage_end"] =
            json!("2026-06-08T00:00:00Z")
    });
    assert_eq!(eval_case(&v, 0).unwrap()[0].2, "ok");
    let v = mutate(FC, |v| {
        v["acceptance_suites"][0]["cases"][0]["context"]["source_status"][1]["coverage_end"] =
            json!("2026-06-07T23:59:59.999999Z")
    });
    assert_eq!(eval_case(&v, 0).unwrap()[0].2, "insufficient_history");
    // Coverage must start at or before T − W.
    let v = mutate(FC, |v| {
        v["acceptance_suites"][0]["cases"][0]["context"]["source_status"][1]["coverage_start"] =
            json!("2026-06-01T00:00:00.000001Z")
    });
    assert_eq!(eval_case(&v, 0).unwrap()[0].2, "insufficient_history");
    // Missing population status is an error, not an assumption.
    let v = mutate(FC, |v| {
        v["acceptance_suites"][0]["cases"][0]["context"]["source_status"]
            .as_array_mut()
            .unwrap()
            .remove(0)
            .as_null()
            .map(|_| ())
            .unwrap_or(())
    });
    assert_eq!(eval_case(&v, 0).unwrap_err(), "MISSING_SOURCE_STATUS");
    // Missing source rows (not even an empty list) is an error.
    let v = mutate(FC, |v| {
        v["acceptance_suites"][0]["cases"][0]["source_rows"]
            .as_object_mut()
            .unwrap()
            .remove("fault_events")
            .map(|_| ())
            .unwrap()
    });
    assert_eq!(eval_case(&v, 0).unwrap_err(), "MISSING_SOURCE_ROWS");
    // Fixture population disagreeing with source history.
    let v = mutate(FC, |v| {
        v["acceptance_suites"][0]["cases"][0]["population_rows"]
            .as_array_mut()
            .unwrap()
            .push(json!({"tenant_id":"tenant_b","device_id":"device_1"}))
    });
    assert_eq!(eval_case(&v, 0).unwrap_err(), "POPULATION_MISMATCH");
    // A device registered after T is not in the population.
    let v = mutate(FC, |v| {
        v["acceptance_suites"][0]["cases"][0]["source_rows"]["device_registry"][0]["available_at"] =
            json!("2026-06-08T00:00:01Z")
    });
    assert_eq!(eval_case(&v, 0).unwrap_err(), "POPULATION_MISMATCH");
    // Same-tenant/device duplicate registration with differing timestamps conflicts.
    let v = mutate(FC, |v| {
        v["acceptance_suites"][0]["cases"][0]["source_rows"]["device_registry"].as_array_mut().unwrap().push(json!({"tenant_id":"tenant_a","device_id":"device_1","registered_at":"2026-01-02T00:00:00Z","available_at":"2026-01-02T00:00:00Z"}));
    });
    assert_eq!(eval_case(&v, 0).unwrap_err(), "DUPLICATE_CONFLICT");
}

#[test]
fn replay_selection_rules() {
    // publication_visibility (case 9): requested_at after the newer publication selects it.
    let v = mutate(FC, |v| {
        v["acceptance_suites"][0]["cases"][9]["context"]["requested_at"] =
            json!("2026-06-08T00:01:30Z")
    });
    assert_eq!(eval_case(&v, 9).unwrap()[0].1, json!(3));
    // One microsecond earlier it is invisible.
    let v = mutate(FC, |v| {
        v["acceptance_suites"][0]["cases"][9]["context"]["requested_at"] =
            json!("2026-06-08T00:01:29.999999Z")
    });
    assert_eq!(eval_case(&v, 9).unwrap()[0].1, json!(2));
    // No eligible publication: not_published, never a recomputation.
    let v = mutate(FC, |v| {
        v["acceptance_suites"][0]["cases"][9]["context"]["requested_at"] =
            json!("2026-06-08T00:00:00Z")
    });
    assert_eq!(eval_case(&v, 9).unwrap()[0].2, "not_published");
    // Staleness boundary: exactly max_staleness is fresh, one microsecond more is stale.
    let v = mutate(FC, |v| {
        v["acceptance_suites"][0]["cases"][11]["context"]["published_values"][0]["as_of"] =
            json!("2026-06-07T23:00:00Z")
    });
    assert_eq!(eval_case(&v, 11).unwrap()[0].1, json!(4));
    let v = mutate(FC, |v| {
        v["acceptance_suites"][0]["cases"][11]["context"]["published_values"][0]["as_of"] =
            json!("2026-06-07T22:59:59.999999Z")
    });
    assert_eq!(eval_case(&v, 11).unwrap_err(), "STALE_FEATURE");
    // Reject policy refuses a selected null without fallback.
    let v = mutate(FC, |v| {
        v["consumers"][1]["missing_value_policy"] = json!("reject")
    });
    assert_eq!(eval_case(&v, 10).unwrap_err(), "MISSING_FEATURE_VALUE");
}

#[test]
fn restricted_yaml_rejects_before_schema() {
    let base = std::fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../spec/feature-engineering-spec-v0.2.0/examples/00_minimal_template.yaml"),
    )
    .unwrap();
    let cases = [
        (
            base.replace(
                "spec_version: 0.2.0",
                "spec_version: 0.2.0\nspec_version: 0.2.0",
            ),
            "PARSE_DUPLICATE_KEY",
        ),
        (
            base.replace(
                "earliest_as_of: '2026-01-01T00:01:00Z'",
                "earliest_as_of: 2026-01-01T00:01:00Z",
            ),
            "PARSE_IMPLICIT_TIMESTAMP",
        ),
        (format!("{base}\n---\n{{}}\n"), "PARSE_MULTIPLE_DOCUMENTS"),
        (
            base.replacen(
                "kind: FeatureEngineeringProject",
                "kind: !!str FeatureEngineeringProject",
                1,
            ),
            "PARSE_TAG",
        ),
    ];
    for (text, code) in cases {
        let (_, report) = load_project(text.as_bytes(), Format::Yaml);
        assert_eq!(report.diagnostics[0].code, code);
        assert_eq!(
            report.stages[1].status,
            featlas_core::load::StageStatus::NotEvaluated
        );
    }
}

#[test]
fn model_round_trip_is_lossless() {
    for name in [
        "01_fault_count.yaml",
        "02_historical_temperature.yaml",
        "04_fitted_and_custom_contracts.yaml",
    ] {
        let v = example_value(name);
        let p: Project = serde_json::from_value(v.clone()).unwrap();
        // Number-typed contract fields are binary64: 0 and 0.0 are the same JSON Schema number.
        assert_eq!(
            normalize(&serde_json::to_value(&p).unwrap()),
            normalize(&v),
            "{name}"
        );
    }
}

fn normalize(v: &Value) -> Value {
    match v {
        Value::Number(n) => json!(n.as_f64().unwrap()),
        Value::Array(a) => Value::Array(a.iter().map(normalize).collect()),
        Value::Object(m) => {
            Value::Object(m.iter().map(|(k, v)| (k.clone(), normalize(v))).collect())
        }
        other => other.clone(),
    }
}

// ---- Regressions for independent-review findings ----

fn eval_any(
    v: &Value,
    suite: usize,
    case_index: usize,
) -> Result<Vec<(Value, Value, String)>, String> {
    let mut v = v.clone();
    let s = v["acceptance_suites"][suite].clone();
    v["acceptance_suites"] = json!([s]);
    eval_case(&v, case_index)
}

#[test]
fn review_integer_result_widens_to_number_output() {
    let v = mutate("00_minimal_template.yaml", |v| {
        v["features"][0]["output"]["type"] = json!("number");
        v["acceptance_suites"][0]["cases"][0]["source_rows"]["fault_events"] = json!([{"event_id":"e1","tenant_id":"tenant_a","device_id":"device_1","occurred_at":"2026-06-07T23:59:30Z","available_at":"2026-06-07T23:59:30Z","is_fault":true}]);
    });
    let rows = eval_any(&v, 0, 0).unwrap();
    assert_eq!(rows[0].1, json!(1.0));
    assert_eq!(rows[0].2, "ok");
}

#[test]
fn review_insufficient_coverage_skips_join_errors() {
    // overlapping intervals would raise MULTIPLE_MATCHES, but history is not established.
    let v = mutate("02_historical_temperature.yaml", |v| {
        v["acceptance_suites"][0]["cases"][6]["context"]["source_status"][1]["coverage_start"] =
            json!("2026-06-07T12:00:00Z")
    });
    let rows = eval_any(&v, 0, 6).unwrap();
    assert_eq!(rows[0].1, Value::Null);
    assert_eq!(rows[0].2, "insufficient_history");
}

#[test]
fn review_label_contract_checks() {
    expect(
        &mutate("03_commerce_ratio.yaml", |v| {
            v["consumers"][0]["label"]["entity_keys"][1]["column"] = json!("amount_usd_cents")
        }),
        "ENTITY_KEY_TYPE",
    );
    expect(
        &mutate("03_commerce_ratio.yaml", |v| {
            v["consumers"][0]["label"]["available_time_column"] = json!("purchased_at")
        }),
        "LABEL_AVAILABILITY_COLUMN",
    );
}

#[test]
fn review_recency_must_use_window_event_time() {
    expect(
        &mutate("05_lookup_and_recency.yaml", |v| {
            v["features"][0]["transform"]["event_time"] = json!("available_at")
        }),
        "RECENCY_EVENT_TIME",
    );
}

#[test]
fn review_parser_rejects_directives_and_underflow() {
    let (_, r) = load_project(b"%YAML 1.1\n---\na: 1\n", Format::Yaml);
    assert_eq!(r.diagnostics[0].code, "PARSE_DIRECTIVE");
    let (_, r) = load_project(b"a: 1e-400\n", Format::Yaml);
    assert_eq!(r.diagnostics[0].code, "PARSE_NUMBER_RANGE");
}

/// Rename a column of one source throughout the document (contract, references, fixtures).
fn rename_source_column(v: &mut Value, source: &str, from: &str, to: &str) {
    fn rename_keys(v: &mut Value, from: &str, to: &str) {
        if let Some(m) = v.as_object_mut() {
            if let Some(x) = m.remove(from) {
                m.insert(to.to_string(), x);
            }
        }
    }
    fn rename_in_list(v: &mut Value, from: &str, to: &str) {
        if let Some(a) = v.as_array_mut() {
            for x in a.iter_mut() {
                if x == from {
                    *x = json!(to);
                }
            }
        }
    }
    let si = v["sources"]
        .as_array()
        .unwrap()
        .iter()
        .position(|s| s["id"] == source)
        .unwrap();
    let s = &mut v["sources"][si];
    for c in s["columns"].as_array_mut().unwrap() {
        if c["name"] == from {
            c["name"] = json!(to);
        }
    }
    rename_in_list(&mut s["primary_key"], from, to);
    rename_in_list(&mut s["record_semantics"]["logical_key"], from, to);
    for u in s["lineage"]["upstream_columns"].as_array_mut().unwrap() {
        if u["column"] == from {
            u["column"] = json!(to);
        }
    }
    for r in v["relationships"].as_array_mut().unwrap() {
        for side in ["from", "to"] {
            if r[side]["source"] == source {
                rename_in_list(&mut r[side]["columns"], from, to);
            }
        }
    }
    for f in v["features"].as_array_mut().unwrap() {
        for i in f["inputs"].as_array_mut().unwrap() {
            if i["source"] == source {
                rename_in_list(&mut i["columns"], from, to);
            }
        }
        for l in f["lineage"]["sources"].as_array_mut().unwrap() {
            if l["source"] == source && l["column"] == from {
                l["column"] = json!(to);
            }
        }
    }
    for d in v["deployments"].as_array_mut().unwrap() {
        for b in d["source_bindings"].as_array_mut().unwrap() {
            if b["source"] == source {
                rename_keys(&mut b["column_mapping"], from, to);
            }
        }
    }
    for suite in v["acceptance_suites"].as_array_mut().unwrap() {
        for c in suite["cases"].as_array_mut().unwrap() {
            if let Some(rows) = c["source_rows"][source].as_array_mut() {
                for r in rows {
                    rename_keys(r, from, to);
                }
            }
        }
    }
}

#[test]
fn review_tenant_join_detected_without_shared_column_names() {
    let mut v = example_value("02_historical_temperature.yaml");
    rename_source_column(&mut v, "device_threshold_history", "tenant_id", "org_id");
    // Rename-only: the project stays valid and every golden still holds.
    let (codes, _) = codes_for(&v);
    assert!(codes.is_empty(), "{codes:?}");
    let text = serde_json::to_string(&v).unwrap();
    let (l, _) = load_project(text.as_bytes(), Format::Json);
    let s =
        featlas_core::fixtures::run_suites(&l.unwrap().project, None, &Limits::default(), false);
    assert_eq!((s.passed, s.failed), (9, 0));
    // Joining on device_id alone now crosses tenants; the name-based check cannot see it.
    let mut bad = v.clone();
    bad["relationships"][0]["from"]["columns"] = json!(["device_id"]);
    bad["relationships"][0]["to"]["columns"] = json!(["device_id"]);
    expect(&bad, "TENANT_JOIN_MISSING");
    // Limitation: pairing the left tenant column with a *non-tenant* dimension column of the same
    // type cannot be detected, because the language has no column-level tenant marker.
}
