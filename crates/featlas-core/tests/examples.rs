use featlas_core::load::load_path;
use std::path::PathBuf;

pub fn example(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../spec/feature-engineering-spec-v0.2.0/examples").join(name)
}

pub const EXAMPLES: &[&str] = &[
    "00_minimal_template.yaml",
    "01_fault_count.yaml",
    "02_historical_temperature.yaml",
    "03_commerce_ratio.yaml",
    "04_fitted_and_custom_contracts.yaml",
    "05_lookup_and_recency.yaml",
    "06_store_migration.yaml",
];

#[test]
fn all_examples_pass_parse_schema_and_references() {
    let mut failures = Vec::new();
    for name in EXAMPLES {
        let (_, report) = load_path(&example(name)).unwrap();
        for d in &report.diagnostics {
            println!("{name}: {d}");
        }
        if !report.passed() {
            failures.push(*name);
        }
    }
    assert!(failures.is_empty(), "failed: {failures:?}");
}

#[test]
fn reference_fixtures_match_goldens() {
    use featlas_core::fixtures::{run_suites, CaseStatus};
    let mut failed = Vec::new();
    let (mut passed, mut skipped) = (0, 0);
    for name in EXAMPLES {
        let (loaded, _) = load_path(&example(name)).unwrap();
        let s = run_suites(&loaded.unwrap().project, None, &Default::default(), false);
        for c in &s.cases {
            match c.status {
                CaseStatus::Passed => passed += 1,
                CaseStatus::NotExecuted => { skipped += 1; println!("{name} {} NOT EXECUTED: {}", c.case, c.reason) }
                CaseStatus::Failed => { println!("{name} {} FAILED: {}", c.case, c.reason); failed.push(format!("{name}:{}", c.case)) }
            }
        }
    }
    println!("passed={passed} not_executed={skipped} failed={}", failed.len());
    assert!(failed.is_empty(), "{failed:?}");
    assert_eq!(passed, 50);
    assert_eq!(skipped, 3);
}
