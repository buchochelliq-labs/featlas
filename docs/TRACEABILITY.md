# SEM-* requirement → implementation → test matrix

Status: **impl+test** (implemented, with decisive tests), **impl** (implemented, covered only
indirectly), **partial**, **unsupported** (explicitly rejected), **open** (needs live target/runtime
evidence; not satisfiable locally). Test names refer to `crates/*/tests/*.rs` and
`python/tests/test_local_spark_differential.py` (LSD).

| Rule | Status | Implementation | Tests / evidence |
| --- | --- | --- | --- |
| SEM-CONF-001 | impl+test | `load.rs` stage report; `fixtures.rs` passed/failed/not_executed; reports carry scope/limitations | `validate_and_test_examples`, `restricted_yaml_rejects_before_schema` |
| SEM-CONF-002 | impl+test | stages gated; planning blocks generation; extensions never read | `generate_writes_manifest_and_refuses_unsupported` |
| SEM-PARSE-001 | impl+test | `parse.rs` | `parse::tests::*`, `review_parser_rejects_directives_and_underflow` |
| SEM-ID-001/002 | impl+test | `validate.rs::check_ids`, reference checks | `reference_and_identity_errors` |
| SEM-ID-003 | impl+test | cycles, declared/used dependencies, row-level feature refs rejected | `dependency_cycles_and_undeclared_dependencies`, `expression_typing_and_arity` |
| SEM-ID-004 | impl+test | `canon.rs` identity scopes | `logical_fingerprints_survive_rebinding_and_adapter_repinning` |
| SEM-SRC-001 | impl+test | column/time/revision/tombstone checks; row validation; Databricks `expected_source_schemas` | `time_and_identity_column_contracts`, `fixture_contracts` |
| SEM-SRC-002 | impl+test | PK vs logical key in reconstruction; tenant joins | goldens (01, 02) |
| SEM-SRC-003 | impl (local) / open (live) | assertions generated as zero-row SQL checks; no reliance on UC constraints | LSD duplicate/overlap cases; live: runbook |
| SEM-SRC-004 | partial | lineage upstream checks; Delta `VERSION AS OF` pinning in host | live: open |
| SEM-ENT-001 | impl+test | population reconstruction + fixture comparison | `temporal_boundary_metamorphic_checks` (population mismatch/late registration) |
| SEM-ENT-002 | impl+test | tenant keys required in grouping, joins, consumers, labels, publications | `tenant_isolation`, `join_tenant_isolation`, `derived_tenant_isolation`, `tenant_isolation_in_labels`, `review_tenant_join_detected_without_shared_column_names`; residual limit in findings #7 |
| SEM-REL-001..003 | impl+test | validation + evaluator + Spark checks | `relationship_contracts`, 02 goldens, LSD |
| SEM-REL-004 | impl+test | availability-aware valid-time join | `historical_join_and_late_revision`, `dimension_boundary_correction` (core + LSD) |
| SEM-TIME-001/002 | impl+test | windows, availability cutoff, µs boundaries | `event_window_edges`, `availability_edges`, metamorphic µs tests |
| SEM-TIME-003 | impl+test | `time.rs` | `time::tests::*`, `INVALID_TIMESTAMP` |
| SEM-REV-001..003 | impl+test | reconstruction order; tie/conflict detection; interval checks | 02 goldens, `review_insufficient_coverage_skips_join_errors` |
| SEM-REPLAY-001/002 | impl+test | `consumer::select`, replay path | 01 replay goldens (core + LSD via generated consumer SQL), `replay_selection_rules` |
| SEM-REPLAY-003 | impl (protocol) / open (live) | Databricks MERGE + commit-history evidence; backfill range checks | `rebinding_round_trip…` (pending evidence); live: open |
| SEM-TRANS-001..003 | impl+test | declared columns, typed AST, filters | `expression_typing_and_arity`, `null_predicate` |
| SEM-TRANS-004 | impl+test | exact aggregates, null policies, all-null → null/ok | 03 goldens, `fsum_is_exact` |
| SEM-TRANS-005 | impl+test | recency floor, lookup ties/nulls | 05 goldens (core + LSD), `review_recency_must_use_window_event_time` |
| SEM-TRANS-006 | impl+test | checked i64, widening, division, non-finite | `expr::tests::*`, `widening_tests`, `review_integer_result_widens_to_number_output` |
| SEM-OUT-001 | impl+test | output type/nullability/range/enum | `missingness_and_output_contracts` |
| SEM-LIN-001/002 | impl+test | role-level lineage completeness | `lineage_must_cover_roles` |
| SEM-LIN-003 | partial | generation manifest, host evidence (declared vs observed) | live: open |
| SEM-MISS-001..003 | impl+test | distinct states, coverage envelopes, outage | `known_empty`, `insufficient_history`, `source_outage`, coverage µs tests |
| SEM-QUAL-001 | impl+test (core, local Spark) | error/warn/quarantine in `eval.rs` `check_output` and generated quality statements (DECISIONS D12) | `projects/quality/*.yaml` fixtures; differential incl. warnings; `test_quarantine_reports_the_violating_rows_and_warn_keeps_values` |
| SEM-FIT-001..003 | partial / unsupported (execution) | artifact/fitting checks (`FIT_SPLIT_MISMATCH`, windows); never executed | 04 preflight goldens |
| SEM-CUSTOM-001/002 | impl+test (rejection) | no fetch/execute; unsupported capability blocks planning | `capability_reports_positive_and_rejecting`, LSD custom rejection |
| SEM-CONS-001 | impl+test | consumer API request validation | `consumer_api_rejects_incomplete_or_foreign_requests` |
| SEM-LABEL-001/002 | impl+test (core, local Spark) | `label.rs` (`label_requests`, `build_labels`); `databricks.rs` `lower_labels`; `featlas_databricks/labels.py` | `labels.rs` (9 tests incl. shared goldens); `test_generated_label_sql_matches_goldens_and_core` (7) |
| SEM-FRESH-001/002 | partial | staleness and reject policy; lag metrics not measured | `replay_selection_rules` |
| SEM-DEPLOY-001 | impl+test | binding completeness, pins | `deployment_binding_contracts` |
| SEM-DEPLOY-002 | partial | credential-free config check; retention preflight (source history vs declared `earliest_reproducible_at`, output change data feed; DECISIONS D13); permissions not probed | `test_retention_preflight_checks_change_data_feed`; live `test_8_retention_contract_is_checked_against_the_tables` |
| SEM-DEPLOY-003 | partial | publication authorization gate; idempotent MERGE; generated paused Databricks job with slot-derived publication identity (D14); job deployment not observed | live retry/crash tests; `test_scheduling.py` |
| SEM-BUDGET-001 | partial | evaluator work/time/row limits; MCP limits; host enforces `max_scan_bytes` and `max_output_rows` with explicit override, reports cost/memory as not enforceable (D13) | `test_output_row_budget_stops_before_publication`; live scan-budget assertion |
| SEM-PORT-001 | impl+test / **open (target gate)** | vendor-free core; Databricks adapter | `core_crate_has_no_vendor_dependencies`; Databricks live gate open |
| SEM-PORT-002 | impl+test | registry resolution, config schema validation | `synthetic_adapter_pins_do_not_plan` |
| SEM-PORT-003/004 | impl+test | mapping completeness/injectivity; case-insensitive collisions on Databricks | `deployment_binding_contracts`, rebinding round trip |
| SEM-PORT-005 | impl+test | consumer bindings & surface | `deployment_binding_contracts`, `consumer_api_rejects…` |
| SEM-PORT-006 | impl+test | derived requirements, scoped assessments, readiness | `capability_reports_positive_and_rejecting`, `kit_sample_report_lacks_planner_derived_requirements` |
| SEM-PORT-007 | impl+test | native policy | `native_policy` |
| SEM-PORT-008 | impl+test | logical vs deployment identities | `logical_fingerprints_survive_rebinding_and_adapter_repinning` |
| SEM-PORT-009 | impl+test (local) | publication metadata preserved; evidence separate | `rebinding_round_trip_preserves_values_states_and_publication_metadata` |
| SEM-PORT-010 | impl (plan only) | `migration.rs` | `migration_plan_names_mappings_and_blockers`; no live migration |
| SEM-TEST-001/002 | impl+test | goldens read, never regenerated; metamorphic checks | all fixture suites; `temporal_boundary_metamorphic_checks` |
| SEM-TEST-003 | impl | `tools/make_evidence.sh` binds evidence to the commit and binary digest | `docs/evidence/candidate.txt` |
