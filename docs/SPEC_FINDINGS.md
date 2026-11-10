# Specification, schema and kit findings

Recorded per the instruction to report schema/prose conflicts precisely. None required changing
the vendored archive; each is resolved by an explicit implementation decision.

1. **Wrapper schema titles say 0.1.0.** Nine convenience schemas (`consumer`, `deployment`, `entity`,
   `expression`, `feature-set`, `feature`, `relationship`, `source`, `acceptance-suite`) carry
   titles "… Specification 0.1.0" under 0.2.0 `$id`s. Titles are annotations; validation uses
   `$id`/`$ref` only. Reported, not changed.
2. **Sample capability report omits planner-derived requirements.** `examples/reports/databricks_capabilities_unverified.json`
   (bound to example 06's bytes) lists only declared requirements; SEM-PORT-006 requires derived
   ones too. Our checker reports `REPORT_REQUIRED_CAPABILITY_MISSING` for
   `databricks_platform/orchestrator/core.scheduling` (interval schedule). The kit's checker
   covers declared requirements only, as its README states.
3. **Declared capabilities that no operation needs.** Examples declare `core.as_of_join` for
   features without joins and use `core.aggregate` for recency/lookup. featlas keeps declared
   requirements and additionally derives `core.recency`/`core.lookup`/`core.relationship_join`
   (warning `CAPABILITY_REQUIREMENT_DERIVED`).
4. **Timestamp format vs calendar validity.** The schema pattern accepts e.g. `2026-02-30T…Z`;
   SEM-TIME-003 requires rejection. featlas validates every contract timestamp
   (`INVALID_TIMESTAMP`).
5. **Number fields.** JSON Schema `number` fields (tolerances, budgets, ranges) are binary64; `0`
   and `0.0` are the same contract value and hash identically.
6. **Label availability column.** The label schema has its own `available_time_column`, while
   reconstruction uses the source's declared availability column. featlas requires them to be
   equal rather than guessing which governs.
7. **Tenant identity of differently named columns.** The schema marks tenant keys only on
   entities; a source column holding the tenant under another name is not machine-identifiable,
   featlas therefore derives tenant columns from each feature's own declarations (the tenant
   key's `group_by` column, then relationship key pairs along the join chain) and rejects joins
   that drop the tenant. Residual limit: a relationship that pairs the tenant with a non-tenant
   dimension column of the same type cannot be detected; a typed column-level tenant marker
   would be a language addition.
8. **Kit leniency.** The kit accepts YAML 1.1 plain scalars like `yes` as strings, checks
   interval validity only on key-matched dimension rows, and does not type-check fixture values
   inside expressions beyond Python semantics; featlas is stricter (all examples still pass).
9. **Publication instant on Delta.** The spec requires observed visibility evidence but no
   portable mechanism; featlas defines the MERGE commit timestamp from Delta history as the
   instant for `offline_snapshot`, recorded after the commit (see `DECISIONS.md` D6). Whether that
   timestamp is a trustworthy commit instant on the pinned runtime is a live-gate question.
