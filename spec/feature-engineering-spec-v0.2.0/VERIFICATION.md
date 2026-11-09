# Verification record — specification bundle 0.2.0

Verified on 9 November 2026. Scope: the exact local specification, example documents, and bounded verification kit included in this archive.

## Observed results

| Check | Result |
| --- | --- |
| Draft 2020-12 schema metaschema validation | 12 schema files passed |
| YAML structure and implemented semantic checks | 7 complete project documents passed |
| Executed reference acceptance cases | 50 / 50 passed |
| Learned/custom adapter cases | 3 explicitly not executed |
| Regression and negative tests | 127 passed; 0 failures |
| Included inert artifact SHA-256 values | 2 matched the supplied bytes |
| Capability report consistency | 1 passed; the report explicitly claims `execution_ready: false` |
| Original logical contracts and goldens | Unchanged across all six upgraded 0.1.0 examples |
| Local Markdown links | All referenced local files resolve |

The v0.1.0 temporal/parser regressions remain covered. Independent review of this revision identified read-only fixture output destinations, duplicate physical resources distinguished only by format labels, and consumers bound to an unguaranteed publication surface. Those findings were reproduced, fixed, and covered by rejecting regression tests. The revised coding prompt makes Databricks the first production target and distinguishes local rebinding from actual target or migration evidence. The final independent review reran the integrated checks, verified the fixes and unchanged logical contracts, and found no remaining material blocker for delivering this specification and agent-handoff bundle within its stated limits.

The 50 fixture executions include 13 existing fault-count cases exercised again in the store-migration project. This is 37 distinct authored baseline cases plus those 13 repeated executions, not 50 newly invented scenarios. The three adapter-required cases remain outside the executed count.

## What was exercised

The fixtures cover event and availability boundaries, identical and conflicting duplicates, tenant identity, population reconstruction, explicit source completeness/outages, corrections that move event times, retractions, historical dimension corrections and overlapping intervals, feature dependencies, integer-money ratios, null versus empty results, actual publication visibility, latest null supersession, stale publications, deterministic lookup ties, and floor-rounded recency.

New tests cover namespaced adapter/version references, role mismatches, complete/injective mappings, output resource collisions, consumer surfaces, native capability declarations, report identity/evidence/readiness consistency, and in-memory mapping round trips preserving null, zero, state, and publication metadata.

Negative tests also cover malformed/misspelled contracts, invalid references/columns/types/versions, missing lineage, dependency cycles, wrong nullability, duplicate mappings, fractional timestamp comparison, integer overflow, three-valued expressions, restricted YAML, digest mismatch, and bundle-path containment.

## Boundaries of this evidence

- Schema validation establishes document structure; the local semantic checker implements the checks in `tools/validation.py`. A production implementation must additionally satisfy the complete normative semantic rule register.
- The three adapter-required cases are golden requirements, not successful executions. Learned/custom artifact bytes were verified without executing them.
- Failed warning/quarantine quality gates return `UNSUPPORTED_CAPABILITY` in the reference kit; this kit does not create operational warning/quarantine artifacts.
- Live Databricks queries, feature registration/publication, SDK/workspace compatibility, access enforcement, retention enforcement, real SLO/cost measurements, incremental processing, label construction, and fitted-transform training remain future implementation obligations.
- Capability reports are checked for structural and record consistency and explicit requirement coverage. The checker does not derive the full production plan, authenticate evidence URIs/digests, probe an adapter, or independently establish execution readiness. Its `capability_reports_consistent` result is not a Databricks support result.
- Local mapping helpers read/write in-memory records only. The alternate-store example does not implement another real store or demonstrate data transfer, cutover, or rollback. The future canonical semantic hash is also outside this kit; current identity checks compare logical objects directly and report hashes bind exact project bytes.
- All fixture histories are synthetic. Their correctness does not establish that any production source retains adequate history or supplies trustworthy completeness evidence.

## Reproduction

Tested with Python 3.12, PyYAML 6.0.3, jsonschema 4.26.0, and referencing 0.37.0. From the extracted directory, install `requirements.txt` in a virtual environment and run:

```sh
python tools/verify_bundle.py
python tools/verify_bundle.py --json
python tools/verify_manifest.py
```

The first command executes the local suite. The JSON command identifies every executed versus deferred case. The manifest command verifies the declared payload bytes. A modified candidate requires fresh verification; this record must not be reused as evidence for changed bytes.
