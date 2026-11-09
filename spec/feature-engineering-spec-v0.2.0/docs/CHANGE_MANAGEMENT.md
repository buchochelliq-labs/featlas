# Versioning, change review, and release evidence

## 1. Versions identify different things

| Version or digest | What it identifies | Example change |
| --- | --- | --- |
| `spec_version` | Document language and normative semantics. | New transform syntax or changed baseline null rules. |
| Project metadata version | A reviewed collection of definitions. | Add a consumer and its acceptance cases. |
| Source/entity/relationship version | Meaning and assumptions of the data model. | Change entity population, source availability definition, or relationship grain. |
| Feature version | The feature's semantic contract. | Change window boundary, predicate, unit, revision policy, or missingness. |
| Feature-set/consumer/deployment version | Published composition, consumer policy, or target intent. | Pin a different feature version or change staleness. |
| Implementation version and digest | Exact code/build that performs the computation. | Optimize a query while preserving behavior. |
| Fitted artifact version and digest | Exact learned state used by a transform. | Fit a scaler on a different training split. |
| Input snapshot identity | Exact source state used for an execution. | Read a new Delta version or file set. |

The schema uses exact semantic-version strings. In the 0.x language series, a minor version may contain incompatible language changes; any such change must include migration notes. Never silently redefine an already released version.

One version of a given object ID appears in a project. A consumer pins exact feature and set versions, not “latest.” If a registry later supports several versions simultaneously, its resolution and isolation rules require an explicit extension.

## 2. What is a semantic change?

Treat changes to any of these as semantic until demonstrated otherwise: entity grain, source meaning, keys, tenant scope, availability definition, revision order, window/boundaries, join cardinality or validity, predicates, aggregation, unit/scale, null and empty behavior, coverage requirement, artifact selection, publication/replay policy, or consumer freshness.

An owner/name/description correction may be non-semantic if it does not alter interpretation. A corrected description that reveals the previous implementation was wrong requires an explicit compatibility decision; a documentation label cannot hide a behavior change.

A bug fix can change outputs while restoring the documented contract. Record that fact, preserve the old candidate's evidence, describe affected historical results, and decide whether to produce a new feature version or a corrected implementation under the existing meaning. Do not rewrite historical publication records.

## 3. Review sequence

1. Identify the changed contract and affected downstream references.
2. Create a concrete semantic diff: changed meaning, expected output impact, compatibility, migration/backfill implications.
3. Update schema, normative prose, examples, and rule IDs together where needed.
4. Add or update independently reasoned expected cases. Do not regenerate goldens from the implementation.
5. Run structural, semantic, capability, fixture, and applicable target checks.
6. Ask an independent reviewer to assess semantic correctness, missing cases, and whether evidence supports the claims.
7. Resolve findings and rerun the checks affected by changes.
8. Freeze the exact candidate and assemble the manifest and verification report.
9. Perform any release, merge, or deployment only under its own authorization.

A schema/prose conflict is release-blocking. A wrapper schema must resolve to the same authoritative definitions, not drift into a parallel language.

## 4. Candidate evidence

The release manifest should name every payload file and its SHA-256 digest. Exclude caches, dependencies, secrets, temporary outputs, and private logs. Do not include the manifest's own digest inside the list it hashes; define the checksum scope clearly.

A product execution manifest is separate from the archive manifest. It binds a run to the normalized project hash, source snapshots, implementation and dependencies, fitted artifacts, target capability report, parameters, output digests, and observed times.

Verify the exact files being released. If a file changes after verification, rerun the relevant checks and regenerate the checksum manifest. A pass from a prior candidate is historical evidence, not proof about the new one.

## 5. Compatibility and migration

A migration must state which fields or meanings change, how existing projects are transformed, what requires human review, and which outputs can differ. Preserve the original specification and lineage. Avoid migrations that infer new time, missingness, permission, or key semantics from defaults.

Before replacing a feature in a consumer, compare expected outputs and operational requirements. Backfill planning must account for retained revisions, earliest reproducible cutoff, historical coverage, fitting splits, and separate publication visibility.

Deprecation should identify the successor, compatibility differences, affected consumers, and retention period for old definitions/artifacts. A replacement must not cause pinned consumers to resolve to an unreviewed version.


## 6. Version 0.2.0 portability revision

This is a breaking deployment-syntax revision from 0.1.0. Existing logical feature definitions and golden expectations retain their meaning. The deployment provider/engine shorthand is replaced by exact adapter instances and independent roles. Generic locations, explicit mappings, consumer routing, publication surfaces, scoped capabilities, and native bindings make a store change reviewable. Follow [MIGRATION.md](MIGRATION.md); changing only `spec_version` is insufficient.

A physical store change updates deployment/version and implementation/evidence identities as appropriate. It does not require changing stable feature or consumer IDs/versions when their semantics are preserved. Keep a separate binding-sensitive full project digest and a logical semantic fingerprint from the production core's specified canonicalization. The bundled report uses the exact project-file SHA-256 and the local tests compare logical objects directly.
