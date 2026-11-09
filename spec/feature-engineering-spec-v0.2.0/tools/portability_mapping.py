"""Bounded, in-memory field mapping for the example-06 conformance checks.

This is not a production adapter or another feature-store integration. It does
not access files, locations, URIs, networks, SDKs, or credentials; nor does it
evaluate features, select publications, assert capabilities, or change clocks.
Temporal/feature semantics remain in the existing canonical fixture evaluator.

The logical snapshot shape is exactly the following mapping envelope::

    {
        "entity": {"logical_entity_key": value, ...},
        "values": {"feature_id": {"value": value, "state": state}, ...},
        "as_of": original_feature_as_of,
        "published_at": original_observed_publication_time,
        "publication_sequence": original_publication_sequence,
        "publication_id": original_publication_identity,
        "materialization_id": current_materialization_identity,
    }

Targets carry exact feature-set/version and feature/version pins. Callers must
first validate their whole project structurally and semantically, including
membership, entity grain, versions, and types. A target alone cannot establish
which entity keys or features its referenced set should contain. Encoding does
also require exact agreement between the target mappings and supplied snapshot.
Decoding requires every declared physical field. Output value/state/publication
semantics remain the caller's responsibility; these functions preserve them
without casts, defaults, timestamp rewriting, or publication fabrication.

Source normalization requires every declared logical column and validates its
value with validation.value_matches_type. Extra physical fields are ignored;
an absent required field is rejected even when its declared value is nullable.
"""
from __future__ import annotations

from typing import Any

from validation import value_matches_type


# These are the required keys of the schema's publicationColumns definition.
PUBLICATION_FIELDS = (
    "as_of", "published_at", "publication_sequence", "publication_id",
    "materialization_id",
)


class MappingError(ValueError):
    """A rejected local mapping, with a stable diagnostic code."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _require_fields(record: dict, fields, code: str, context: str) -> None:
    if not isinstance(record, dict):
        raise MappingError(code, f"{context} must be an object")
    missing = set(fields) - record.keys()
    if missing:
        raise MappingError(code, f"{context} is missing fields: {', '.join(sorted(missing))}")


def _field_names(mapping: dict, code: str, context: str) -> None:
    if not isinstance(mapping, dict) or not mapping:
        raise MappingError(code, f"{context} must be a nonempty field mapping")
    if any(not isinstance(name, str) or not name for pair in mapping.items() for name in pair):
        raise MappingError(code, f"{context} requires nonempty logical and physical field names")


def _unique_physical_fields(fields: list[str]) -> None:
    if len(set(fields)) != len(fields):
        raise MappingError("PHYSICAL_COLUMN_COLLISION", "Each logical field needs a distinct physical field")


def normalize_source_rows(source: dict, binding: dict, physical_rows: list[dict]) -> list[dict]:
    """Project mapped source fields into logical rows and reject implicit casts."""
    _require_fields(source, ("id", "version", "columns"), "SOURCE_COLUMN_MAPPING_INCOMPLETE", "source")
    _require_fields(binding, ("source", "version", "column_mapping"), "SOURCE_COLUMN_MAPPING_INCOMPLETE", "binding")
    if binding["source"] != source["id"] or binding["version"] != source["version"]:
        raise MappingError("SOURCE_BINDING_MISMATCH", "Binding must pin this exact logical source and version")
    mapping = binding["column_mapping"]
    _field_names(mapping, "SOURCE_COLUMN_MAPPING_INCOMPLETE", "source column mapping")
    columns = source["columns"]
    names = [column["name"] for column in columns]
    if len(set(names)) != len(names) or set(mapping) != set(names):
        raise MappingError("SOURCE_COLUMN_MAPPING_INCOMPLETE", "Map every declared source column exactly once")
    _unique_physical_fields(list(mapping.values()))
    logical_rows = []
    for index, row in enumerate(physical_rows):
        _require_fields(row, mapping.values(), "MISSING_PHYSICAL_FIELD", f"source row {index}")
        logical = {}
        for column in columns:
            name = column["name"]
            value = row[mapping[name]]
            if not value_matches_type(value, column["type"], column["nullable"]):
                raise MappingError("SOURCE_VALUE_TYPE_MISMATCH",
                                   f"Source row {index}, column {name!r} does not match its declared type/nullability")
            logical[name] = value
        logical_rows.append(logical)
    return logical_rows


def _output_layout(target: dict) -> tuple[dict, list[dict], dict]:
    _require_fields(target, ("feature_set", "version", "entity_key_mapping", "feature_mapping", "metadata_columns"),
                    "OUTPUT_MAPPING_INCOMPLETE", "output target")
    if any(not isinstance(target[key], str) or not target[key] for key in ("feature_set", "version")):
        raise MappingError("OUTPUT_MAPPING_INCOMPLETE", "Target must identify its pinned feature set and version")
    entity = target["entity_key_mapping"]
    _field_names(entity, "OUTPUT_MAPPING_INCOMPLETE", "entity key mapping")
    metadata = target["metadata_columns"]
    _field_names(metadata, "PUBLICATION_MAPPING_INCOMPLETE", "publication mapping")
    if set(metadata) != set(PUBLICATION_FIELDS):
        raise MappingError("PUBLICATION_MAPPING_INCOMPLETE", "Map every publication metadata field exactly once")
    features = target["feature_mapping"]
    if not isinstance(features, list) or not features:
        raise MappingError("OUTPUT_MAPPING_INCOMPLETE", "At least one pinned feature mapping is required")
    feature_ids = set()
    physical_fields = list(entity.values()) + list(metadata.values())
    for feature in features:
        _require_fields(feature, ("feature", "version", "column", "state_column"),
                        "OUTPUT_MAPPING_INCOMPLETE", "feature mapping")
        if any(not isinstance(feature[key], str) or not feature[key]
               for key in ("feature", "version", "column", "state_column")):
            raise MappingError("OUTPUT_MAPPING_INCOMPLETE", "Feature pins and value/state field names must be nonempty")
        if feature["feature"] in feature_ids:
            raise MappingError("DUPLICATE_FEATURE_MAPPING", "Map each logical feature exactly once")
        feature_ids.add(feature["feature"])
        physical_fields.extend((feature["column"], feature["state_column"]))
    _unique_physical_fields(physical_fields)
    return entity, features, metadata


def encode_snapshot(target: dict, snapshot: dict) -> dict[str, Any]:
    """Map one already-evaluated logical snapshot to a physical row in memory."""
    entity, features, metadata = _output_layout(target)
    _require_fields(snapshot, ("entity", "values", *PUBLICATION_FIELDS), "MISSING_SNAPSHOT_FIELD", "snapshot")
    if (not isinstance(snapshot["entity"], dict) or set(snapshot["entity"]) != set(entity)
            or not isinstance(snapshot["values"], dict)
            or set(snapshot["values"]) != {feature["feature"] for feature in features}):
        raise MappingError("SNAPSHOT_MAPPING_MISMATCH", "Snapshot entity and feature identities must exactly match the target")
    physical = {column: snapshot["entity"][key] for key, column in entity.items()}
    for feature in features:
        result = snapshot["values"][feature["feature"]]
        _require_fields(result, ("value", "state"), "MISSING_SNAPSHOT_FIELD", f"feature {feature['feature']!r}")
        physical[feature["column"]] = result["value"]
        physical[feature["state_column"]] = result["state"]
    physical.update({column: snapshot[key] for key, column in metadata.items()})
    return physical


def decode_snapshot(target: dict, row: dict) -> dict[str, Any]:
    """Recover mapped snapshot fields without evaluating or selecting a value."""
    entity, features, metadata = _output_layout(target)
    required = list(entity.values()) + list(metadata.values())
    required.extend(column for feature in features for column in (feature["column"], feature["state_column"]))
    _require_fields(row, required, "MISSING_PHYSICAL_FIELD", "snapshot physical row")
    return {
        "entity": {key: row[column] for key, column in entity.items()},
        "values": {feature["feature"]: {"value": row[feature["column"]], "state": row[feature["state_column"]]}
                   for feature in features},
        **{key: row[column] for key, column in metadata.items()},
    }
