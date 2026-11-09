"""Conformance helper for the specification bundle; not the future product.

All schemas resolve from the shipped directory. No documents, references, code,
or credentials are fetched from the network by these helpers.
"""
from __future__ import annotations

import copy
import json
import math
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource
from referencing.exceptions import NoSuchResource
from binding_validation import check_binding_semantics


class DocumentError(ValueError):
    pass


class JsonYamlLoader(yaml.SafeLoader):
    """Safe parser with JSON scalar resolution and unique string keys."""

    def construct_scalar(self, node):
        if isinstance(node, yaml.ScalarNode) and node.style is None and re.fullmatch(
            r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?Z", node.value
        ):
            raise DocumentError("Timestamp scalars must be quoted")
        return super().construct_scalar(node)


JsonYamlLoader.yaml_implicit_resolvers = {
    key: [(tag, pattern) for tag, pattern in items
          if tag not in {
              "tag:yaml.org,2002:bool", "tag:yaml.org,2002:int",
              "tag:yaml.org,2002:float", "tag:yaml.org,2002:null",
              "tag:yaml.org,2002:timestamp", "tag:yaml.org,2002:merge",
          }]
    for key, items in yaml.SafeLoader.yaml_implicit_resolvers.items()
}
JsonYamlLoader.add_implicit_resolver(
    "tag:yaml.org,2002:bool", re.compile(r"^(?:true|false)$"), list("tf"))
JsonYamlLoader.add_implicit_resolver(
    "tag:yaml.org,2002:null", re.compile(r"^null$"), ["n"])
JsonYamlLoader.add_implicit_resolver(
    "tag:yaml.org,2002:int", re.compile(r"^-?(?:0|[1-9][0-9]*)$"), list("-0123456789"))
JsonYamlLoader.add_implicit_resolver(
    "tag:yaml.org,2002:float",
    re.compile(r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+(?:[eE][+-]?[0-9]+)?|[eE][+-]?[0-9]+)$"),
    list("-0123456789"))


def _mapping(loader: JsonYamlLoader, node: yaml.MappingNode, deep: bool = False) -> dict:
    result = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if not isinstance(key, str):
            raise DocumentError("YAML mapping keys must be strings")
        if key == "<<":
            raise DocumentError("YAML merge keys are not permitted")
        if key in result:
            raise DocumentError(f"Duplicate YAML mapping key: {key!r}")
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


JsonYamlLoader.add_constructor("tag:yaml.org,2002:map", _mapping)


def load_yaml_text(text: str) -> dict:
    if len(text.encode("utf-8")) > 4 * 1024 * 1024:
        raise DocumentError("Document exceeds the verification kit's 4 MiB limit")
    try:
        for token in yaml.scan(text):
            if isinstance(token, (yaml.AliasToken, yaml.AnchorToken, yaml.TagToken)):
                raise DocumentError("YAML aliases, anchors, and explicit tags are not permitted")
        result = yaml.load(text, Loader=JsonYamlLoader)
        if not isinstance(result, dict):
            raise DocumentError("Exactly one YAML mapping document is required")
        json.dumps(result, allow_nan=False)
        return result
    except (yaml.YAMLError, TypeError, ValueError, RecursionError) as exc:
        if isinstance(exc, DocumentError):
            raise
        raise DocumentError(str(exc)) from exc


def load_yaml(path: Path) -> dict:
    return load_yaml_text(path.read_text(encoding="utf-8"))


def utc_timestamp(value: str) -> datetime:
    if not isinstance(value, str) or not re.fullmatch(
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z", value
    ):
        raise DocumentError(f"Expected UTC RFC3339 timestamp with Z: {value!r}")
    try:
        return datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise DocumentError(f"Invalid UTC timestamp: {value!r}") from exc


@dataclass(frozen=True)
class Diagnostic:
    code: str
    path: str
    message: str
    severity: str = "error"

    def to_dict(self) -> dict:
        return asdict(self)


def _no_remote(uri: str) -> Resource:
    raise NoSuchResource(ref=uri)


def schema_registry(directory: Path) -> tuple[dict, Registry, int]:
    schemas = []
    project = None
    for path in sorted(directory.glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(data)
        if "$id" not in data:
            raise DocumentError(f"Schema {path.name} has no $id")
        schemas.append((data["$id"], Resource.from_contents(data)))
        if path.name == "project.schema.json":
            project = data
    if project is None:
        raise DocumentError("Missing schemas/project.schema.json")
    if len({uri for uri, _ in schemas}) != len(schemas):
        raise DocumentError("Duplicate schema $id")
    return project, Registry(retrieve=_no_remote).with_resources(schemas), len(schemas)


def structural_diagnostics(document: dict, schema: dict, registry: Registry) -> list[Diagnostic]:
    checker = FormatChecker()

    @checker.checks("date-time", raises=DocumentError)
    def check_datetime(value):
        return not isinstance(value, str) or bool(utc_timestamp(value))

    validator = Draft202012Validator(schema, registry=registry, format_checker=checker)
    errors = sorted(validator.iter_errors(document), key=lambda e: (str(list(e.path)), e.message))
    return [Diagnostic("SCHEMA_INVALID", "/" + "/".join(str(x) for x in e.path), e.message)
            for e in errors]


def value_matches_type(value: Any, type_name: str, nullable: bool = False) -> bool:
    if value is None:
        return nullable
    if type_name == "string":
        return isinstance(value, str)
    if type_name in {"bool", "boolean"}:
        return isinstance(value, bool)
    if type_name in {"int64", "integer"}:
        return type(value) is int and -(2**63) <= value < 2**63
    if type_name in {"float64", "number"}:
        return type(value) in {int, float} and math.isfinite(value)
    if type_name in {"timestamp", "timestamp_utc"}:
        try:
            utc_timestamp(value)
            return True
        except DocumentError:
            return False
    if type_name == "date":
        try:
            return isinstance(value, str) and datetime.strptime(value, "%Y-%m-%d").strftime("%Y-%m-%d") == value
        except ValueError:
            return False
    if type_name == "json":
        return True
    # Additional type families must be implemented rather than implicitly accepted.
    return False


def semantic_diagnostics(document: dict) -> list[Diagnostic]:
    """Check local references and a bounded subset of normative semantic rules.

    Call only after structural validation. Full production conformance also
    requires the runtime/data/adapter checks in docs/SEMANTIC_RULES.md.
    """
    diagnostics: list[Diagnostic] = []

    def error(code, path, message):
        diagnostics.append(Diagnostic(code, path, message))

    collections = ["entities", "sources", "relationships", "features", "feature_sets", "consumers", "deployments", "acceptance_suites"]
    indexes = {}
    for collection in collections:
        index = {}
        for i, obj in enumerate(document.get(collection, [])):
            if obj["id"] in index:
                error("DUPLICATE_ID", f"/{collection}/{i}/id", f"Duplicate {collection} id {obj['id']}")
            index[obj["id"]] = obj
        indexes[collection] = index
    sources, entities, features = indexes["sources"], indexes["entities"], indexes["features"]
    columns = {}
    for sid, source in sources.items():
        columns[sid] = {}
        for col in source["columns"]:
            if col["name"] in columns[sid]:
                error("DUPLICATE_COLUMN", f"/sources/{sid}/columns", col["name"])
            columns[sid][col["name"]] = col

    def check_source(sid, path):
        if sid not in sources:
            error("UNKNOWN_SOURCE", path, f"Unknown source {sid}")
            return False
        return True

    def check_column(sid, name, path):
        if not check_source(sid, path):
            return None
        if name not in columns[sid]:
            error("UNKNOWN_COLUMN", path, f"Unknown source column {sid}.{name}")
            return None
        return columns[sid][name]

    def check_feature(fid, version, path):
        if fid not in features:
            error("UNKNOWN_FEATURE", path, f"Unknown feature {fid}")
            return None
        if features[fid]["version"] != version:
            error("VERSION_MISMATCH", path, f"Requested {fid}@{version}; project has {features[fid]['version']}")
        return features[fid]

    for sid, source in sources.items():
        p = f"/sources/{sid}"
        sem = source["record_semantics"]
        refs = source["primary_key"] + sem["logical_key"]
        for name in refs:
            col = check_column(sid, name, p)
            if col and col["nullable"]:
                error("NULLABLE_IDENTITY", p, f"Identity column {sid}.{name} must be non-null")
        for field in ["event_time", "available_time"]:
            col = check_column(sid, source["time"][field], p + "/time/" + field)
            if col and (col["type"] not in {"timestamp", "timestamp_utc"} or col["nullable"]):
                error("INVALID_TIME_COLUMN", p, f"{field} must identify a non-null UTC timestamp column")
        if sem["kind"] == "versioned":
            rev = sem["revision"]
            for item in rev["order_by"]:
                col = check_column(sid, item["column"], p + "/record_semantics/revision")
                if col and col["nullable"]:
                    error("NULLABLE_REVISION", p, "Revision ordering columns must be non-null")
            if rev.get("tombstone_column"):
                col = check_column(sid, rev["tombstone_column"], p + "/record_semantics/revision")
                if col and not value_matches_type(rev["tombstone_value"], col["type"], col["nullable"]):
                    error("TYPE_MISMATCH", p, "Tombstone value does not match column type")

    for eid, entity in entities.items():
        p = f"/entities/{eid}"
        pop_source = entity["population"]["source"]
        check_source(pop_source, p + "/population/source")
        seen = set()
        for key in entity["keys"]:
            if key["name"] in seen:
                error("DUPLICATE_ENTITY_KEY", p, key["name"])
            seen.add(key["name"])
            col = check_column(pop_source, key["source_column"], p + "/keys")
            if col and (col["type"] != key["type"] or col["nullable"]):
                error("ENTITY_KEY_TYPE", p, "Entity key must match a non-null population column")

    for sid, source in sources.items():
        upstream = {item["column"] for item in source["lineage"]["upstream_columns"]}
        for name in columns[sid]:
            if name not in upstream:
                error("MISSING_SOURCE_LINEAGE", f"/sources/{sid}/lineage", name)
        for name in upstream:
            check_column(sid, name, f"/sources/{sid}/lineage")

    for rid, relation in indexes["relationships"].items():
        p = f"/relationships/{rid}"
        left, right = relation["from"], relation["to"]
        if len(left["columns"]) != len(right["columns"]):
            error("RELATIONSHIP_ARITY", p, "Relationship endpoint key counts differ")
        for lcol, rcol in zip(left["columns"], right["columns"]):
            a = check_column(left["source"], lcol, p + "/from")
            b = check_column(right["source"], rcol, p + "/to")
            if a and b and a["type"] != b["type"]:
                error("RELATIONSHIP_KEY_TYPE", p, "Relationship key types differ")
        if (relation["join_type"] == "inner" and relation["unmatched"] == "preserve_null") or (relation["join_type"] == "left" and relation["unmatched"] == "drop"):
            error("JOIN_POLICY_CONFLICT", p, "Join type and unmatched behavior conflict")
        temporal = relation["temporal"]
        if temporal["mode"] == "as_of":
            for name in ["valid_from", "valid_to"]:
                col = check_column(right["source"], temporal[name], p + "/temporal")
                if col and (col["type"] != "timestamp" or (name == "valid_from" and col["nullable"])):
                    error("INVALID_TIME_COLUMN", p, "Historical intervals require timestamps and non-null valid_from")
        tenant_keys = {key["name"] for entity in entities.values() for key in entity["keys"] if key["is_tenant"]}
        for name in tenant_keys:
            if name in columns.get(left["source"], {}) and name in columns.get(right["source"], {}):
                if (name, name) not in set(zip(left["columns"], right["columns"])):
                    error("TENANT_JOIN_MISSING", p, f"Both sides contain tenant key {name}, but it is not joined")

    graph = {fid: set() for fid in features}
    for fid, feature in features.items():
        p = f"/features/{fid}"
        entity = entities.get(feature["entity"])
        if entity is None:
            error("UNKNOWN_ENTITY", p + "/entity", feature["entity"])
        inputs = {}
        required_lineage = set()
        for item in feature["inputs"]:
            alias, sid = item["alias"], item["source"]
            if alias in inputs:
                error("DUPLICATE_INPUT_ALIAS", p + "/inputs", alias)
            inputs[alias] = item
            for name in item["columns"]:
                check_column(sid, name, p + "/inputs")
                required_lineage.add((sid, name))
            if sid in sources:
                source = sources[sid]
                implicit = source["record_semantics"]["logical_key"] + list(source["time"][key] for key in ["event_time", "available_time"])
                if source["record_semantics"]["kind"] == "versioned":
                    rev = source["record_semantics"]["revision"]
                    implicit += [x["column"] for x in rev["order_by"]]
                    if rev.get("tombstone_column"):
                        implicit.append(rev["tombstone_column"])
                for name in implicit:
                    required_lineage.add((sid, name))
                    if name not in item["columns"]:
                        error("MISSING_INPUT_COLUMN", p + "/inputs", f"Required identity/time/revision column {sid}.{name} not declared")
        if entity is not None:
            sid = entity["population"]["source"]
            required_lineage.update((sid, key["source_column"]) for key in entity["keys"])
            if sid in sources:
                required_lineage.update((sid, sources[sid]["time"][k]) for k in ["event_time", "available_time"])
        dependencies = {d["feature"]: d for d in feature["dependencies"]}
        for dep in feature["dependencies"]:
            target = check_feature(dep["feature"], dep["version"], p + "/dependencies")
            graph[fid].add(dep["feature"])
            if target and target["entity"] != feature["entity"]:
                error("DEPENDENCY_ENTITY_MISMATCH", p, "Baseline dependencies require the same entity")
        lineage = set()
        for edge in feature["lineage"]["sources"]:
            check_column(edge["source"], edge["column"], p + "/lineage/sources")
            lineage.add((edge["source"], edge["column"]))
        for sid, name in sorted(required_lineage - lineage):
            error("MISSING_LINEAGE", p + "/lineage/sources", f"Missing lineage for {sid}.{name}")
        lineage_deps = set()
        for edge in feature["lineage"]["dependencies"]:
            check_feature(edge["feature"], edge["version"], p + "/lineage/dependencies")
            lineage_deps.add(edge["feature"])
        for dep in dependencies:
            if dep not in lineage_deps:
                error("MISSING_LINEAGE", p + "/lineage/dependencies", dep)

        def ast_type(node, path):
            if node is None:
                return None
            kind = node.get("kind")
            if kind == "literal":
                if not value_matches_type(node["value"], node["type"], True):
                    error("TYPE_MISMATCH", path, "Literal value and declared type disagree")
                return None if node["type"] == "null" else node["type"]
            if kind == "column":
                item = inputs.get(node["input"])
                if item is None:
                    error("UNKNOWN_INPUT", path, node["input"])
                    return None
                if node["column"] not in item["columns"]:
                    error("UNDECLARED_INPUT_COLUMN", path, node["column"])
                col = check_column(item["source"], node["column"], path)
                return col["type"] if col else None
            if kind == "feature":
                target = check_feature(node["feature"], node["version"], path)
                if node["feature"] not in dependencies:
                    error("UNDECLARED_DEPENDENCY", path, node["feature"])
                return target["output"]["type"] if target else None
            if kind != "call":
                return None
            types = [ast_type(arg, path + f"/args/{i}") for i, arg in enumerate(node["args"])]
            op = node["operator"]
            numeric = {"integer", "number"}
            arity = {"absolute": 1, "not": 1, "is_null": 1, "if_else": 3}
            expected = arity.get(op, 2)
            if op not in {"and", "or", "coalesce"} and len(types) != expected:
                error("OPERATOR_ARITY", path, f"{op} requires {expected} arguments")
            if op in {"add", "subtract", "multiply", "divide", "safe_divide", "absolute"}:
                if any(t is not None and t not in numeric for t in types):
                    error("TYPE_MISMATCH", path, f"{op} requires numeric arguments")
                return "number" if op in {"divide", "safe_divide"} or "number" in types else "integer"
            if op in {"and", "or", "not"}:
                if any(t is not None and t not in {"bool", "boolean"} for t in types):
                    error("TYPE_MISMATCH", path, f"{op} requires boolean arguments")
                return "boolean"
            if op == "is_null":
                return "boolean"
            if op in {"equal", "not_equal", "less_than", "less_or_equal", "greater_than", "greater_or_equal"}:
                known = {t for t in types if t is not None}
                if len(known) > 1 and not known <= numeric:
                    error("TYPE_MISMATCH", path, "Comparison operands have incompatible types")
                return "boolean"
            if op == "if_else":
                if types and types[0] not in {"bool", "boolean", None}:
                    error("TYPE_MISMATCH", path, "if_else condition must be boolean")
                branches = types[1:]
            else:
                branches = types
            known = {t for t in branches if t is not None}
            if len(known) > 1 and not known <= numeric:
                error("TYPE_MISMATCH", path, "Branches have incompatible types")
            return "number" if "number" in known else next(iter(known), None)

        transform = feature["transform"]
        kind = transform["kind"]
        joined_aliases = {transform["input"]} if transform.get("input") else set()
        for join in transform.get("joins", []):
            relation = indexes["relationships"].get(join["relationship"])
            if relation is None:
                error("UNKNOWN_RELATIONSHIP", p + "/transform/joins", join["relationship"])
                continue
            if relation["version"] != join["version"]:
                error("VERSION_MISMATCH", p + "/transform/joins", join["relationship"])
            left, right = inputs.get(join["left_alias"]), inputs.get(join["right_alias"])
            if left is None or right is None:
                error("UNKNOWN_INPUT", p + "/transform/joins", "Join aliases must be declared feature inputs")
                continue
            if left["source"] != relation["from"]["source"] or right["source"] != relation["to"]["source"]:
                error("RELATIONSHIP_ENDPOINT_MISMATCH", p + "/transform/joins", "Alias sources do not match relationship endpoints")
            if join["left_alias"] not in joined_aliases or join["right_alias"] in joined_aliases:
                error("JOIN_ORDER_INVALID", p, "Join must extend the existing row environment exactly once")
            joined_aliases.add(join["right_alias"])
            for endpoint, item in [(relation["from"], left), (relation["to"], right)]:
                for name in endpoint["columns"]:
                    if name not in item["columns"]:
                        error("MISSING_INPUT_COLUMN", p, f"Join column {endpoint['source']}.{name} is not declared")
            if relation["temporal"]["mode"] == "as_of":
                for name in [relation["temporal"]["valid_from"], relation["temporal"]["valid_to"]]:
                    if name not in right["columns"]:
                        error("MISSING_INPUT_COLUMN", p, f"Historical interval column {right['source']}.{name} is not declared")
        if kind in {"aggregate", "recency", "lookup"} and set(inputs) != joined_aliases:
            error("UNUSED_INPUT", p + "/inputs", "Every source alias must participate in the transform")
        if kind == "expression" and inputs:
            error("EXPRESSION_SOURCE_INPUT", p, "Derived expressions must use declared feature dependencies")
        if transform.get("filter") is not None:
            if ast_type(transform["filter"], p + "/transform/filter") not in {"bool", "boolean", None}:
                error("TYPE_MISMATCH", p, "Filter must return boolean")
        if transform.get("input") and transform["input"] not in inputs:
            error("UNKNOWN_INPUT", p + "/transform/input", transform["input"])
        value_type = ast_type(transform.get("value"), p + "/transform/value")
        for node in transform.get("distinct_by", []):
            ast_type(node, p + "/transform/distinct_by")
        if kind == "expression":
            value_type = ast_type(transform.get("expression"), p + "/transform/expression")
        if kind == "aggregate":
            op = transform["operation"]
            if op in {"sum", "mean"} and value_type not in {"integer", "number", None}:
                error("TYPE_MISMATCH", p, f"{op} requires a numeric expression")
            expected_type = "integer" if op in {"count", "count_distinct"} else "number" if op == "mean" else value_type
        elif kind == "recency":
            expected_type = "integer"
        else:
            expected_type = value_type
        actual_type = feature["output"]["type"]
        if expected_type is not None and actual_type != expected_type and not (expected_type == "integer" and actual_type == "number"):
            error("OUTPUT_TYPE_MISMATCH", p + "/output/type", f"Expected {expected_type}, found {actual_type}")
        if entity and kind in {"aggregate", "recency", "lookup"}:
            groups = transform.get("group_by", [])
            if len({g["entity_key"] for g in groups}) != len(groups):
                error("DUPLICATE_ENTITY_MAPPING", p + "/transform/group_by", "Each entity key must be mapped exactly once")
            if {g["entity_key"] for g in groups} != {key["name"] for key in entity["keys"]}:
                error("ENTITY_MAPPING_INCOMPLETE", p + "/transform/group_by", "Map every entity key exactly once")
            for g in groups:
                item = inputs.get(g["input"])
                if item is None:
                    error("UNKNOWN_INPUT", p, g["input"])
                else:
                    col = check_column(item["source"], g["column"], p + "/transform/group_by")
                    if g["column"] not in item["columns"]:
                        error("UNDECLARED_INPUT_COLUMN", p + "/transform/group_by", g["column"])
                    if (item["source"], g["column"]) not in lineage:
                        error("MISSING_LINEAGE", p + "/lineage", f"Entity mapping reads {item['source']}.{g['column']}")
                    key = next((k for k in entity["keys"] if k["name"] == g["entity_key"]), None)
                    if col and key and col["type"] != key["type"]:
                        error("ENTITY_KEY_TYPE", p, "Feature entity mapping has incompatible types")
        implicit_reads = []
        if kind == "lookup":
            implicit_reads.extend((part["input"], part["column"]) for part in transform["order_by"])
        if kind == "recency":
            implicit_reads.append((transform["input"], transform["event_time"]))
        for alias, name in implicit_reads:
            item = inputs.get(alias)
            if item is None:
                error("UNKNOWN_INPUT", p, alias)
                continue
            col = check_column(item["source"], name, p)
            if name not in item["columns"]:
                error("UNDECLARED_INPUT_COLUMN", p, name)
            if (item["source"], name) not in lineage:
                error("MISSING_LINEAGE", p, f"Implicit read {item['source']}.{name}")
            if kind == "recency" and col and col["type"] != "timestamp":
                error("INVALID_TIME_COLUMN", p, "Recency requires a timestamp column")
        missing = feature["missingness"]
        if not feature["output"]["nullable"] and (
            missing["empty"]["action"] == "null" or missing["insufficient_history"] == "null" or
            missing["source_unavailable"] == "null" or missing["null_input"] == "propagate"
        ):
            error("NULLABILITY_CONFLICT", p + "/missingness", "Missingness policy can produce null for non-null output")
        if missing["empty"]["action"] == "value" and not value_matches_type(missing["empty"]["value"], actual_type, feature["output"]["nullable"]):
            error("TYPE_MISMATCH", p, "Empty result value does not match output type")
        if kind == "aggregate" and missing["null_input"] != transform["nulls"]:
            error("NULL_POLICY_CONFLICT", p, "Aggregate nulls and feature null_input must agree")
        if feature["history"]["coverage"] == "contiguous" and feature["history"]["required_seconds"] < feature["time"]["window"]["lookback_seconds"]:
            error("HISTORY_WINDOW_MISMATCH", p, "Contiguous history must cover the window")
        constraints = feature["output"]["constraints"]
        if "minimum" in constraints and "maximum" in constraints and constraints["minimum"] > constraints["maximum"]:
            error("INVALID_RANGE", p, "Minimum exceeds maximum")
        for suite_id in feature["acceptance"]["suites"]:
            if suite_id not in indexes["acceptance_suites"]:
                error("UNKNOWN_SUITE", p + "/acceptance", suite_id)
        if kind in {"learned", "custom"} and transform["conformance_suite"] not in indexes["acceptance_suites"]:
            error("UNKNOWN_SUITE", p + "/transform/conformance_suite", transform["conformance_suite"])

    active, complete = set(), set()

    def visit(fid):
        if fid in active:
            error("DEPENDENCY_CYCLE", f"/features/{fid}/dependencies", f"Dependency cycle includes {fid}")
            return
        if fid in complete or fid not in graph:
            return
        active.add(fid)
        for dep in graph[fid]:
            visit(dep)
        active.remove(fid)
        complete.add(fid)

    for fid in graph:
        visit(fid)

    for fsid, feature_set in indexes["feature_sets"].items():
        p = f"/feature_sets/{fsid}"
        seen = set()
        if feature_set["entity"] not in entities:
            error("UNKNOWN_ENTITY", p, feature_set["entity"])
        for member in feature_set["features"]:
            target = check_feature(member["id"], member["version"], p + "/features")
            if member["id"] in seen:
                error("DUPLICATE_FEATURE_MEMBER", p, member["id"])
            seen.add(member["id"])
            if target and target["entity"] != feature_set["entity"]:
                error("FEATURE_SET_ENTITY_MISMATCH", p, member["id"])

    def check_feature_set(ref, path):
        target = indexes["feature_sets"].get(ref["feature_set"])
        if target is None:
            error("UNKNOWN_FEATURE_SET", path, ref["feature_set"])
        elif target["version"] != ref["version"]:
            error("VERSION_MISMATCH", path, ref["feature_set"])
        return target

    for cid, consumer in indexes["consumers"].items():
        p = f"/consumers/{cid}"
        feature_set = check_feature_set(consumer["feature_set"], p + "/feature_set")
        entity = entities.get(feature_set["entity"]) if feature_set else None
        if entity and {m["entity_key"] for m in consumer["entity_inputs"]} != {k["name"] for k in entity["keys"]}:
            error("ENTITY_MAPPING_INCOMPLETE", p, "Consumer must map every entity key")
        if len({m["entity_key"] for m in consumer["entity_inputs"]}) != len(consumer["entity_inputs"]):
            error("DUPLICATE_ENTITY_MAPPING", p, "Consumer repeats an entity key")
        label = consumer["label"]
        if label is not None:
            if len({m["entity_key"] for m in label["entity_keys"]}) != len(label["entity_keys"]):
                error("DUPLICATE_ENTITY_MAPPING", p, "Label repeats an entity key")
            for name in [label["value_column"], label["event_time_column"], label["available_time_column"]] + [m["column"] for m in label["entity_keys"]]:
                check_column(label["source"], name, p + "/label")
            if "dataset_cutoff" not in consumer["time_inputs"]:
                error("LABEL_CUTOFF_REQUIRED", p, "Label availability requires a dataset cutoff request field")

    for did, deployment in indexes["deployments"].items():
        p = f"/deployments/{did}"
        check_binding_semantics(document, deployment, indexes, error)
        bound = set()
        for binding in deployment["source_bindings"]:
            check_source(binding["source"], p + "/source_bindings")
            if binding["source"] in bound:
                error("DUPLICATE_SOURCE_BINDING", p, binding["source"])
            bound.add(binding["source"])
            location = binding["location"]
            if location["kind"] == "fixture" and location["suite"] not in indexes["acceptance_suites"]:
                error("UNKNOWN_SUITE", p, location["suite"])
        required = set()

        def dependency_sources(fid, seen=None):
            seen = set() if seen is None else seen
            if fid in seen or fid not in features:
                return set()
            seen.add(fid)
            f = features[fid]
            result = {i["source"] for i in f["inputs"]}
            if f["entity"] in entities:
                result.add(entities[f["entity"]]["population"]["source"])
            for dep in f["dependencies"]:
                result |= dependency_sources(dep["feature"], seen)
            return result

        for target in deployment["output_targets"]:
            feature_set = check_feature_set({"feature_set": target["feature_set"], "version": target["version"]}, p + "/output_targets")
            if feature_set:
                for member in feature_set["features"]:
                    required |= dependency_sources(member["id"])
        for sid in sorted(required - bound):
            error("MISSING_SOURCE_BINDING", p, sid)

    for suite in document.get("acceptance_suites", []):
        seen_cases = set()
        for case in suite["cases"]:
            p = f"/acceptance_suites/{suite['id']}/cases/{case['id']}"
            if case["id"] in seen_cases:
                error("DUPLICATE_CASE_ID", p, case["id"])
            seen_cases.add(case["id"])
            feature = check_feature(case["feature"]["id"], case["feature"]["version"], p + "/feature")
            if feature:
                entity = entities.get(feature["entity"])
                if entity:
                    expected_keys = {k["name"] for k in entity["keys"]}
                    seen = set()
                    for row in case["population_rows"]:
                        if set(row) != expected_keys:
                            error("POPULATION_KEYS", p, "Fixture population must contain exactly the entity keys")
                        for key in entity["keys"]:
                            if not value_matches_type(row.get(key["name"]), key["type"]):
                                error("FIXTURE_TYPE_MISMATCH", p, f"Invalid entity key {key['name']}")
                        fingerprint = json.dumps(row, sort_keys=True)
                        if fingerprint in seen:
                            error("DUPLICATE_POPULATION", p, "Duplicate fixture population row")
                        seen.add(fingerprint)
            for sid, rows in case["source_rows"].items():
                if not check_source(sid, p + "/source_rows"):
                    continue
                for i, row in enumerate(rows):
                    row_path = p + f"/source_rows/{sid}/{i}"
                    if set(row) != set(columns[sid]):
                        error("FIXTURE_COLUMNS", row_path, "Fixture row columns must exactly match source contract")
                    for name, col in columns[sid].items():
                        if name in row and not value_matches_type(row[name], col["type"], col["nullable"]):
                            error("FIXTURE_TYPE_MISMATCH", row_path + "/" + name, f"Expected {col['type']}")
            status_sources = set()
            for status in case["context"]["source_status"]:
                check_source(status["source"], p + "/context/source_status")
                if status["source"] in status_sources:
                    error("DUPLICATE_SOURCE_STATUS", p, status["source"])
                status_sources.add(status["source"])
                if status["state"] == "available" and utc_timestamp(status["coverage_start"]) >= utc_timestamp(status["coverage_end"]):
                    error("INVALID_COVERAGE_INTERVAL", p, status["source"])
            if case["context"]["mode"] == "historical_serving_replay":
                if not {"published_values", "requested_at", "consumer"} <= set(case["context"]):
                    error("REPLAY_CONTEXT_REQUIRED", p, "Replay requires observed publications and a consumer")
                elif case["context"]["consumer"] not in indexes["consumers"]:
                    error("UNKNOWN_CONSUMER", p, case["context"]["consumer"])
                else:
                    consumer = indexes["consumers"][case["context"]["consumer"]]
                    if consumer["selection"]["mode"] != case["context"]["mode"]:
                        error("CONSUMER_MODE_MISMATCH", p, "Consumer and fixture selection modes disagree")
                    fs = indexes["feature_sets"].get(consumer["feature_set"]["feature_set"])
                    if fs and case["feature"] not in fs["features"]:
                        error("CONSUMER_FEATURE_MISMATCH", p, "Feature/version is not in the consumer's pinned feature set")
            for publication in case["context"].get("published_values", []):
                target = check_feature(publication["feature"], publication["version"], p + "/context/published_values")
                if target and not value_matches_type(publication["value"], target["output"]["type"], target["output"]["nullable"]):
                    error("FIXTURE_TYPE_MISMATCH", p, "Publication value has incorrect type")
                if utc_timestamp(publication["published_at"]) < utc_timestamp(publication["as_of"]):
                    error("PUBLICATION_BEFORE_AS_OF", p, "Publication cannot precede its feature as-of")
    return diagnostics
