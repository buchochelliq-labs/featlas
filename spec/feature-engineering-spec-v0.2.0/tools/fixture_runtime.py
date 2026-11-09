"""Bounded, in-memory conformance evaluator for synthetic fixture examples.

This is deliberately not a cloud adapter or production feature engine. Code in
custom/learned definitions is never loaded or executed.
"""
from __future__ import annotations

import json
import math
from datetime import timedelta
from functools import cmp_to_key

from validation import utc_timestamp, value_matches_type


class EvaluationError(ValueError):
    def __init__(self, code, message):
        self.code = code
        super().__init__(message)


def _typed_value(value, type_name):
    return utc_timestamp(value) if value is not None and type_name == "timestamp" else value


def _type_map(document, feature):
    sources = {s["id"]: s for s in document["sources"]}
    result = {(item["alias"], col["name"]): col["type"] for item in feature["inputs"]
              for col in sources[item["source"]]["columns"]}
    result.update({("feature", f["id"]): f["output"]["type"] for f in document["features"]})
    return result


def _node_type(node, types):
    kind = node["kind"]
    if kind == "literal":
        return node["type"]
    if kind == "column":
        return types.get((node["input"], node["column"]))
    if kind == "feature":
        return types.get(("feature", node["feature"]))
    if kind == "call":
        op = node["operator"]
        if op in {"divide", "safe_divide"}:
            return "number"
        if op in {"equal", "not_equal", "less_than", "less_or_equal", "greater_than", "greater_or_equal", "and", "or", "not", "is_null"}:
            return "boolean"
        args = node["args"][1:] if op == "if_else" else node["args"]
        choices = [_node_type(arg, types) for arg in args]
        return "number" if "number" in choices else next((t for t in choices if t not in {None, "null"}), None)
    return None


def select_source_rows(source, rows, as_of, availability_upper="inclusive"):
    cutoff = utc_timestamp(as_of)
    available = source["time"]["available_time"]
    eligible = [row for row in rows
                if (utc_timestamp(row[available]) <= cutoff if availability_upper == "inclusive"
                    else utc_timestamp(row[available]) < cutoff)]
    semantics = source["record_semantics"]
    column_types = {c["name"]: c["type"] for c in source.get("columns", [])}
    column_types.update({source["time"][key]: "timestamp" for key in ["event_time", "available_time"]})

    def typed_row(row):
        return {key: _typed_value(value, column_types.get(key)) for key, value in row.items()}
    keys = semantics["logical_key"]
    grouped = {}
    for row in eligible:
        identity = tuple(_typed_value(row[name], column_types.get(name)) for name in keys)
        grouped.setdefault(identity, []).append(row)
    output = []
    for identity, group in grouped.items():
        if semantics["kind"] == "append_only":
            if len(group) > 1 and (semantics["duplicate_policy"] == "reject" or
                                  any(typed_row(r) != typed_row(group[0]) for r in group[1:])):
                raise EvaluationError("DUPLICATE_CONFLICT", f"Conflicting or forbidden duplicates for {identity}")
            selected = group[0]
        else:
            revision = semantics["revision"]
            ordering = revision["order_by"]

            def compare(left, right):
                for item in ordering:
                    lval = _typed_value(left[item["column"]], column_types.get(item["column"]))
                    rval = _typed_value(right[item["column"]], column_types.get(item["column"]))
                    if lval == rval:
                        continue
                    result = -1 if lval < rval else 1
                    return -result if item["direction"] == "desc" else result
                return 0

            # Conflicts are checked for every eligible revision, not just the winning one.
            seen = {}
            for row in group:
                revkey = tuple(_typed_value(row[item["column"]], column_types.get(item["column"])) for item in ordering)
                if revkey in seen and (semantics["duplicate_policy"] == "reject" or typed_row(row) != typed_row(seen[revkey])):
                    raise EvaluationError("DUPLICATE_CONFLICT", f"Conflicting or forbidden revision for {identity}")
                seen[revkey] = row
            selected = sorted(group, key=cmp_to_key(compare))[0]
            tombstone = revision.get("tombstone_column")
            if tombstone is not None and selected[tombstone] == revision["tombstone_value"]:
                continue
        output.append(selected)
    return output


def in_window(event_time, as_of, seconds, lower, upper):
    event, end = utc_timestamp(event_time), utc_timestamp(as_of)
    start = end - timedelta(seconds=seconds)
    return ((event >= start if lower == "inclusive" else event > start) and
            (event <= end if upper == "inclusive" else event < end))


def expression(expr, rows, dependencies, types=None):
    types = {} if types is None else types
    if expr is None:
        return None
    kind = expr["kind"]
    if kind == "literal":
        return expr["value"]
    if kind == "column":
        if expr["input"] not in rows:
            raise EvaluationError("UNKNOWN_INPUT", f"Unknown expression input {expr['input']}")
        row = rows[expr["input"]]
        if row is None:  # Explicit unmatched side of a left join.
            return None
        if expr["column"] not in row:
            raise EvaluationError("UNKNOWN_COLUMN", f"Missing column {expr['column']}")
        return row[expr["column"]]
    if kind == "feature":
        if expr["feature"] not in dependencies:
            raise EvaluationError("UNKNOWN_FEATURE", expr["feature"])
        return dependencies[expr["feature"]]
    if kind != "call":
        raise EvaluationError("UNSUPPORTED_CAPABILITY", f"Expression kind {kind}")
    op, nodes = expr["operator"], expr["args"]
    if op == "if_else":
        condition = expression(nodes[0], rows, dependencies, types)
        return expression(nodes[1] if condition is True else nodes[2], rows, dependencies, types)
    if op == "coalesce":
        for node in nodes:
            value = expression(node, rows, dependencies, types)
            if value is not None:
                return value
        return None
    if op in {"and", "or"}:
        unknown = False
        for node in nodes:
            value = expression(node, rows, dependencies, types)
            if value is not None and not isinstance(value, bool):
                raise EvaluationError("TYPE_MISMATCH", f"{op} requires boolean arguments")
            unknown |= value is None
            if op == "and" and value is False:
                return False
            if op == "or" and value is True:
                return True
        return None if unknown else op == "and"
    values = [expression(node, rows, dependencies, types) for node in nodes]
    if op == "is_null":
        return values[0] is None
    if any(v is None for v in values):
        return None
    if op == "not":
        if not isinstance(values[0], bool):
            raise EvaluationError("TYPE_MISMATCH", "not requires a boolean")
        return not values[0]
    comparisons = {
        "equal": lambda a, b: a == b,
        "not_equal": lambda a, b: a != b,
        "less_than": lambda a, b: a < b,
        "less_or_equal": lambda a, b: a <= b,
        "greater_than": lambda a, b: a > b,
        "greater_or_equal": lambda a, b: a >= b,
    }
    if op in comparisons:
        return comparisons[op](*[_typed_value(value, _node_type(node, types)) for value, node in zip(values, nodes)])
    if any(isinstance(v, bool) or not isinstance(v, (int, float)) for v in values):
        raise EvaluationError("TYPE_MISMATCH", f"{op} requires numeric values")
    if op in {"divide", "safe_divide"} and values[1] == 0:
        if op == "safe_divide":
            return None
        raise EvaluationError("DIVISION_BY_ZERO", "Division by zero")
    operations = {
        "add": lambda a, b: a + b, "subtract": lambda a, b: a - b,
        "multiply": lambda a, b: a * b, "divide": lambda a, b: a / b,
        "safe_divide": lambda a, b: a / b, "absolute": abs,
    }
    if op not in operations:
        raise EvaluationError("UNSUPPORTED_CAPABILITY", f"Operator {op}")
    result = operations[op](*values)
    if isinstance(result, int) and not -(2**63) <= result < 2**63:
        raise EvaluationError("NUMERIC_OVERFLOW", "Signed int64 overflow")
    if isinstance(result, float) and not math.isfinite(result):
        raise EvaluationError("NUMERIC_OVERFLOW", "Non-finite float result")
    return result


def _empty(feature):
    policy = feature["missingness"]["empty"]
    if policy["action"] == "error":
        raise EvaluationError("EMPTY_FEATURE", "No eligible source rows")
    return (policy.get("value") if policy["action"] == "value" else None), "empty"


def _missing(feature, state):
    if feature["missingness"][state] == "error":
        raise EvaluationError(state.upper(), state.replace("_", " "))
    return None, state


def _source_ids(document, feature, visited=None):
    visited = set() if visited is None else visited
    if feature["id"] in visited:
        raise EvaluationError("DEPENDENCY_CYCLE", feature["id"])
    visited = visited | {feature["id"]}
    entities = {e["id"]: e for e in document["entities"]}
    sources = {i["source"] for i in feature["inputs"]}
    sources.add(entities[feature["entity"]]["population"]["source"])
    features = {f["id"]: f for f in document["features"]}
    for dependency in feature["dependencies"]:
        sources |= _source_ids(document, features[dependency["feature"]], visited)
    return sources


def _population(document, feature, case):
    entity = next(e for e in document["entities"] if e["id"] == feature["entity"])
    source = next(s for s in document["sources"] if s["id"] == entity["population"]["source"])
    status = next((s for s in case["context"]["source_status"] if s["source"] == source["id"]), None)
    if status is None:
        raise EvaluationError("MISSING_SOURCE_STATUS", source["id"])
    if status["state"] != "available":
        raise EvaluationError("SOURCE_UNAVAILABLE", "Cannot reconstruct the entity population")
    if source["id"] not in case["source_rows"]:
        raise EvaluationError("MISSING_SOURCE_ROWS", source["id"])
    rows = select_source_rows(source, case["source_rows"][source["id"]], case["as_of"],
                              entity["population"]["availability_upper"])
    cutoff = utc_timestamp(case["as_of"])
    rows = [r for r in rows if (utc_timestamp(r[source["time"]["event_time"]]) <= cutoff
                               if entity["population"]["event_time_upper"] == "inclusive"
                               else utc_timestamp(r[source["time"]["event_time"]]) < cutoff)]
    population = [{key["name"]: row[key["source_column"]] for key in entity["keys"]} for row in rows]
    fingerprints = [json.dumps(row, sort_keys=True) for row in population]
    if len(fingerprints) != len(set(fingerprints)):
        raise EvaluationError("DUPLICATE_POPULATION", "Population source produces duplicate entity keys")
    expected = sorted(json.dumps(row, sort_keys=True) for row in case["population_rows"])
    if sorted(fingerprints) != expected:
        raise EvaluationError("POPULATION_MISMATCH", "Injected population rows disagree with historical source reconstruction")
    return population


def _coverage(document, feature, case):
    status = {item["source"]: item for item in case["context"]["source_status"]}
    sources = {s["id"]: s for s in document["sources"]}
    cutoff = utc_timestamp(case["as_of"])
    if cutoff < utc_timestamp(feature["history"]["earliest_as_of"]):
        return _missing(feature, "insufficient_history")
    start = cutoff - timedelta(seconds=feature["history"]["required_seconds"])
    for sid in sorted(_source_ids(document, feature)):
        if sid not in status:
            raise EvaluationError("MISSING_SOURCE_STATUS", sid)
        if sid not in case["source_rows"]:
            raise EvaluationError("MISSING_SOURCE_ROWS", sid)
        if status[sid]["state"] == "unavailable":
            return _missing(feature, "source_unavailable")
        if cutoff < utc_timestamp(sources[sid]["retention"]["earliest_reproducible_at"]):
            return _missing(feature, "insufficient_history")
        if feature["history"]["coverage"] == "contiguous":
            covered_end = utc_timestamp(status[sid]["coverage_end"])
            end_insufficient = covered_end <= cutoff if feature["time"]["window"]["upper"] == "inclusive" else covered_end < cutoff
            if utc_timestamp(status[sid]["coverage_start"]) > start or end_insufficient:
                return _missing(feature, "insufficient_history")
    return None


def _joined_rows(document, feature, case):
    transform = feature["transform"]
    sources = {s["id"]: s for s in document["sources"]}
    aliases = {i["alias"]: i["source"] for i in feature["inputs"]}
    relationships = {r["id"]: r for r in document["relationships"]}
    types = _type_map(document, feature)
    available = feature["time"]["availability"]["upper"]
    primary = transform["input"]
    source = sources[aliases[primary]]
    selected = select_source_rows(source, case["source_rows"][source["id"]], case["as_of"], available)
    window = feature["time"]["window"]
    selected = [r for r in selected if in_window(r[source["time"]["event_time"]], case["as_of"],
                    window["lookback_seconds"], window["lower"], window["upper"])]
    rows = [{primary: row} for row in selected]
    for join in transform["joins"]:
        relationship = relationships[join["relationship"]]
        left_alias, right_alias = join["left_alias"], join["right_alias"]
        right_source = sources[aliases[right_alias]]
        candidates = select_source_rows(right_source, case["source_rows"][right_source["id"]], case["as_of"], available)
        left_keys, right_keys = relationship["from"]["columns"], relationship["to"]["columns"]
        output, seen_left = [], set()
        for env in rows:
            left = env[left_alias]
            key = tuple(left[k] for k in left_keys) if left is not None else tuple(None for _ in left_keys)
            if None in key and relationship["null_keys"] == "error":
                raise EvaluationError("NULL_JOIN_KEY", relationship["id"])
            if relationship["cardinality"] == "one_to_one" and None not in key:
                if key in seen_left:
                    raise EvaluationError("CARDINALITY_VIOLATION", "Repeated eligible left join key")
                seen_left.add(key)
            matches = []
            for right in candidates:
                target_key = tuple(right[k] for k in right_keys)
                if None in key or None in target_key or key != target_key:
                    continue
                temporal = relationship["temporal"]
                if temporal["mode"] == "as_of":
                    lower = utc_timestamp(right[temporal["valid_from"]])
                    upper_value = right[temporal["valid_to"]]
                    upper = utc_timestamp(upper_value) if upper_value is not None else None
                    if upper is not None and upper <= lower:
                        raise EvaluationError("INVALID_INTERVAL", relationship["id"])
                    left_source = sources[aliases[left_alias]]
                    event = utc_timestamp(left[left_source["time"]["event_time"]])
                    if event < lower or (upper is not None and event >= upper):
                        continue
                matches.append(right)
            if len(matches) > 1:
                raise EvaluationError("MULTIPLE_MATCHES", relationship["id"])
            if matches:
                output.append({**env, right_alias: matches[0]})
            elif relationship["unmatched"] == "error":
                raise EvaluationError("UNMATCHED_JOIN", relationship["id"])
            elif relationship["unmatched"] == "preserve_null":
                output.append({**env, right_alias: None})
        rows = output
    if transform["filter"] is not None:
        rows = [r for r in rows if expression(transform["filter"], r, {}, types) is True]
    return rows


def _aggregate(feature, rows, types):
    transform = feature["transform"]
    if not rows:
        return _empty(feature)
    op = transform["operation"]
    if op == "count":
        return len(rows), "ok"
    if op == "count_distinct":
        values = [tuple(_typed_value(expression(node, env, {}, types), _node_type(node, types)) for node in transform["distinct_by"]) for env in rows]
        is_null = lambda v: any(part is None for part in v)
    else:
        values = [expression(transform["value"], env, {}, types) for env in rows]
        is_null = lambda v: v is None
    if any(is_null(v) for v in values):
        if transform["nulls"] == "error":
            raise EvaluationError("NULL_INPUT", "Null aggregate input")
        if transform["nulls"] == "propagate":
            return None, "ok"
        values = [v for v in values if not is_null(v)]
    if op == "count_distinct":
        return len(set(values)), "ok"
    if not values:
        return None, "ok"
    if op == "sum":
        value = sum(values) if all(type(v) is int for v in values) else math.fsum(values)
    elif op == "mean":
        value = math.fsum(values) / len(values)
    elif op == "min":
        value = min(values, key=lambda v: _typed_value(v, _node_type(transform["value"], types)))
    elif op == "max":
        value = max(values, key=lambda v: _typed_value(v, _node_type(transform["value"], types)))
    else:
        raise EvaluationError("UNSUPPORTED_CAPABILITY", op)
    if type(value) is int and not -(2**63) <= value < 2**63:
        raise EvaluationError("NUMERIC_OVERFLOW", "Aggregate signed int64 overflow")
    if type(value) is float and not math.isfinite(value):
        raise EvaluationError("NUMERIC_OVERFLOW", "Non-finite aggregate result")
    return value, "ok"


def _check_output(feature, value, state="ok"):
    output = feature["output"]
    if not value_matches_type(value, output["type"], output["nullable"]):
        raise EvaluationError("OUTPUT_TYPE_MISMATCH", "Calculated output violates its type/nullability")
    constraints = output["constraints"]
    if value is not None:
        if "minimum" in constraints and value < constraints["minimum"]:
            raise EvaluationError("QUALITY_VIOLATION", "Output below minimum")
        if "maximum" in constraints and value > constraints["maximum"]:
            raise EvaluationError("QUALITY_VIOLATION", "Output above maximum")
        if "enum" in constraints and value not in constraints["enum"]:
            raise EvaluationError("QUALITY_VIOLATION", "Output outside enum")
    for check in feature["quality"]["checks"]:
        violated = False
        if check["kind"] == "not_null":
            violated = state in {"ok", "empty"} and value is None
        elif check["kind"] == "range" and value is not None:
            violated = ("minimum" in check and value < check["minimum"]) or ("maximum" in check and value > check["maximum"])
        elif check["kind"] == "finite" and value is not None:
            violated = type(value) not in {int, float} or not math.isfinite(value)
        if violated:
            if check["severity"] == "warn" or feature["quality"]["on_violation"] == "quarantine":
                raise EvaluationError("UNSUPPORTED_CAPABILITY", "Reference kit cannot emit warning/quarantine artifacts for failed quality checks")
            raise EvaluationError("QUALITY_VIOLATION", f"Declared {check['kind']} check failed")


def _replay(document, feature, case, population):
    context = case["context"]
    if "requested_at" not in context or "published_values" not in context or "consumer" not in context:
        raise EvaluationError("REPLAY_CONTEXT_REQUIRED", "Replay requires actual publications, requested_at and consumer")
    consumer = next(c for c in document["consumers"] if c["id"] == context["consumer"])
    if consumer["selection"]["mode"] != "historical_serving_replay":
        raise EvaluationError("CONSUMER_MODE_MISMATCH", "Selected consumer does not permit replay")
    feature_set = next(s for s in document["feature_sets"] if s["id"] == consumer["feature_set"]["feature_set"])
    if {"id": feature["id"], "version": feature["version"]} not in feature_set["features"]:
        raise EvaluationError("CONSUMER_FEATURE_MISMATCH", "Feature is not a pinned member of the consumer feature set")
    cutoff, requested = utc_timestamp(case["as_of"]), utc_timestamp(context["requested_at"])
    result = []
    for entity in population:
        eligible = [p for p in context["published_values"] if p["feature"] == feature["id"] and
                    p["version"] == feature["version"] and p["entity_keys"] == entity and
                    utc_timestamp(p["as_of"]) <= cutoff and utc_timestamp(p["published_at"]) <= requested]
        ordering = lambda p: (utc_timestamp(p["as_of"]), utc_timestamp(p["published_at"]), p["sequence"])
        if len({ordering(p) for p in eligible}) != len(eligible):
            raise EvaluationError("PUBLICATION_TIE", "Duplicate publication total-order keys")
        if not eligible:
            value, state = None, "not_published"
        else:
            chosen = max(eligible, key=ordering)
            if (cutoff - utc_timestamp(chosen["as_of"])).total_seconds() > consumer["max_staleness_seconds"]:
                raise EvaluationError("STALE_FEATURE", "Selected feature is too old")
            value, state = chosen["value"], chosen["state"]
        if value is None and consumer["missing_value_policy"] == "reject":
            raise EvaluationError("MISSING_FEATURE_VALUE", "Consumer rejects the latest selected null/missing result")
        result.append({"entity_keys": entity, "value": value, "state": state})
    return result


def evaluate_case(document, case):
    features = {f["id"]: f for f in document["features"]}
    target = features[case["feature"]["id"]]
    if target["transform"]["kind"] in {"learned", "custom"}:
        raise EvaluationError("UNSUPPORTED_CAPABILITY", "Reference verifier never executes learned artifacts or custom code")
    population = _population(document, target, case)
    if case["context"]["mode"] == "historical_serving_replay":
        return _replay(document, target, case, population)
    cache = {}

    def calculate(feature):
        if feature["id"] in cache:
            return cache[feature["id"]]
        kind = feature["transform"]["kind"]
        types = _type_map(document, feature)
        if kind in {"learned", "custom"}:
            raise EvaluationError("UNSUPPORTED_CAPABILITY", f"Reference verifier cannot execute {kind}")
        state = _coverage(document, feature, case)
        deps = {d["feature"]: calculate(features[d["feature"]]) for d in feature["dependencies"]}
        envs = _joined_rows(document, feature, case) if kind in {"aggregate", "lookup", "recency"} and state is None else []
        output = []
        for i, entity in enumerate(population):
            if state is not None:
                value, output_state = state
            elif kind == "expression":
                dep_values = {fid: values[i]["value"] for fid, values in deps.items()}
                value, output_state = expression(feature["transform"]["expression"], {}, dep_values, types), "ok"
            else:
                transform = feature["transform"]
                rows = [env for env in envs if all(env[g["input"]] is not None and
                         env[g["input"]][g["column"]] == entity[g["entity_key"]] for g in transform["group_by"])]
                if kind == "aggregate":
                    value, output_state = _aggregate(feature, rows, types)
                elif not rows:
                    value, output_state = _empty(feature)
                elif kind == "recency":
                    latest = max(utc_timestamp(r[transform["input"]][transform["event_time"]]) for r in rows)
                    value, output_state = (utc_timestamp(case["as_of"]) - latest) // timedelta(seconds=1), "ok"
                elif kind == "lookup":
                    def compare(left, right):
                        for part in transform["order_by"]:
                            value_type = types.get((part["input"], part["column"]))
                            a = _typed_value(left[part["input"]][part["column"]], value_type)
                            b = _typed_value(right[part["input"]][part["column"]], value_type)
                            if a is None or b is None:
                                raise EvaluationError("NULL_ORDER_KEY", "Lookup ordering key is null")
                            if a != b:
                                sign = -1 if a < b else 1
                                return -sign if part["direction"] == "desc" else sign
                        return 0
                    rows = sorted(rows, key=cmp_to_key(compare))
                    if len(rows) > 1 and compare(rows[0], rows[1]) == 0:
                        raise EvaluationError("LOOKUP_TIE", "Ambiguous lookup ordering")
                    value, output_state = expression(transform["value"], rows[0], {}, types), "ok"
                else:
                    raise EvaluationError("UNSUPPORTED_CAPABILITY", kind)
            _check_output(feature, value, output_state)
            output.append({"entity_keys": entity, "value": value, "state": output_state})
        cache[feature["id"]] = output
        return output

    return calculate(target)


def compare_rows(feature, actual, expected):
    key = lambda row: json.dumps(row["entity_keys"], sort_keys=True)
    actual, expected = sorted(actual, key=key), sorted(expected, key=key)
    if len(actual) != len(expected):
        return False, f"Expected {len(expected)} rows, got {len(actual)}"
    tolerance = feature["output"].get("numeric_tolerance", {"absolute": 0, "relative": 0})
    for got, want in zip(actual, expected):
        if got["entity_keys"] != want["entity_keys"] or got["state"] != want["state"]:
            return False, f"Keys/state differ: expected {want}, got {got}"
        a, b = got["value"], want["value"]
        if type(a) in {int, float} and type(b) in {int, float}:
            equal = abs(a - b) <= max(tolerance["absolute"], tolerance["relative"] * abs(b))
        else:
            equal = type(a) is type(b) and _typed_value(a, feature["output"]["type"]) == _typed_value(b, feature["output"]["type"])
        if not equal:
            return False, f"Values differ: expected {want}, got {got}"
    return True, ""
