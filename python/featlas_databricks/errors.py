"""Map Spark failures to the core's stable diagnostic codes (no semantics here)."""

from __future__ import annotations

import re


class FeatlasError(RuntimeError):
    def __init__(self, code: str, message: str, rule: str = "", detail: object = None):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.rule = rule
        self.detail = detail


_RAISED = re.compile(r"FEATLAS:([A-Z][A-Z0-9_]+):\s*([^'\"\n]*)")
_SPARK_CODES = {
    "ARITHMETIC_OVERFLOW": "NUMERIC_OVERFLOW",
    "DIVIDE_BY_ZERO": "DIVISION_BY_ZERO",
    "CAST_INVALID_INPUT": "INVALID_SOURCE_ROW",
    "CAST_OVERFLOW": "NUMERIC_OVERFLOW",
}


def map_spark_exception(exc: BaseException) -> FeatlasError:
    text = str(exc)
    m = _RAISED.search(text)
    if m:
        return FeatlasError(m.group(1), m.group(2).strip(), detail=text[:2000])
    for spark_code, code in _SPARK_CODES.items():
        if spark_code in text:
            return FeatlasError(code, f"Spark reported {spark_code}", detail=text[:2000])
    return FeatlasError("EXECUTION_FAILURE", "Spark execution failed", detail=text[:2000])
