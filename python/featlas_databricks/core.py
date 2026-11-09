"""Subprocess boundary to the authoritative Rust core (``fe-spec``)."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any


class CoreError(RuntimeError):
    def __init__(self, message: str, payload: Any = None, exit_code: int | None = None):
        super().__init__(message)
        self.payload = payload
        self.exit_code = exit_code


def _find_binary() -> str:
    env = os.environ.get("FEATLAS_FE_SPEC")
    if env:
        return env
    found = shutil.which("fe-spec")
    if found:
        return found
    repo = Path(__file__).resolve().parents[2]
    for profile in ("release", "debug"):
        candidate = repo / "target" / profile / "fe-spec"
        if candidate.exists():
            return str(candidate)
    raise CoreError("fe-spec binary not found; build it with `cargo build` or set FEATLAS_FE_SPEC")


class Core:
    """Invoke core commands; every result is the core's JSON output."""

    def __init__(self, binary: str | None = None, timeout_seconds: int = 120):
        self.binary = binary or _find_binary()
        self.timeout = timeout_seconds

    def _run(self, *args: str, ok_codes: tuple[int, ...] = (0,)) -> Any:
        proc = subprocess.run([self.binary, *args], capture_output=True, text=True, timeout=self.timeout, check=False)
        payload = None
        if proc.stdout.strip():
            try:
                payload = json.loads(proc.stdout)
            except json.JSONDecodeError:
                payload = proc.stdout
        if proc.returncode not in ok_codes:
            raise CoreError(f"fe-spec {' '.join(args[:1])} failed ({proc.returncode}): {proc.stderr.strip() or proc.stdout[-2000:]}", payload, proc.returncode)
        return payload

    def version(self) -> str:
        return subprocess.run([self.binary, "--version"], capture_output=True, text=True, check=True).stdout.strip()

    def generate(self, project: str, deployment: str, out_dir: str) -> dict[str, dict]:
        self._run("generate", project, "--deployment", deployment, "--out", out_dir)
        root = Path(out_dir)
        artifacts = {}
        for path in sorted((root / "databricks").glob("*/artifacts.json")):
            data = json.loads(path.read_text(encoding="utf-8"))
            artifacts[data["feature_set"]] = data
        manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
        plan = json.loads((root / "plan.json").read_text(encoding="utf-8"))
        return {"artifacts": artifacts, "manifest": manifest, "plan": plan}

    def coverage(self, project: str, deployment: str, feature_set: str, as_of: str, statuses: list[dict]) -> dict:
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump(statuses, f)
            path = f.name
        try:
            return self._run("coverage", project, "--deployment", deployment, "--feature-set", feature_set, "--as-of", as_of, "--status", path)["features"]
        finally:
            os.unlink(path)

    def consume(self, project: str, request: dict, records: list[dict] | None) -> dict:
        tmp = []
        try:
            with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
                json.dump(request, f)
                tmp.append(f.name)
            args = ["consume", project, "--request", tmp[0]]
            if records is not None:
                with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
                    json.dump(records, f)
                    tmp.append(f.name)
                args += ["--records", tmp[1]]
            return self._run(*args, ok_codes=(0, 1))
        finally:
            for p in tmp:
                os.unlink(p)

    def evaluate_case(self, project: str, case_id: str) -> dict:
        return self._run("evaluate", project, "--fixture", case_id, "--format", "json", ok_codes=(0, 1))["result"]
