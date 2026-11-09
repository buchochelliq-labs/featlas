#!/usr/bin/env python3
"""Check all files declared by the bundle manifest, without network access."""
import hashlib
import json
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    manifest = json.loads((root / "MANIFEST.json").read_text(encoding="utf-8"))
    failures = []
    for item in manifest["files"]:
        path = (root / item["path"]).resolve()
        if not path.is_relative_to(root) or not path.is_file():
            failures.append(item["path"] + ": missing or unsafe path")
            continue
        payload = path.read_bytes()
        if len(payload) != item["size_bytes"] or hashlib.sha256(payload).hexdigest() != item["sha256"]:
            failures.append(item["path"] + ": digest or size mismatch")
    if failures:
        print("FAIL\n" + "\n".join(failures))
        return 1
    print(f"PASS: {len(manifest['files'])} declared payload files verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
