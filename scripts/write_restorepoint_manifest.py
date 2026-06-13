"""Write a source-focused manifest hash for restore points."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path


EXCLUDE_PARTS = {
    ".git",
    "__pycache__",
    ".pytest_cache",
    "venv",
    "logs",
    "archive",
    "data_store",
    "restore_point_22",
    "restore_point_23",
    "restore_point_24",
    "restore_point_25",
    "restore_point_26",
    "ut1-index-final3",
}
EXCLUDE_NAMES = {
    "audit_results.json",
    "RESTORE_POINT_24_MANIFEST.json",
    "RESTORE_POINT_25_MANIFEST.json",
    "RESTORE_POINT_26_MANIFEST.json",
}
INCLUDE_SUFFIXES = {
    ".py",
    ".js",
    ".css",
    ".html",
    ".json",
    ".md",
    ".txt",
    ".ini",
    ".bat",
    ".ps1",
    ".gitignore",
}


def iter_source_files(root: Path):
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(root)
        if path.name in EXCLUDE_NAMES:
            continue
        if any(part in EXCLUDE_PARTS or part.startswith("restore_point_") for part in rel.parts):
            continue
        if path.suffix.lower() in INCLUDE_SUFFIXES or path.name == ".gitignore":
            yield path


def build_manifest(root: Path) -> dict:
    files = []
    aggregate = hashlib.sha256()
    for path in iter_source_files(root):
        rel = path.relative_to(root).as_posix()
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        aggregate.update(rel.encode("utf-8"))
        aggregate.update(digest.encode("ascii"))
        files.append({"path": rel, "sha256": digest, "bytes": path.stat().st_size})
    return {
        "generated_at": datetime.now().isoformat(),
        "root": str(root),
        "file_count": len(files),
        "source_tree_sha256": aggregate.hexdigest(),
        "files": files,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    manifest = build_manifest(Path(args.root).resolve())
    Path(args.out).write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps({k: manifest[k] for k in ("file_count", "source_tree_sha256")}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
