"""Fast repository checks used locally and in GitHub Actions."""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN_PATTERNS = (
    re.compile(r"\b\d{10}@qq\.com\b", re.IGNORECASE),
    re.compile(r"dbc-[a-z0-9-]+\.cloud\.databricks\.com", re.IGNORECASE),
    re.compile(r"student\s+id", re.IGNORECASE),
)
MAX_FILE_BYTES = 20 * 1024 * 1024


def validate_python() -> None:
    path = ROOT / "databricks" / "ride_hailing_medallion_pipeline.py"
    ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def validate_notebooks() -> None:
    for path in sorted((ROOT / "notebooks" / "legacy").glob("*.ipynb")):
        notebook = json.loads(path.read_text(encoding="utf-8"))
        assert notebook.get("nbformat") == 4, f"Unexpected notebook version: {path}"
        for cell in notebook.get("cells", []):
            if cell.get("cell_type") == "code":
                assert not cell.get("outputs"), f"Notebook output must be stripped: {path}"
                assert cell.get("execution_count") is None, f"Execution count must be cleared: {path}"


def validate_public_content() -> None:
    text_suffixes = {".md", ".py", ".sql", ".txt", ".yml", ".yaml", ".json", ".ipynb"}
    for path in ROOT.rglob("*"):
        if not path.is_file() or ".git" in path.parts:
            continue
        assert path.stat().st_size <= MAX_FILE_BYTES, f"File is too large for this portfolio repo: {path}"
        if path.suffix.lower() in text_suffixes:
            text = path.read_text(encoding="utf-8", errors="ignore")
            for pattern in FORBIDDEN_PATTERNS:
                assert not pattern.search(text), f"Private identifier found in {path}: {pattern.pattern}"


if __name__ == "__main__":
    validate_python()
    validate_notebooks()
    validate_public_content()
    print("Repository validation passed.")
