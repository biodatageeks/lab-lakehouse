"""Offline checks: generated notebooks are in sync with their sources."""

import json

from lab import build
from lab.sf import ROOT


def test_notebooks_in_sync_with_sources():
    assert build.build(check=True) == [], "run: uv run python -m lab.build"


def test_every_notebook_is_valid_json_with_code():
    for nb in (ROOT / "notebooks").glob("*.ipynb"):
        cells = json.loads(nb.read_text())["cells"]
        assert any(c["cell_type"] == "code" for c in cells), nb.name


def test_requirements_cover_notebook_imports():
    req = (ROOT / "notebooks" / "requirements.txt").read_text()
    for pkg in ("duckdb", "polars", "pyiceberg"):
        assert pkg in req


def test_course_notebooks_install_packages_first():
    """Interactive Workspaces notebooks must install requirements.txt before importing anything."""
    for nb in sorted((ROOT / "notebooks").glob("0*.ipynb")):
        code = [c for c in json.loads(nb.read_text())["cells"] if c["cell_type"] == "code"]
        assert "!pip install -q -r requirements.txt" in "".join(code[0]["source"]), nb.name
