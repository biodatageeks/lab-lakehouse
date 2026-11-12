"""Build course notebooks: notebooks/src/*.py (percent format) -> notebooks/*.ipynb.

    uv run python -m lab.build           # write the .ipynb files
    uv run python -m lab.build --check   # exit 1 if any .ipynb is out of date

Sources are plain Python so diffs stay readable; the generated .ipynb files are committed
too, because that is what students open in Snowflake Workspaces.
"""

from __future__ import annotations

import json
import sys

from lab.sf import ROOT

SRC = ROOT / "notebooks" / "src"
OUT = ROOT / "notebooks"
TARGETS = [(SRC, OUT), (ROOT / "notebooks" / "colab" / "src", ROOT / "notebooks" / "colab")]


def _cells(text: str) -> list[dict]:
    cells: list[tuple[str, list[str]]] = []
    for line in text.splitlines():
        if line.startswith("# %%"):
            cells.append(("markdown" if "[markdown]" in line else "code", []))
            continue
        if not cells:
            cells.append(("code", []))
        cells[-1][1].append(line)
    out = []
    for i, (kind, lines) in enumerate(cells):
        if kind == "markdown":
            lines = [ln[2:] if ln.startswith("# ") else ln.lstrip("#") for ln in lines]
        while lines and not lines[0].strip():
            lines.pop(0)
        while lines and not lines[-1].strip():
            lines.pop()
        if not lines:
            continue
        src = [ln + "\n" for ln in lines[:-1]] + [lines[-1]]
        cell = {"cell_type": kind, "id": f"cell{i:02d}", "metadata": {}, "source": src}
        if kind == "code":
            cell.update(execution_count=None, outputs=[])
        out.append(cell)
    return out


def notebook(text: str) -> str:
    nb = {
        "cells": _cells(text),
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    return json.dumps(nb, indent=1, ensure_ascii=False) + "\n"


def build(check: bool = False) -> list[str]:
    stale = []
    for src_dir, out_dir in TARGETS:
        stale += _build_dir(src_dir, out_dir, check)
    return stale


def _build_dir(src_dir, out_dir, check: bool) -> list[str]:
    stale = []
    for src in sorted(src_dir.glob("*.py")):
        target = out_dir / f"{src.stem}.ipynb"
        content = notebook(src.read_text())
        if not target.exists() or target.read_text() != content:
            stale.append(target.name)
            if not check:
                target.write_text(content)
    return stale


if __name__ == "__main__":
    check = "--check" in sys.argv
    stale = build(check)
    print(("out of date: " if check else "built: ") + (", ".join(stale) or "nothing"))
    sys.exit(1 if check and stale else 0)
