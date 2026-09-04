# Databricks notebook source
# MAGIC %md
# MAGIC # Path bootstrap
# MAGIC
# MAGIC Finds this repo's `src/` (sibling of `notebooks/`) and puts it on `sys.path`
# MAGIC so packages like `dq` and `matching` import correctly.
# MAGIC
# MAGIC Usage from another notebook in this folder:
# MAGIC ```
# MAGIC %run ./00_path_setup
# MAGIC ```

# COMMAND ----------

from __future__ import annotations

import sys
from pathlib import Path


def _notebook_workspace_path() -> Path | None:
    """Return the current notebook path under /Workspace, if available."""
    try:
        nb = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
        # Context path is like /Repos/user/Engine/notebooks/foo
        # On the driver FS it lives under /Workspace/...
        return Path("/Workspace") / str(nb).lstrip("/")
    except Exception:
        return None


def _looks_like_repo_src(src: Path) -> bool:
    return src.is_dir() and ((src / "dq").is_dir() or (src / "matching").is_dir())


def resolve_src() -> Path:
    """
    Resolve absolute path to this repo's ``src/`` directory.

    Search order:
      1. Walk up from the current Databricks notebook path
      2. Walk up from process cwd
      3. Common Workspace/Repos fallbacks
    """
    tried: list[str] = []

    starts: list[Path] = []
    nb_path = _notebook_workspace_path()
    if nb_path is not None:
        starts.append(nb_path)
    starts.append(Path.cwd())

    for start in starts:
        for parent in [start, *start.parents]:
            src = parent / "src"
            tried.append(str(src))
            if _looks_like_repo_src(src):
                return src.resolve()

    for root in (
        Path("/Workspace/Repos/Engine"),
        Path.cwd() / "Engine",
    ):
        src = root / "src"
        tried.append(str(src))
        if _looks_like_repo_src(src):
            return src.resolve()

    raise ModuleNotFoundError(
        "Could not find repo src/ containing dq/ or matching/.\n"
        "Import the full Engine folder (not only notebooks/), then re-run.\n"
        "Paths tried:\n  - " + "\n  - ".join(dict.fromkeys(tried))
    )


SRC = resolve_src()
REPO_ROOT = SRC.parent

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

print(f"REPO_ROOT = {REPO_ROOT}")
print(f"SRC       = {SRC}")
print(f"dq exists = {(SRC / 'dq').is_dir()}")
