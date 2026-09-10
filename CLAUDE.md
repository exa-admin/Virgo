# CLAUDE.md

**Read [AGENTS.md](AGENTS.md) first.** It is the single source for this repo: what the
project is, the pipeline, the golden ID continuity policy, the module map, conventions,
current state and open items. This file only carries what a coding session needs up front.

## In one paragraph

Databricks / Spark-native **Customer MDM match engine**, replacing Informatica MDM country
by country. It reads an operator population, standardizes it, runs exact and fuzzy rules in
a priority waterfall, builds connected components without GraphFrames, and assigns stable
golden IDs to Delta. The hard requirement running through everything: **an id Informatica
already issued must never change.**

## Where things are

| | |
|---|---|
| Engine | `src/matching/` — 9 modules; `pipeline.py` orchestrates, `rules.py` matches, `graph.py` groups, `golden_ids.py` assigns |
| Config | `src/matching/conf/base.json` + `conf/countries/{CC}.json` (inside the package, ships in the wheel) |
| DDL | `sql/setup_tables.sql` |
| Notebooks | `notebooks/run_match.py`, `run_tests.py`, `enrich_addresses.py` |
| Tests | `tests/` — run on a cluster, see `docs/TESTING.md` |
| Docs | `docs/ARCHITECTURE.md`, `DEPLOYMENT.md`, `TABLES.md`, `TESTING.md` |

## Rules of engagement

- **Everything runs on Databricks.** There is no supported local workflow. Locally you can
  lint and import-check; that is all. Do not build local Spark workflows or suggest them.
- **Never weaken the golden ID policy** in AGENTS.md. An Informatica group must not split,
  be re-minted, or be renamed. `validate_group_assignments` enforces it — do not relax it
  to make something pass.
- **No Python UDFs, no GraphFrames.** Column/SQL expressions only.
- **Writes are country slices** (`replaceWhere` / `DELETE WHERE CountryCode`). Keep it that
  way so one country's run cannot damage another's.
- Match rules live in **JSON config**, never hardcoded in Python.
- The engine-only components pass (`engine_match_id`) exists so the over/undermatch views
  have an honest basis for comparison. It looks redundant. It is not — see AGENTS.md.
- After changing matching semantics, run `tests/` on a cluster. A failure there is a
  migration risk, not a flaky test.

## Setup on a new machine

```bash
python3.11 -m venv .venv && .venv/bin/pip install -r requirements.txt
```

Editing only — `requirements.txt` is never installed on a cluster, and the wheel
(`./scripts/build_wheel.sh`) has no dependencies of its own.

## Before you start

AGENTS.md → "Open items" lists what is outstanding, including a **leaked API key that still
needs rotating** and a **config trap in `MY.json`**. Read it.
