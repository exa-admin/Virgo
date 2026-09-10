# Running the tests on Databricks

The test suite runs on a Databricks cluster. Each test builds a throwaway schema from the
real `sql/setup_tables.sql`, loads a small dummy operator population, runs the real
`run_country` with the real MY rules from `MY.json`, asserts on `mdm_matched_results`, then
drops the schema. DDL, config and engine are covered together.

> **The suite drops and recreates its schema between tests.** Point it at a scratch schema.
> It refuses to start if the name ends in `schema_informatica`.

## What you need once

1. **The repo on the cluster**, not just the wheel — the tests need `tests/` and
   `sql/setup_tables.sql`. Either:
   - **Git folder (recommended):** Workspace → Create → Git folder → paste the repo URL. Or
   - **Upload:** zip the project, Workspace → ⋮ → Import → File.
2. **A scratch schema** you can create and drop tables in, e.g.
   `pds_auroradsar_dev.mdm_test`. Ask for `CREATE SCHEMA` / `CREATE TABLE` there if you do
   not have it.
3. **A cluster** running DBR 13.3 LTS or newer. Any size — the test data is a handful of
   rows. A shared/serverless cluster is fine.

Do **not** install `delta-spark` on the cluster; Databricks provides Delta itself.

## Option A — the runner notebook (easiest)

1. Open `notebooks/run_tests.py` from the repo folder in your Workspace.
2. Attach it to a cluster.
3. Set the widgets:
   - `test_schema` → `pds_auroradsar_dev.mdm_test` (your scratch schema)
   - `repo_path` → leave blank; it finds the repo from the notebook's own path
   - `pytest_args` → leave as `-q`
4. **Run All.**

The last cell prints the pytest summary and fails the notebook if anything failed, so it
also works as a job task.

## Option B — a few lines in any notebook

```python
%pip install pytest
```

```python
dbutils.library.restartPython()
```

```python
import os, sys, pytest

repo = "/Workspace/Users/<you>/Virgo"        # or /Workspace/Repos/<you>/Virgo
os.environ["MDM_TEST_SCHEMA"] = "pds_auroradsar_dev.mdm_test"
sys.path.insert(0, f"{repo}/src")            # test the checkout, not an installed wheel

pytest.main(["-q", f"{repo}/tests"])
```

## Everyday commands

Pass these through the `pytest_args` widget, or as extra list entries to `pytest.main`.

| Goal | Argument |
|---|---|
| One test | `-k test_fuzzy_matches_a_name_typo` |
| One topic | `-k undermatch` or `-k informatica` |
| Show why a test failed, with values | `-vv` |
| Stop at the first failure | `-x` |
| See the engine's own progress logs | `-s` |
| List the tests without running them | `--collect-only -q` |

## As a scheduled job

Add a **Notebook task** pointing at `notebooks/run_tests.py`, with the base parameter
`test_schema` set to your scratch schema. The notebook asserts on the exit code, so a
failing test fails the job. Useful as a gate before rebuilding and deploying the wheel.

## Reading a failure

Test names state the expectation, so the failure line usually tells you the story:

```
FAILED tests/test_matching.py::test_different_businesses_are_not_matched
```

That means two operators the engine should have kept apart were merged — an overmatch, the
most damaging kind of regression. Rerun that one test with `-vv -s` to see the engine's
stage-by-stage output, then inspect the leftover evidence in your scratch schema:

```sql
SELECT * FROM pds_auroradsar_dev.mdm_test.mdm_rule_evaluations WHERE IsMatched;
SELECT * FROM pds_auroradsar_dev.mdm_test.mdm_rule_results ORDER BY RuleExecutionOrder;
```

The fixture drops the schema at the end of each test, so add `--pdb` or comment out the
teardown in `tests/conftest.py` if you need the tables to survive for inspection.

## What the scenarios cover

**Must match** — exact name + zip; a fuzzy single-character name typo; a transitive chain
(A~B on name+zip, B~C on SAP id).

**Must not match** (the overmatch guard) — different businesses sharing a city and zip;
junk names such as `TEST`; records excluded by `%invalid%`, which stay unmatched but keep
their Informatica group.

**Informatica continuity** — a group is never split or renamed even when our rules
disagree; a new record inherits the group's id; a brand-new cluster is minted above the
`golden_id_floor`; an engine rule that would bridge two Informatica groups is blocked and
recorded in stage `999`; and a blanket check that no `SourceGoldenRecordId` ever changes.

**Stability and tracing** — rerunning unchanged data yields identical ids and an empty
changelog; a record moved by a data change produces exactly one `operator_golden_changelog`
row with the right before/after values and reason.

**The comparison views** — undermatch flags what Informatica missed, overmatch flags what
it wrongly joined, and both are empty when the engine and Informatica agree.

**A known config trap** — `test_null_in_an_exclusion_column_silently_drops_the_record_from_that_rule`
pins the fact that MY's `Exact_SAP_Customer_ID` rule skips every record with a NULL
`OTMText`, because `NOT (OTMText IN (...))` is NULL rather than TRUE in SQL. If you fix the
filter with `coalesce(OTMText, '')`, that test will fail — and should be updated.

## Adding a scenario

Tests read as data in, expectation out. `operator()` fills everything you do not name with
NULL, which is what a sparse real source looks like.

```python
def test_two_branches_of_one_chain_stay_apart(mdm):
    mdm.run([
        operator("OP1", "Kopitiam", ZipCode="50000", CityText="Kuala Lumpur", StreetText="Jalan A"),
        operator("OP2", "Kopitiam", ZipCode="59000", CityText="Ampang", StreetText="Jalan B"),
    ])
    assert not mdm.grouped("OP1", "OP2")
```

Helpers on the `mdm` fixture: `run(rows)`, `rows_by_key()`, `golden(*ids)`,
`grouped(*ids)` (same golden id), `engine_grouped(*ids)` (same id from our rules alone,
ignoring Informatica's groupings) and `table(name)` for any table in the test schema.
