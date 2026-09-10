"""Test harness for the match engine. **Run these on a Databricks cluster** — see
``docs/TESTING.md``, or just open ``notebooks/run_tests.py``.

Every test builds a throwaway schema from the real ``sql/setup_tables.sql``, loads a small
source population, and runs the real ``run_country`` with the real MY rules — so the tests
cover the DDL, the config and the engine together.

On Databricks the notebook's own Spark session is reused and the DDL runs verbatim,
IDENTITY columns included, so the tests exercise exactly what production does. Point
``MDM_TEST_SCHEMA`` at a scratch catalog.schema; the fixture drops and recreates it around
every test.

The ``ON_DATABRICKS`` branches below fall back to a local Spark + Delta session. That path
is **not a supported workflow** and is not in the docs: it exists only so the suite can be
smoke-checked without a cluster. Open-source Delta cannot parse
``GENERATED ALWAYS AS IDENTITY``, so locally the DDL is rewritten to a plain BIGINT and
``MDMRowId`` is assigned up front — the fallback described in setup_tables.sql, and the
only behavioural difference between the two environments.
"""
from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SETUP_SQL = REPO_ROOT / "sql" / "setup_tables.sql"
REAL_CONF = REPO_ROOT / "src" / "matching" / "conf"
DDL_SCHEMA = "pds_auroradsar_prod.schema_informatica"
IDENTITY_DDL = "BIGINT        GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1)"

ON_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
TEST_SCHEMA = os.environ.get("MDM_TEST_SCHEMA", "mdm_test")

# Homebrew keg-only JDK, for local runs only. Override with JAVA_HOME.
DEFAULT_JAVA_HOME = "/opt/homebrew/opt/openjdk@17"


def pytest_configure(config):
    if ON_DATABRICKS:
        return
    if not os.environ.get("JAVA_HOME") and Path(DEFAULT_JAVA_HOME).is_dir():
        os.environ["JAVA_HOME"] = DEFAULT_JAVA_HOME
    # Workers must be the same interpreter as the driver, or Spark refuses to run.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    # Without this Spark binds to the LAN address and can pick up foreign traffic.
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    if ON_DATABRICKS:
        yield SparkSession.builder.getOrCreate()
        return

    from delta import configure_spark_with_delta_pip

    warehouse = tempfile.mkdtemp(prefix="mdm_test_warehouse_")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("mdm-tests")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


def _ddl_statements() -> list[str]:
    """The real setup SQL, retargeted at the test schema."""
    sql = SETUP_SQL.read_text().replace(DDL_SCHEMA, TEST_SCHEMA)
    if not ON_DATABRICKS:
        sql = sql.replace(IDENTITY_DDL, "BIGINT")
    # Strip comments before splitting: several of them contain a ';'.
    sql = "\n".join(line.split("--")[0] for line in sql.splitlines())
    return [s.strip() for s in sql.split(";") if s.strip()]


# The columns the real operator view supplies. Anything a scenario does not name is NULL,
# which is what a sparse real source looks like.
SOURCE_COLUMNS = [
    "OperatorConcatId",
    "CountryCode",
    "GoldenRecordId",
    "BDLLoadTimestamp",
    "OperatorName",
    "HouseNumberText",
    "HouseNumberExtensionText",
    "StreetText",
    "CityText",
    "StateText",
    "ZipCode",
    "CountryName",
    "SAPCustomerId",
    "OTMText",
    "LatitudeText",
    "LongitudeText",
]
SOURCE_SCHEMA = ", ".join(f"{c} timestamp" if c == "BDLLoadTimestamp" else f"{c} string" for c in SOURCE_COLUMNS)


def operator(concat_id, name=None, **fields):
    """One source row. Only what a scenario cares about needs naming."""
    row = {c: None for c in SOURCE_COLUMNS}
    row.update(OperatorConcatId=concat_id, CountryCode="MY", CountryName="Malaysia", OperatorName=name)
    row.update(fields)
    return row


class Harness:
    """Drives run_country against the test schema and reads the results back."""

    def __init__(self, spark):
        self.spark = spark
        self._next_row_id = 1

    def table(self, name):
        return self.spark.table(f"{TEST_SCHEMA}.{name}")

    def run(self, rows, country="MY"):
        """Load `rows` as the source, then match. Returns mdm_matched_results for the country."""
        import datetime

        from matching import run_country

        prepared = []
        for row in rows:
            row = dict(row)
            row["BDLLoadTimestamp"] = row.get("BDLLoadTimestamp") or datetime.datetime(2026, 1, 1)
            prepared.append(tuple(row[c] for c in SOURCE_COLUMNS))
        self.spark.createDataFrame(prepared, SOURCE_SCHEMA).write.format("delta").mode("overwrite").saveAsTable(
            f"{TEST_SCHEMA}.source_operators"
        )
        if not ON_DATABRICKS:
            self._assign_row_ids(country)
        return run_country(self.spark, country)

    def _assign_row_ids(self, country):
        """Local only: stand in for the Databricks IDENTITY column (see module docstring)."""
        known = {r.OperatorConcatId for r in self.table("mdm_row_registry").collect()}
        new = sorted(
            r.OperatorConcatId
            for r in self.table("source_operators").select("OperatorConcatId").distinct().collect()
            if r.OperatorConcatId is not None and r.OperatorConcatId not in known
        )
        if not new:
            return
        rows = [(country, self._next_row_id + i, key) for i, key in enumerate(new)]
        self._next_row_id += len(new)
        self.spark.createDataFrame(
            rows, "CountryCode string, MDMRowId bigint, OperatorConcatId string"
        ).write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
            f"{TEST_SCHEMA}.mdm_row_registry"
        )

    # ---- assertion helpers -------------------------------------------------------

    def rows_by_key(self, country="MY"):
        """{OperatorConcatId: Row} from mdm_matched_results."""
        rows = self.table("mdm_matched_results").where(f"CountryCode = '{country}'").collect()
        return {r.OperatorConcatId: r for r in rows}

    def golden(self, *concat_ids):
        by_key = self.rows_by_key()
        return [by_key[k].golden_id for k in concat_ids]

    def grouped(self, *concat_ids):
        """True when every named operator carries the same golden id."""
        return len(set(self.golden(*concat_ids))) == 1

    def engine_grouped(self, *concat_ids):
        """True when OUR RULES alone put every named operator in one group."""
        by_key = self.rows_by_key()
        return len({by_key[k].engine_match_id for k in concat_ids}) == 1


@pytest.fixture
def mdm(spark, tmp_path):
    """A clean schema, tables built from the real DDL, and the real MY match rules."""
    spark.sql(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA} CASCADE")
    spark.sql(f"CREATE SCHEMA {TEST_SCHEMA}")
    for statement in _ddl_statements():
        spark.sql(statement)

    # Empty inputs, so the golden-backfill and enrichment code paths still execute.
    spark.createDataFrame([], "CountryCode string, GoldenRecordId string").write.format("delta").mode(
        "overwrite"
    ).saveAsTable(f"{TEST_SCHEMA}.golden_masters")
    spark.createDataFrame(
        [],
        "OperatorConcatId string, OperatorName string, HouseNumberText string, StreetText string, "
        "CityText string, StateText string, CountryName string, latitude string, longitude string, "
        "ZipCode string, match_found string",
    ).write.format("delta").mode("overwrite").saveAsTable(f"{TEST_SCHEMA}.mdmenrichedoperators")

    # Real MY rules; only the schema and source tables are pointed at the test fixtures.
    conf_dir = tmp_path / "conf"
    (conf_dir / "countries").mkdir(parents=True)
    base = json.loads((REAL_CONF / "base.json").read_text())
    base["target_schema"] = TEST_SCHEMA
    base["source"] = {"format": "delta", "table": f"{TEST_SCHEMA}.source_operators"}
    base["golden_source"] = {"format": "delta", "table": f"{TEST_SCHEMA}.golden_masters"}
    (conf_dir / "base.json").write_text(json.dumps(base))
    shutil.copy(REAL_CONF / "countries" / "MY.json", conf_dir / "countries" / "MY.json")
    os.environ["MDM_CONF_DIR"] = str(conf_dir)

    yield Harness(spark)

    os.environ.pop("MDM_CONF_DIR", None)
    spark.sql(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA} CASCADE")
