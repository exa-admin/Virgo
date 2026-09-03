"""Final golden ID assignment with Informatica continuity.

Golden id selection per connected component ("cluster", keyed by TempClusterId):

1. Candidate ids are collected from every record in the cluster:
   - tier 1: Informatica ``SourceGoldenRecordId`` (candidate date = ``GoldenIDCreatedDate``)
   - tier 2: engine id previously persisted in ``MDMRowRegistry.MDMGoldenId``
     with ``MDMGoldenIdSource = 'ENGINE'`` (candidate date = ``MDMGoldenIdAssignedDate``)
2. A candidate id may be present in several clusters (Informatica cluster split by
   the engine, or an engine cluster that split). Each id is *claimed* by exactly one
   cluster, ranked by: most records that already carried the id as their assigned
   golden id (incumbency), then most records carrying the id, then earliest candidate
   date, then smallest TempClusterId.
3. Each cluster picks one of the ids it claimed: lowest tier first (Informatica beats
   engine), then earliest candidate date, then smallest id.
4. Clusters left without an id are minted a new id from ``MDMGoldenIdSequence``
   (see ``_reserve_golden_id_range``): ids are ``>= golden_id_floor`` and greater than
   every golden id known to the registry, assigned in TempClusterId order.

Every rule is deterministic, so re-running on unchanged data yields identical ids.
Transitions (old id -> new id) are appended to ``MDMGoldenIdHistory`` and the chosen
ids are written back to ``MDMRowRegistry`` so the next run prefers them (tier 2).

With ``preserve_source_golden_groups`` (default) the records of an Informatica group are
hard-linked before components (``source_golden_groups.py``), so step 2 never has to
arbitrate an Informatica id between components: a SPLIT of an Informatica id is impossible
and ``validate_source_golden_group_assignments`` fails the run if it ever happens. A MERGE
of two Informatica ids only occurs when ``allow_source_golden_group_merge`` is true.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F

from matching.config import GOLDEN_ID_SOURCE_ENGINE, GOLDEN_ID_SOURCE_INFORMATICA
from matching.utils import _is_empty, _sql_literal

# Columns _ensure_row_registry attaches to the processed DataFrame for prior assignments.
PREVIOUS_GOLDEN_ID_COLUMN = "PreviousMDMGoldenId"
PREVIOUS_GOLDEN_ID_SOURCE_COLUMN = "PreviousMDMGoldenIdSource"
PREVIOUS_GOLDEN_ID_ASSIGNED_COLUMN = "PreviousMDMGoldenIdAssignedDate"

HISTORY_REASON_MERGE = "MERGE"
HISTORY_REASON_SPLIT = "SPLIT"


def collect_record_match_rules(match_links_df: DataFrame) -> DataFrame:
    edge_rules = (
        match_links_df.filter(F.col("src_is_rule_subject")).select(F.col("src").alias("record_id"), "match_rule")
        .unionByName(match_links_df.filter(F.col("dst_is_rule_subject")).select(F.col("dst").alias("record_id"), "match_rule"))
        .dropDuplicates(["record_id", "match_rule"])
    )
    return edge_rules.groupBy("record_id").agg(F.concat_ws(", ", F.sort_array(F.collect_set("match_rule"))).alias("final_match_rule"))


def _registry_golden_id_bounds(spark: SparkSession, cfg: Dict[str, Any]) -> Dict[str, Optional[int]]:
    """Max golden ids known to the registry across ALL countries (ids are a single global space)."""
    row = (
        spark.table(cfg["rowRegistryTable"])
        .agg(
            F.max(F.col("SourceGoldenRecordId").cast("long")).alias("max_source"),
            F.max(F.col("MDMGoldenId").cast("long")).alias("max_engine"),
        )
        .first()
    )
    return {"max_source": row["max_source"], "max_engine": row["max_engine"]}


def validate_golden_id_space(spark: SparkSession, cfg: Dict[str, Any]) -> None:
    """Fail fast when engine and Informatica id ranges are not (or no longer) disjoint.

    - Informatica ids must stay below ``golden_id_floor`` (otherwise Informatica could
      later mint an id the engine already handed out for a migrated country).
    - No engine-minted id may equal any Informatica id present in the registry.
    """
    floor = int(cfg["golden_id_floor"])
    bounds = _registry_golden_id_bounds(spark, cfg)
    if bounds["max_source"] is not None and bounds["max_source"] >= floor:
        raise ValueError(
            f"Informatica SourceGoldenRecordId values reach {bounds['max_source']}, which is >= "
            f"golden_id_floor {floor}. Engine-minted ids would no longer be guaranteed disjoint from "
            "Informatica ids. Raise 'golden_id_floor' in the country config (and keep it identical for "
            "all countries)."
        )

    registry = spark.table(cfg["rowRegistryTable"])
    engine_ids = (
        registry.filter(F.col("MDMGoldenIdSource") == F.lit(GOLDEN_ID_SOURCE_ENGINE))
        .select(F.col("MDMGoldenId").cast("long").alias("golden_id"))
        .filter(F.col("golden_id").isNotNull())
        .distinct()
    )
    source_ids = (
        registry.select(F.col("SourceGoldenRecordId").cast("long").alias("golden_id"))
        .filter(F.col("golden_id").isNotNull())
        .distinct()
    )
    collisions = engine_ids.join(source_ids, "golden_id", "inner")
    if not _is_empty(collisions):
        sample = [r["golden_id"] for r in collisions.limit(5).collect()]
        raise ValueError(
            f"Engine-minted golden ids collide with Informatica SourceGoldenRecordId values (sample: {sample}). "
            "Informatica has produced ids inside the engine range; raise 'golden_id_floor' and remediate the "
            "affected MDMRowRegistry rows before continuing."
        )


def _reserve_golden_id_range(spark: SparkSession, cfg: Dict[str, Any], count: int) -> int:
    """Reserve ``count`` consecutive golden ids and return the first one.

    Base = max(golden_id_floor, sequence high-water mark, max known Informatica id + 1,
    max known engine id + 1). The high-water mark is advanced with a conditional MERGE
    and read back, so a concurrent run that raced on the same sequence is detected
    (Delta's optimistic concurrency also rejects the losing writer). Ranges burnt by a
    failed run simply leave gaps, like a database sequence.
    """
    if count <= 0:
        raise ValueError("count must be positive")
    sequence_table = cfg["goldenIdSequenceTable"]
    sequence_name = cfg["golden_id_sequence_name"]
    sequence_name_sql = _sql_literal(sequence_name)
    floor = int(cfg["golden_id_floor"])

    hwm_row = (
        spark.table(sequence_table)
        .filter(F.col("SequenceName") == F.lit(sequence_name))
        .select(F.col("NextValue").cast("long").alias("NextValue"))
        .first()
    )
    hwm = int(hwm_row["NextValue"]) if hwm_row is not None and hwm_row["NextValue"] is not None else None
    bounds = _registry_golden_id_bounds(spark, cfg)

    base = max(
        floor,
        hwm or 0,
        (bounds["max_source"] or 0) + 1,
        (bounds["max_engine"] or 0) + 1,
    )
    next_value = base + count

    if hwm is None:
        matched_guard = "target.NextValue IS NULL"
    else:
        matched_guard = f"target.NextValue = {hwm}"
    spark.sql(
        f"""
        MERGE INTO {sequence_table} AS target
        USING (SELECT '{sequence_name_sql}' AS SequenceName, CAST({next_value} AS BIGINT) AS NextValue) AS source
        ON target.SequenceName = source.SequenceName
        WHEN MATCHED AND {matched_guard} THEN UPDATE SET
          target.NextValue = source.NextValue,
          target.DateUpdated = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (SequenceName, NextValue, DateUpdated)
        VALUES (source.SequenceName, source.NextValue, current_timestamp())
        """
    )
    readback = (
        spark.table(sequence_table)
        .filter(F.col("SequenceName") == F.lit(sequence_name))
        .select(F.col("NextValue").cast("long").alias("NextValue"))
        .first()
    )
    if readback is None or readback["NextValue"] != next_value:
        raise RuntimeError(
            f"Golden id range reservation failed for sequence '{sequence_name}' in {sequence_table}: "
            f"expected NextValue {next_value}, found {None if readback is None else readback['NextValue']}. "
            "Another run probably allocated ids concurrently; re-run this country."
        )
    print(f"  -> Reserved {count} golden ids [{base}, {next_value - 1}] from {sequence_table}")
    return base


def _build_final_golden_ids(processed_df: DataFrame, cluster_labels_df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    """Return one row per record_id: TempClusterId, golden_id, golden_id_source, golden_id_is_new."""
    spark = processed_df.sparkSession
    required = {
        "CountryCode",
        "record_id",
        "SourceGoldenRecordId",
        "GoldenIDCreatedDate",
        PREVIOUS_GOLDEN_ID_COLUMN,
        PREVIOUS_GOLDEN_ID_SOURCE_COLUMN,
        PREVIOUS_GOLDEN_ID_ASSIGNED_COLUMN,
    }
    missing = sorted(required - set(processed_df.columns))
    if missing:
        raise ValueError(f"processed DataFrame is missing columns required for golden id assignment: {missing}")

    cluster_rows = (
        processed_df.select(
            "CountryCode",
            F.col("record_id").cast("long").alias("record_id"),
            F.col("SourceGoldenRecordId").cast("long").alias("SourceGoldenRecordId"),
            F.col("GoldenIDCreatedDate").cast("timestamp").alias("GoldenIDCreatedDate"),
            F.col(PREVIOUS_GOLDEN_ID_COLUMN).cast("long").alias("PreviousGoldenId"),
            F.col(PREVIOUS_GOLDEN_ID_SOURCE_COLUMN).cast("string").alias("PreviousGoldenIdSource"),
            F.col(PREVIOUS_GOLDEN_ID_ASSIGNED_COLUMN).cast("timestamp").alias("PreviousGoldenIdAssignedDate"),
        )
        .dropDuplicates(["record_id"])
        .join(
            cluster_labels_df.select("record_id", F.col("golden_id").alias("TempClusterId")),
            "record_id",
            "left",
        )
        .withColumn("TempClusterId", F.coalesce(F.col("TempClusterId"), F.col("record_id")).cast("long"))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    # --- 1. candidate ids per cluster -------------------------------------------------
    informatica_candidates = cluster_rows.filter(F.col("SourceGoldenRecordId").isNotNull()).select(
        "CountryCode",
        "TempClusterId",
        F.col("SourceGoldenRecordId").alias("candidate_id"),
        F.lit(1).alias("tier"),
        F.col("GoldenIDCreatedDate").alias("candidate_date"),
        F.when(F.col("PreviousGoldenId") == F.col("SourceGoldenRecordId"), F.lit(1)).otherwise(F.lit(0)).alias("incumbent"),
    )
    engine_candidates = (
        cluster_rows.filter(
            F.col("PreviousGoldenId").isNotNull()
            & (F.col("PreviousGoldenIdSource") == F.lit(GOLDEN_ID_SOURCE_ENGINE))
        )
        .select(
            "CountryCode",
            "TempClusterId",
            F.col("PreviousGoldenId").alias("candidate_id"),
            F.lit(2).alias("tier"),
            F.col("PreviousGoldenIdAssignedDate").alias("candidate_date"),
            F.lit(1).alias("incumbent"),
        )
    )
    candidates = (
        informatica_candidates.unionByName(engine_candidates)
        .groupBy("CountryCode", "TempClusterId", "candidate_id")
        .agg(
            F.min("tier").alias("tier"),
            F.count(F.lit(1)).alias("record_count"),
            F.sum("incumbent").alias("incumbent_count"),
            F.min("candidate_date").alias("candidate_date"),
        )
    )

    # --- 2. each candidate id is claimed by exactly one cluster -----------------------
    claim_window = Window.partitionBy("CountryCode", "candidate_id").orderBy(
        F.col("incumbent_count").desc(),
        F.col("record_count").desc(),
        F.col("candidate_date").asc_nulls_last(),
        F.col("TempClusterId").asc(),
    )
    claimed = (
        candidates.withColumn("_claim_rank", F.row_number().over(claim_window))
        .filter(F.col("_claim_rank") == F.lit(1))
        .drop("_claim_rank")
    )

    # --- 3. each cluster picks its best claimed id ------------------------------------
    choice_window = Window.partitionBy("CountryCode", "TempClusterId").orderBy(
        F.col("tier").asc(),
        F.col("candidate_date").asc_nulls_last(),
        F.col("candidate_id").asc(),
    )
    chosen = (
        claimed.withColumn("_choice_rank", F.row_number().over(choice_window))
        .filter(F.col("_choice_rank") == F.lit(1))
        .select(
            "CountryCode",
            "TempClusterId",
            F.col("candidate_id").cast("long").alias("golden_id"),
            F.when(F.col("tier") == F.lit(1), F.lit(GOLDEN_ID_SOURCE_INFORMATICA))
            .otherwise(F.lit(GOLDEN_ID_SOURCE_ENGINE))
            .alias("golden_id_source"),
            F.lit(False).alias("golden_id_is_new"),
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    # --- 4. mint ids for clusters without any reusable id ----------------------------
    clusters = cluster_rows.select("CountryCode", "TempClusterId").dropDuplicates(["CountryCode", "TempClusterId"])
    new_clusters = clusters.join(chosen.select("CountryCode", "TempClusterId"), ["CountryCode", "TempClusterId"], "left_anti").persist(
        StorageLevel.MEMORY_AND_DISK
    )
    new_cluster_count = new_clusters.count()
    if new_cluster_count > 0:
        base = _reserve_golden_id_range(spark, cfg, new_cluster_count)
        # Single-partition window: rows are (country, long) pairs only, so this stays cheap
        # even for a first run that mints an id for every unmatched record.
        mint_window = Window.orderBy(F.col("CountryCode").asc(), F.col("TempClusterId").asc())
        minted = new_clusters.select(
            "CountryCode",
            "TempClusterId",
            (F.lit(base) + F.row_number().over(mint_window) - F.lit(1)).cast("long").alias("golden_id"),
            F.lit(GOLDEN_ID_SOURCE_ENGINE).alias("golden_id_source"),
            F.lit(True).alias("golden_id_is_new"),
        )
        cluster_golden_ids = chosen.unionByName(minted)
    else:
        cluster_golden_ids = chosen
    print(f"  -> Golden ids: {new_cluster_count} newly minted cluster ids")

    result = (
        cluster_rows.join(cluster_golden_ids, ["CountryCode", "TempClusterId"], "inner")
        .select(
            "record_id",
            "TempClusterId",
            F.col("golden_id").cast("long").alias("golden_id"),
            "golden_id_source",
            "golden_id_is_new",
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    result.count()
    cluster_rows.unpersist()
    chosen.unpersist()
    new_clusters.unpersist()
    return result


def validate_source_golden_group_assignments(
    final_golden_ids_df: DataFrame,
    processed_df: DataFrame,
    cfg: Dict[str, Any],
    country_code: str,
) -> None:
    """Fail fast if an Informatica grouping was split, re-minted or (optionally) renamed.

    Only active with ``preserve_source_golden_groups``. For every non-null
    ``SourceGoldenRecordId`` in the country:

    - all its records must sit in exactly one component (no SPLIT of an Informatica group);
    - that component's golden id must be Informatica-sourced (never an engine-minted id);
    - unless ``allow_source_golden_group_merge`` is true, the golden id must equal the
      Informatica id itself (no id change at all, not even by merging two groups).

    Called before anything is written back to the registry, so a violation leaves no trace.
    """
    if not bool(cfg.get("preserve_source_golden_groups", True)):
        return
    allow_merge = bool(cfg.get("allow_source_golden_group_merge", False))

    rows = (
        processed_df.select(
            F.col("record_id").cast("long").alias("record_id"),
            F.col("SourceGoldenRecordId").cast("long").alias("SourceGoldenRecordId"),
        )
        .filter(F.col("SourceGoldenRecordId").isNotNull())
        .dropDuplicates(["record_id"])
        .join(final_golden_ids_df.select("record_id", "TempClusterId", "golden_id", "golden_id_source"), "record_id", "inner")
    )
    per_id = rows.groupBy("SourceGoldenRecordId").agg(
        F.countDistinct("TempClusterId").alias("component_count"),
        F.countDistinct("golden_id").alias("golden_id_count"),
        F.min("golden_id").alias("golden_id"),
        F.sum(F.when(F.col("golden_id_source") != F.lit(GOLDEN_ID_SOURCE_INFORMATICA), F.lit(1)).otherwise(F.lit(0))).alias(
            "engine_sourced_records"
        ),
    )
    violation = (F.col("component_count") > F.lit(1)) | (F.col("engine_sourced_records") > F.lit(0))
    if not allow_merge:
        violation = violation | (F.col("golden_id_count") > F.lit(1)) | (F.col("golden_id") != F.col("SourceGoldenRecordId"))
    violations = per_id.filter(violation)
    if not _is_empty(violations):
        sample = [r.asDict() for r in violations.limit(5).collect()]
        raise RuntimeError(
            f"Informatica golden groupings for {country_code} would not be preserved (sample: {sample}). "
            "With preserve_source_golden_groups every SourceGoldenRecordId must map to exactly one component whose "
            "golden id is Informatica-sourced"
            + ("" if allow_merge else " and equal to the SourceGoldenRecordId itself")
            + ". No golden ids were written to MDMRowRegistry / MDMMatchedResults; inspect MDMMatchLinks / "
            "MDMComponentLabels for this country."
        )


def persist_golden_id_assignments(final_golden_ids_df: DataFrame, cfg: Dict[str, Any], country_code: str) -> None:
    """Write chosen golden ids back to MDMRowRegistry (MDMGoldenId / Source / AssignedDate).

    ``MDMGoldenIdAssignedDate`` is only reset when the id actually changes, so tier-2
    tie-breaking ("earliest assigned") is stable across runs.
    """
    spark = final_golden_ids_df.sparkSession
    registry_table = cfg["rowRegistryTable"]
    view_name = "TmpMDMGoldenIdAssignments"
    (
        final_golden_ids_df.select(
            F.lit(country_code).alias("CountryCode"),
            F.col("record_id").cast("long").alias("MDMRowId"),
            F.col("golden_id").cast("long").alias("golden_id"),
            F.col("golden_id_source").cast("string").alias("golden_id_source"),
        )
        .dropDuplicates(["CountryCode", "MDMRowId"])
        .createOrReplaceTempView(view_name)
    )
    spark.sql(
        f"""
        MERGE INTO {registry_table} AS target
        USING {view_name} AS source
        ON target.CountryCode = source.CountryCode
           AND target.MDMRowId = source.MDMRowId
        WHEN MATCHED AND (
             target.MDMGoldenId IS NULL
          OR target.MDMGoldenId <> source.golden_id
          OR target.MDMGoldenIdSource IS NULL
          OR target.MDMGoldenIdSource <> source.golden_id_source
        ) THEN UPDATE SET
          target.MDMGoldenId = source.golden_id,
          target.MDMGoldenIdSource = source.golden_id_source,
          target.MDMGoldenIdAssignedDate = CASE
            WHEN target.MDMGoldenId IS NULL OR target.MDMGoldenId <> source.golden_id THEN current_timestamp()
            ELSE target.MDMGoldenIdAssignedDate
          END,
          target.DateUpdated = current_timestamp()
        """
    )


def build_golden_id_history(final_df: DataFrame, country_code: str) -> DataFrame:
    """Aggregate (old golden id -> new golden id) transitions for MDMGoldenIdHistory.

    ``old`` is the id the record carried before this run: the engine's prior assignment
    if any, else the Informatica id (so the first engine run also records how Informatica
    clusters were merged or split). Reason:
      - SPLIT: the old id survives as the golden id of another cluster in this run.
      - MERGE: the old id is retired (no cluster carries it any more).
    """
    old_id = F.coalesce(F.col("previous_golden_id"), F.col("SourceGoldenRecordId")).cast("long")
    transitions = (
        final_df.select(
            old_id.alias("OldGoldenId"),
            F.col("golden_id").cast("long").alias("NewGoldenId"),
        )
        .filter(F.col("OldGoldenId").isNotNull() & (F.col("OldGoldenId") != F.col("NewGoldenId")))
        .groupBy("OldGoldenId", "NewGoldenId")
        .agg(F.count(F.lit(1)).alias("RecordCount"))
    )
    surviving_ids = final_df.select(F.col("golden_id").cast("long").alias("OldGoldenId")).distinct().withColumn("_survives", F.lit(True))
    return (
        transitions.join(surviving_ids, "OldGoldenId", "left")
        .select(
            F.lit(country_code).alias("CountryCode"),
            "OldGoldenId",
            "NewGoldenId",
            F.when(F.col("_survives") == F.lit(True), F.lit(HISTORY_REASON_SPLIT)).otherwise(F.lit(HISTORY_REASON_MERGE)).alias("Reason"),
            F.col("RecordCount").cast("long").alias("RecordCount"),
            F.current_timestamp().alias("RunTimestamp"),
        )
    )


def append_golden_id_history(history_df: DataFrame, cfg: Dict[str, Any]) -> int:
    """Append transitions to MDMGoldenIdHistory; returns the number of rows appended."""
    history_df = history_df.persist(StorageLevel.MEMORY_AND_DISK)
    history_count = history_df.count()
    if history_count > 0:
        history_df.write.format("delta").mode("append").saveAsTable(cfg["goldenIdHistoryTable"])
    history_df.unpersist()
    return history_count
