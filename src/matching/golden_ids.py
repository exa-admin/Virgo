"""Assigning a golden id to each component, with Informatica continuity.

Informatica MDM is being replaced country by country, so an id it already issued must
survive. Per component ("cluster", keyed by TempClusterId):

1. Collect candidate ids from every record in the cluster —
   tier 1: the Informatica ``SourceGoldenRecordId``;
   tier 2: an engine id this pipeline assigned on a previous run.
2. An id can appear in several clusters (one split apart since the last run). Each id is
   *claimed* by exactly one cluster: most records that already carried it, then most
   records holding it, then earliest candidate date, then smallest TempClusterId.
3. Each cluster picks the best id it claimed: Informatica beats engine, then earliest
   date, then smallest id.
4. Clusters left with nothing are minted a fresh id from ``mdm_golden_id_sequence``.

Every tie-break is deterministic, so re-running on unchanged data yields identical ids.
Chosen ids are written back to ``mdm_row_registry`` (so the next run prefers them via
tier 2) and old -> new transitions are appended to ``mdm_golden_id_history``.

With ``preserve_source_golden_groups`` (the default), an Informatica group is hard-linked
before components run (``graph.build_group_links``), so step 2 never has to arbitrate an
Informatica id between clusters — and ``validate_group_assignments`` fails the run if it
ever happens.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from matching import read
from matching.config import GOLDEN_ID_SOURCE_ENGINE, GOLDEN_ID_SOURCE_INFORMATICA
from matching.helpers import is_empty, sql_literal

# Columns pipeline._attach_row_registry adds for the previous run's assignment.
PREVIOUS_GOLDEN_ID_COLUMN = "PreviousMDMGoldenId"
PREVIOUS_GOLDEN_ID_SOURCE_COLUMN = "PreviousMDMGoldenIdSource"
PREVIOUS_GOLDEN_ID_ASSIGNED_COLUMN = "PreviousMDMGoldenIdAssignedDate"

HISTORY_REASON_MERGE = "MERGE"
HISTORY_REASON_SPLIT = "SPLIT"


def collect_record_match_rules(match_links: DataFrame) -> DataFrame:
    """Per record, the comma-joined names of the rules that matched it."""
    return (
        match_links.filter(F.col("src_is_rule_subject")).select(F.col("src").alias("record_id"), "match_rule")
        .unionByName(match_links.filter(F.col("dst_is_rule_subject")).select(F.col("dst").alias("record_id"), "match_rule"))
        .dropDuplicates(["record_id", "match_rule"])
        .groupBy("record_id")
        .agg(F.concat_ws(", ", F.sort_array(F.collect_set("match_rule"))).alias("final_match_rule"))
    )


def _registry_bounds(spark: SparkSession, cfg: Dict[str, Any]) -> Dict[str, Optional[int]]:
    """Highest golden ids the registry knows, across all countries (one global id space)."""
    row = (
        read.read(spark, cfg, "row_registry")
        .agg(
            F.max(F.col("SourceGoldenRecordId").cast("long")).alias("max_source"),
            F.max(F.col("MDMGoldenId").cast("long")).alias("max_engine"),
        )
        .first()
    )
    return {"max_source": row["max_source"], "max_engine": row["max_engine"]}


def validate_golden_id_space(spark: SparkSession, cfg: Dict[str, Any]) -> None:
    """Fail fast if the engine and Informatica id ranges are not (or no longer) disjoint."""
    floor = int(cfg["golden_id_floor"])
    bounds = _registry_bounds(spark, cfg)
    if bounds["max_source"] is not None and bounds["max_source"] >= floor:
        raise ValueError(
            f"Informatica SourceGoldenRecordId values reach {bounds['max_source']}, which is >= golden_id_floor "
            f"{floor}. Engine-minted ids would no longer be guaranteed disjoint from Informatica ids. Raise "
            "'golden_id_floor' in the country config (and keep it identical for all countries)."
        )

    registry = read.read(spark, cfg, "row_registry")
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
    if not is_empty(collisions):
        sample = [r["golden_id"] for r in collisions.limit(5).collect()]
        raise ValueError(
            f"Engine-minted golden ids collide with Informatica SourceGoldenRecordId values (sample: {sample}). "
            "Informatica has produced ids inside the engine range; raise 'golden_id_floor' and remediate the "
            "affected mdm_row_registry rows before continuing."
        )


def _reserve_id_range(spark: SparkSession, cfg: Dict[str, Any], count: int) -> int:
    """Reserve ``count`` consecutive golden ids and return the first.

    Base = max(floor, sequence high-water mark, every id the registry knows + 1). The mark
    is advanced with a guarded MERGE and read back, so a concurrent run that raced on the
    same sequence is detected. A range burnt by a failed run just leaves a gap, like any
    database sequence.
    """
    if count <= 0:
        raise ValueError("count must be positive")
    sequence_table = cfg["goldenIdSequenceTable"]
    sequence_name = cfg["golden_id_sequence_name"]

    def read_mark() -> Optional[int]:
        row = (
            read.read(spark, cfg, "golden_id_sequence")
            .filter(F.col("SequenceName") == F.lit(sequence_name))
            .select(F.col("NextValue").cast("long").alias("NextValue"))
            .first()
        )
        return None if row is None or row["NextValue"] is None else int(row["NextValue"])

    mark = read_mark()
    bounds = _registry_bounds(spark, cfg)
    base = max(
        int(cfg["golden_id_floor"]),
        mark or 0,
        (bounds["max_source"] or 0) + 1,
        (bounds["max_engine"] or 0) + 1,
    )
    next_value = base + count
    guard = "target.NextValue IS NULL" if mark is None else f"target.NextValue = {mark}"

    spark.sql(
        f"""
        MERGE INTO {sequence_table} AS target
        USING (SELECT '{sql_literal(sequence_name)}' AS SequenceName, CAST({next_value} AS BIGINT) AS NextValue) AS source
        ON target.SequenceName = source.SequenceName
        WHEN MATCHED AND {guard} THEN UPDATE SET
          target.NextValue = source.NextValue,
          target.DateUpdated = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (SequenceName, NextValue, DateUpdated)
        VALUES (source.SequenceName, source.NextValue, current_timestamp())
        """
    )
    written = read_mark()
    if written != next_value:
        raise RuntimeError(
            f"Golden id range reservation failed for sequence '{sequence_name}' in {sequence_table}: expected "
            f"NextValue {next_value}, found {written}. Another run probably allocated ids concurrently; "
            "re-run this country."
        )
    print(f"  -> Reserved {count} golden ids [{base}, {next_value - 1}] from {sequence_table}")
    return base


def assign_golden_ids(processed: DataFrame, components: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    """One row per record_id: TempClusterId, golden_id, golden_id_source, golden_id_is_new."""
    spark = processed.sparkSession
    required = [
        "CountryCode",
        "record_id",
        "SourceGoldenRecordId",
        "GoldenIDCreatedDate",
        PREVIOUS_GOLDEN_ID_COLUMN,
        PREVIOUS_GOLDEN_ID_SOURCE_COLUMN,
        PREVIOUS_GOLDEN_ID_ASSIGNED_COLUMN,
    ]
    missing = [c for c in required if c not in processed.columns]
    if missing:
        raise ValueError(f"processed DataFrame is missing columns required for golden id assignment: {missing}")

    cluster_rows = (
        processed.select(
            "CountryCode",
            F.col("record_id").cast("long").alias("record_id"),
            F.col("SourceGoldenRecordId").cast("long").alias("SourceGoldenRecordId"),
            F.col("GoldenIDCreatedDate").cast("timestamp").alias("GoldenIDCreatedDate"),
            F.col(PREVIOUS_GOLDEN_ID_COLUMN).cast("long").alias("PreviousGoldenId"),
            F.col(PREVIOUS_GOLDEN_ID_SOURCE_COLUMN).cast("string").alias("PreviousGoldenIdSource"),
            F.col(PREVIOUS_GOLDEN_ID_ASSIGNED_COLUMN).cast("timestamp").alias("PreviousGoldenIdAssignedDate"),
        )
        .dropDuplicates(["record_id"])
        .join(components.select("record_id", F.col("golden_id").alias("TempClusterId")), "record_id", "left")
        # A record with no link is its own cluster.
        .withColumn("TempClusterId", F.coalesce(F.col("TempClusterId"), F.col("record_id")).cast("long"))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    # 1. Candidate ids per cluster.
    informatica = cluster_rows.filter(F.col("SourceGoldenRecordId").isNotNull()).select(
        "CountryCode",
        "TempClusterId",
        F.col("SourceGoldenRecordId").alias("candidate_id"),
        F.lit(1).alias("tier"),
        F.col("GoldenIDCreatedDate").alias("candidate_date"),
        F.when(F.col("PreviousGoldenId") == F.col("SourceGoldenRecordId"), F.lit(1)).otherwise(F.lit(0)).alias("incumbent"),
    )
    engine = cluster_rows.filter(
        F.col("PreviousGoldenId").isNotNull() & (F.col("PreviousGoldenIdSource") == F.lit(GOLDEN_ID_SOURCE_ENGINE))
    ).select(
        "CountryCode",
        "TempClusterId",
        F.col("PreviousGoldenId").alias("candidate_id"),
        F.lit(2).alias("tier"),
        F.col("PreviousGoldenIdAssignedDate").alias("candidate_date"),
        F.lit(1).alias("incumbent"),
    )
    candidates = (
        informatica.unionByName(engine)
        .groupBy("CountryCode", "TempClusterId", "candidate_id")
        .agg(
            F.min("tier").alias("tier"),
            F.count(F.lit(1)).alias("record_count"),
            F.sum("incumbent").alias("incumbent_count"),
            F.min("candidate_date").alias("candidate_date"),
        )
    )

    # 2. Each candidate id is claimed by exactly one cluster.
    claim_order = Window.partitionBy("CountryCode", "candidate_id").orderBy(
        F.col("incumbent_count").desc(),
        F.col("record_count").desc(),
        F.col("candidate_date").asc_nulls_last(),
        F.col("TempClusterId").asc(),
    )
    # 3. Each cluster picks its best claimed id.
    choice_order = Window.partitionBy("CountryCode", "TempClusterId").orderBy(
        F.col("tier").asc(),
        F.col("candidate_date").asc_nulls_last(),
        F.col("candidate_id").asc(),
    )
    chosen = (
        candidates.withColumn("_claim_rank", F.row_number().over(claim_order))
        .filter(F.col("_claim_rank") == F.lit(1))
        .withColumn("_choice_rank", F.row_number().over(choice_order))
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

    # 4. Mint ids for clusters with nothing to reuse.
    new_clusters = (
        cluster_rows.select("CountryCode", "TempClusterId")
        .dropDuplicates(["CountryCode", "TempClusterId"])
        .join(chosen.select("CountryCode", "TempClusterId"), ["CountryCode", "TempClusterId"], "left_anti")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    new_count = new_clusters.count()
    print(f"  -> Golden ids: {new_count} newly minted cluster ids")
    if new_count > 0:
        base = _reserve_id_range(spark, cfg, new_count)
        # Single-partition window, but the rows are only (country, long) pairs.
        mint_order = Window.orderBy(F.col("CountryCode").asc(), F.col("TempClusterId").asc())
        cluster_golden_ids = chosen.unionByName(
            new_clusters.select(
                "CountryCode",
                "TempClusterId",
                (F.lit(base) + F.row_number().over(mint_order) - F.lit(1)).cast("long").alias("golden_id"),
                F.lit(GOLDEN_ID_SOURCE_ENGINE).alias("golden_id_source"),
                F.lit(True).alias("golden_id_is_new"),
            )
        )
    else:
        cluster_golden_ids = chosen

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


def validate_group_assignments(
    golden_ids: DataFrame,
    processed: DataFrame,
    cfg: Dict[str, Any],
    country_code: str,
) -> None:
    """Fail fast if an Informatica grouping was split, re-minted or (optionally) renamed.

    For every non-null ``SourceGoldenRecordId`` in the country: all its records must sit in
    one component, that component's id must be Informatica-sourced, and — unless
    ``allow_source_golden_group_merge`` — it must be the Informatica id itself.

    Runs before anything is written back, so a violation leaves no trace.
    """
    if not bool(cfg["preserve_source_golden_groups"]):
        return
    allow_merge = bool(cfg["allow_source_golden_group_merge"])

    per_group = (
        processed.select(
            F.col("record_id").cast("long").alias("record_id"),
            F.col("SourceGoldenRecordId").cast("long").alias("SourceGoldenRecordId"),
        )
        .filter(F.col("SourceGoldenRecordId").isNotNull())
        .dropDuplicates(["record_id"])
        .join(golden_ids.select("record_id", "TempClusterId", "golden_id", "golden_id_source"), "record_id", "inner")
        .groupBy("SourceGoldenRecordId")
        .agg(
            F.countDistinct("TempClusterId").alias("component_count"),
            F.countDistinct("golden_id").alias("golden_id_count"),
            F.min("golden_id").alias("golden_id"),
            F.sum(F.when(F.col("golden_id_source") != F.lit(GOLDEN_ID_SOURCE_INFORMATICA), F.lit(1)).otherwise(F.lit(0)))
            .alias("engine_sourced_records"),
        )
    )

    violation = (F.col("component_count") > F.lit(1)) | (F.col("engine_sourced_records") > F.lit(0))
    if not allow_merge:
        violation = violation | (F.col("golden_id_count") > F.lit(1)) | (F.col("golden_id") != F.col("SourceGoldenRecordId"))

    violations = per_group.filter(violation)
    if not is_empty(violations):
        sample = [r.asDict() for r in violations.limit(5).collect()]
        raise RuntimeError(
            f"Informatica golden groupings for {country_code} would not be preserved (sample: {sample}). "
            "With preserve_source_golden_groups every SourceGoldenRecordId must map to exactly one component whose "
            "golden id is Informatica-sourced"
            + ("" if allow_merge else " and equal to the SourceGoldenRecordId itself")
            + ". No golden ids were written to mdm_row_registry / mdm_matched_results; inspect mdm_match_links / "
            "mdm_component_labels for this country."
        )


def persist_assignments(golden_ids: DataFrame, cfg: Dict[str, Any], country_code: str) -> None:
    """Write the chosen ids back to mdm_row_registry.

    ``MDMGoldenIdAssignedDate`` only moves when the id actually changes, so the tier-2
    "earliest assigned" tie-break stays stable across runs.
    """
    spark = golden_ids.sparkSession
    view = "tmp_mdm_golden_id_assignments"
    golden_ids.select(
        F.lit(country_code).alias("CountryCode"),
        F.col("record_id").cast("long").alias("MDMRowId"),
        F.col("golden_id").cast("long").alias("golden_id"),
        F.col("golden_id_source").cast("string").alias("golden_id_source"),
    ).dropDuplicates(["CountryCode", "MDMRowId"]).createOrReplaceTempView(view)

    spark.sql(
        f"""
        MERGE INTO {cfg["rowRegistryTable"]} AS target
        USING {view} AS source
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


# Standardized attributes captured on both sides of a change, so a reviewer can see what
# the record looked like when it got each id.
_TRACED_ATTRIBUTES = [("c_name", "Name"), ("c_address", "Address"), ("c_city", "City"), ("c_zip", "Zip")]


def save_change_log(final_df: DataFrame, cfg: Dict[str, Any], country_code: str) -> int:
    """Trace every OperatorConcatId whose golden id changed, and why.

    ``mdm_golden_id_history`` records id-level remaps; this is the record-level counterpart:
    one row per operator key that moved, carrying the previous run's matching data, this
    run's, and a reason derived from what actually differs.

    Reads the previous run's mdm_matched_results slice, so it MUST be called before that
    slice is overwritten. On the first run there is nothing to compare and it writes
    nothing.
    """
    spark = final_df.sparkSession
    key_column = cfg["rowRegistryKeyColumn"]
    previous_columns = ["golden_id", "golden_id_source", "SourceGoldenRecordId", "final_match_rule", "match_group_size"]

    previous = read.read(spark, cfg, "matched_results").where(f"CountryCode = '{sql_literal(country_code)}'")

    # The traced attributes and MatchRunTimestamp reach mdm_matched_results through
    # mergeSchema on the first write, so they are absent from a table that only the setup
    # DDL has created. Selecting them unconditionally fails analysis on the very first run
    # of a country, before there is anything to compare against anyway.
    def prior(column: str, data_type: str):
        return F.col(column) if column in previous.columns else F.lit(None).cast(data_type)

    previous = previous.select(
        F.col(key_column).alias("_key"),
        *[F.col(c).alias(f"prev_{c}") for c in previous_columns],
        *[prior(source, "string").alias(f"prev_{alias}") for source, alias in _TRACED_ATTRIBUTES],
        prior("MatchRunTimestamp", "timestamp").alias("prev_run"),
    ).dropDuplicates(["_key"])

    changed = final_df.filter(F.col("golden_id_changed")).join(
        previous, F.col(key_column) == F.col("_key"), "left"
    )

    attributes_changed = None
    for source, alias in _TRACED_ATTRIBUTES:
        differs = ~F.coalesce(F.col(f"prev_{alias}"), F.lit("")).eqNullSafe(F.coalesce(F.col(source), F.lit("")))
        attributes_changed = differs if attributes_changed is None else (attributes_changed | differs)

    # First matching cause wins. Ordered root cause before symptom: a record that was
    # renamed into an Informatica group changed because of the rename, not because the id
    # it landed on happens to be Informatica's.
    reason = (
        F.when(
            F.col("SourceGoldenRecordId").isNotNull()
            & ~F.col("SourceGoldenRecordId").eqNullSafe(F.col("prev_SourceGoldenRecordId")),
            F.lit("INFORMATICA_ID_CHANGED"),
        )
        .when(F.coalesce(attributes_changed, F.lit(False)), F.lit("SOURCE_DATA_CHANGED"))
        .when(F.col("match_group_size") > F.col("prev_match_group_size"), F.lit("GROUP_GREW"))
        .when(F.col("match_group_size") < F.col("prev_match_group_size"), F.lit("GROUP_SHRANK"))
        .when(
            (F.col("golden_id_source") == F.lit(GOLDEN_ID_SOURCE_INFORMATICA))
            & (F.col("prev_golden_id_source") == F.lit(GOLDEN_ID_SOURCE_ENGINE)),
            F.lit("INFORMATICA_ID_ADOPTED"),
        )
        .when(~F.col("final_match_rule").eqNullSafe(F.col("prev_final_match_rule")), F.lit("MATCH_RULE_CHANGED"))
        .when(F.col("prev_golden_id").isNull(), F.lit("NO_PREVIOUS_RESULT"))
        .otherwise(F.lit("REASSIGNED"))
    )

    log = changed.select(
        F.lit(country_code).alias("CountryCode"),
        F.col(key_column).cast("string").alias("OperatorConcatId"),
        F.col("MDMRowId").cast("long").alias("MDMRowId"),
        reason.alias("ChangeReason"),
        F.col("previous_golden_id").cast("long").alias("PreviousGoldenId"),
        F.col("golden_id").cast("long").alias("NewGoldenId"),
        F.col("previous_golden_id_source").cast("string").alias("PreviousGoldenIdSource"),
        F.col("golden_id_source").cast("string").alias("NewGoldenIdSource"),
        F.col("prev_SourceGoldenRecordId").cast("long").alias("PreviousSourceGoldenRecordId"),
        F.col("SourceGoldenRecordId").cast("long").alias("SourceGoldenRecordId"),
        F.col("prev_final_match_rule").cast("string").alias("PreviousMatchRule"),
        F.col("final_match_rule").cast("string").alias("NewMatchRule"),
        F.col("prev_match_group_size").cast("long").alias("PreviousMatchGroupSize"),
        F.col("match_group_size").cast("long").alias("NewMatchGroupSize"),
        *[F.col(f"prev_{alias}").cast("string").alias(f"Previous{alias}") for _, alias in _TRACED_ATTRIBUTES],
        *[F.col(source).cast("string").alias(f"New{alias}") for source, alias in _TRACED_ATTRIBUTES],
        F.col("prev_run").cast("timestamp").alias("PreviousRunTimestamp"),
        F.current_timestamp().alias("RunTimestamp"),
    ).persist(StorageLevel.MEMORY_AND_DISK)

    count = log.count()
    if count > 0:
        log.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(cfg["changeLogTable"])
    log.unpersist()
    return count


def save_history(final_df: DataFrame, cfg: Dict[str, Any], country_code: str) -> int:
    """Append this run's old -> new golden id transitions to mdm_golden_id_history.

    ``old`` is the id the record carried before this run: the engine's prior assignment if
    any, else the Informatica id (so the first engine run also records how Informatica
    clusters were merged or split). SPLIT means the old id still labels another cluster;
    MERGE means it was retired.
    """
    old_id = F.coalesce(F.col("previous_golden_id"), F.col("SourceGoldenRecordId")).cast("long")
    transitions = (
        final_df.select(old_id.alias("OldGoldenId"), F.col("golden_id").cast("long").alias("NewGoldenId"))
        .filter(F.col("OldGoldenId").isNotNull() & (F.col("OldGoldenId") != F.col("NewGoldenId")))
        .groupBy("OldGoldenId", "NewGoldenId")
        .agg(F.count(F.lit(1)).alias("RecordCount"))
    )
    surviving = (
        final_df.select(F.col("golden_id").cast("long").alias("OldGoldenId")).distinct().withColumn("_survives", F.lit(True))
    )
    history = (
        transitions.join(surviving, "OldGoldenId", "left")
        .select(
            F.lit(country_code).alias("CountryCode"),
            "OldGoldenId",
            "NewGoldenId",
            F.when(F.col("_survives") == F.lit(True), F.lit(HISTORY_REASON_SPLIT))
            .otherwise(F.lit(HISTORY_REASON_MERGE))
            .alias("Reason"),
            F.col("RecordCount").cast("long").alias("RecordCount"),
            F.current_timestamp().alias("RunTimestamp"),
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    count = history.count()
    if count > 0:
        history.write.format("delta").mode("append").saveAsTable(cfg["goldenIdHistoryTable"])
    history.unpersist()
    return count
