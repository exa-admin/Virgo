-- =============================================================================
-- MDM Match engine — Delta table DDL
-- =============================================================================
-- Schema placeholder matches Python defaults in matching.config.DEFAULT_TARGET_SCHEMA.
-- Parameterize catalog/schema per environment before running, e.g.:
--   REPLACE pds_auroradsar_prod.schema_informatica WITH <catalog>.<schema>
--
-- Source views (sl_bdl_processed_cd_prod.cd.vw_ufsoperator and ...vw_ufsoperatorgoden,
-- configured via src/matching/conf/base.json source/golden_source) and enrichment table
-- (mdmenrichedoperators) are externally populated and are NOT created here.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS pds_auroradsar_prod.schema_informatica;

-- -----------------------------------------------------------------------------
-- MDMRowRegistry
-- Stable per-country surrogate keys (MDMRowId) for OperatorConcatId, plus the
-- golden id crosswalk used for Informatica -> engine continuity:
--   SourceGoldenRecordId     last NON-NULL Informatica GoldenRecordId seen for the
--                            row (never downgraded to NULL once Informatica stops
--                            populating it for a migrated country). The source column
--                            is a numeric STRING; the engine fails the run if a
--                            non-blank value is not an integer string, so no id is
--                            silently lost in the BIGINT cast.
--   MDMGoldenId              golden id the engine assigned on its last run
--   MDMGoldenIdSource        'INFORMATICA' (reused Informatica id) | 'ENGINE' (minted)
--   MDMGoldenIdAssignedDate  when MDMGoldenId last changed (tie-breaker for reuse)
-- MERGE in the engine inserts new keys, updates SourceGoldenRecordId only when the
-- source carries a non-null value, and writes MDMGoldenId* after golden id assignment.
--
-- Databricks IDENTITY columns (GENERATED ALWAYS AS IDENTITY):
--   - Supported on Delta tables in Databricks Runtime with Unity Catalog / HHM.
--   - The MERGE INSERT path deliberately omits MDMRowId so the identity generator
--     assigns values for new rows.
--   - If IDENTITY CREATE is not available in your workspace, create MDMRowId as
--     BIGINT NOT NULL and pre-populate / generate IDs before MERGE, or use a
--     sequence + DEFAULT — the engine expects MDMRowId to be present after MERGE.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pds_auroradsar_prod.schema_informatica.MDMRowRegistry (
  CountryCode           STRING        NOT NULL,
  MDMRowId              BIGINT        GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
  OperatorConcatId      STRING        NOT NULL,
  SourceGoldenRecordId  BIGINT,
  GoldenIDCreatedDate   TIMESTAMP,
  MDMGoldenId             BIGINT,
  MDMGoldenIdSource       STRING,
  MDMGoldenIdAssignedDate TIMESTAMP,
  DateCreated           TIMESTAMP,
  DateUpdated           TIMESTAMP
) USING DELTA
COMMENT 'Stable MDM row keys per country / OperatorConcatId + golden id crosswalk';

-- Migration for deployments created before the golden id crosswalk existed:
-- ALTER TABLE pds_auroradsar_prod.schema_informatica.MDMRowRegistry
--   ADD COLUMNS (MDMGoldenId BIGINT, MDMGoldenIdSource STRING, MDMGoldenIdAssignedDate TIMESTAMP);
--
-- Optional one-off backfill so ids already published from MDMMatchedResults by the
-- previous (offset-based) engine are carried forward instead of re-minted. Only do
-- this if downstream systems consumed those ids; review before running.
-- MERGE INTO pds_auroradsar_prod.schema_informatica.MDMRowRegistry AS t
-- USING (
--   SELECT CountryCode, MDMRowId, golden_id, SourceGoldenRecordId
--   FROM pds_auroradsar_prod.schema_informatica.MDMMatchedResults
-- ) AS s
-- ON t.CountryCode = s.CountryCode AND t.MDMRowId = s.MDMRowId
-- WHEN MATCHED AND t.MDMGoldenId IS NULL THEN UPDATE SET
--   t.MDMGoldenId = s.golden_id,
--   t.MDMGoldenIdSource = CASE WHEN s.golden_id = s.SourceGoldenRecordId THEN 'INFORMATICA' ELSE 'ENGINE' END,
--   t.MDMGoldenIdAssignedDate = current_timestamp();
-- NOTE: offset-based ids (100000000 + MDMRowId) may sit inside the Informatica id
-- range; validate_golden_id_space will refuse to run if any collide.

-- -----------------------------------------------------------------------------
-- MDMGoldenIdSequence — high-water mark for engine-minted golden ids
-- One global row (SequenceName = 'MDMGoldenId'); ids are a single space across
-- countries. The engine reserves ranges with a conditional MERGE and reads back.
-- Seed the row at or above the configured golden_id_floor (default 100000000 = 1e8;
-- Informatica GoldenRecordId is ~1e7 and grows slowly); the engine also raises the
-- base above max(SourceGoldenRecordId) / max(MDMGoldenId) automatically. Because the
-- allocator takes max(floor, NextValue, ...), an existing higher NextValue is never
-- lowered — the floor may only ever be raised.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pds_auroradsar_prod.schema_informatica.MDMGoldenIdSequence (
  SequenceName   STRING     NOT NULL,
  NextValue      BIGINT     NOT NULL,
  DateUpdated    TIMESTAMP
) USING DELTA
COMMENT 'Golden id allocator high-water mark (next unassigned value)';

-- Idempotent seed (optional: the engine inserts the row itself on first allocation).
MERGE INTO pds_auroradsar_prod.schema_informatica.MDMGoldenIdSequence AS t
USING (SELECT 'MDMGoldenId' AS SequenceName, CAST(100000000 AS BIGINT) AS NextValue) AS s
ON t.SequenceName = s.SequenceName
WHEN NOT MATCHED THEN INSERT (SequenceName, NextValue, DateUpdated)
VALUES (s.SequenceName, s.NextValue, current_timestamp());

-- -----------------------------------------------------------------------------
-- MDMGoldenIdHistory — append-only XREF of golden id transitions per run
--   Reason = 'MERGE' : OldGoldenId retired, all its records now carry NewGoldenId
--   Reason = 'SPLIT' : OldGoldenId still lives on another cluster; RecordCount
--                      records moved from it to NewGoldenId (record-level detail is
--                      in MDMMatchedResults.previous_golden_id for that run)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pds_auroradsar_prod.schema_informatica.MDMGoldenIdHistory (
  CountryCode    STRING     NOT NULL,
  OldGoldenId    BIGINT     NOT NULL,
  NewGoldenId    BIGINT     NOT NULL,
  Reason         STRING     NOT NULL,
  RecordCount    BIGINT,
  RunTimestamp   TIMESTAMP  NOT NULL
) USING DELTA
COMMENT 'Golden id remaps (old -> new) for downstream crosswalks';

-- -----------------------------------------------------------------------------
-- operator_golden_changelog — record-level golden id trace (AUDIT, append-only)
-- MDMGoldenIdHistory answers "which ids remapped"; this answers "what happened to THIS
-- OperatorConcatId, and why". One row per operator key whose golden id changed in a run,
-- carrying the previous run's matching data next to the current run's.
--
-- ChangeReason (first matching cause wins, root causes before their symptoms):
--   INFORMATICA_ID_CHANGED  Informatica issued a different GoldenRecordId for the row
--   SOURCE_DATA_CHANGED     name / address / city / zip changed, so the rules saw
--                           different input than last run — the usual root cause
--   GROUP_GREW              more records in the group now (a merge pulled the row over)
--   GROUP_SHRANK            fewer records in the group now (a split)
--   INFORMATICA_ID_ADOPTED  row had an engine-minted id and now sits under an Informatica
--                           one, with nothing else changed
--   MATCH_RULE_CHANGED      same group size, but a different rule linked the row
--   NO_PREVIOUS_RESULT      registry knew a prior id but MDMMatchedResults had no row
--                           (results table was cleared, or first run after an upgrade)
--   REASSIGNED              id moved with nothing else observably different — inspect
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pds_auroradsar_prod.schema_informatica.operator_golden_changelog (
  CountryCode                   STRING,
  OperatorConcatId              STRING,   -- the primary key being traced
  MDMRowId                      BIGINT,
  ChangeReason                  STRING,
  PreviousGoldenId              BIGINT,
  NewGoldenId                   BIGINT,
  PreviousGoldenIdSource        STRING,
  NewGoldenIdSource             STRING,
  PreviousSourceGoldenRecordId  BIGINT,
  SourceGoldenRecordId          BIGINT,
  PreviousMatchRule             STRING,   -- ---- previous matching data ----
  PreviousMatchGroupSize        BIGINT,
  PreviousName                  STRING,
  PreviousAddress               STRING,
  PreviousCity                  STRING,
  PreviousZip                   STRING,
  NewMatchRule                  STRING,   -- ---- current matching data ----
  NewMatchGroupSize             BIGINT,
  NewName                       STRING,
  NewAddress                    STRING,
  NewCity                       STRING,
  NewZip                        STRING,
  PreviousRunTimestamp          TIMESTAMP,
  RunTimestamp                  TIMESTAMP
) USING DELTA
COMMENT 'Record-level trace of golden id changes per OperatorConcatId, with cause';

-- -----------------------------------------------------------------------------
-- MDMRuleResults — per-rule match links (stewardship / audit)
-- Stages written by the engine:
--   000_Source_GoldenRecordId   RuleType 'source_golden' — Informatica grouping edges
--                               (preserve_source_golden_groups); match_key = the
--                               Informatica id, rule_priority 0, edge_type 'source_golden'
--   001_.. NNN_<rule_name>      RuleType 'exact' | 'fuzzy' — accepted waterfall edges
--   999_Blocked_Source_GoldenRecordId_Merge
--                               RuleType 'blocked' — engine edges DROPPED because they
--                               would merge two Informatica groups
--                               (allow_source_golden_group_merge = false). Not part of
--                               MDMMatchLinks. blocked_source_group_merge = true and
--                               src/dst_source_golden_id give the Informatica group each
--                               endpoint belongs to (resolved group for transitive bridges).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pds_auroradsar_prod.schema_informatica.MDMRuleResults (
  CountryCode                 STRING,
  RuleType                    STRING,
  RuleStageName               STRING,
  RuleExecutionOrder          INT,
  src                         BIGINT,
  dst                         BIGINT,
  SrcOperatorConcatId         STRING,
  DstOperatorConcatId         STRING,
  match_rule                  STRING,
  rule_priority               INT,
  edge_type                   STRING,
  block_name                  STRING,
  match_key                   STRING,
  src_is_rule_subject         BOOLEAN,
  dst_is_rule_subject         BOOLEAN,
  blocked_source_group_merge  BOOLEAN,  -- only set (true) on the 999_Blocked_* stage
  src_source_golden_id        BIGINT,   -- Informatica group of src (blocked stage only)
  dst_source_golden_id        BIGINT    -- Informatica group of dst (blocked stage only)
) USING DELTA
COMMENT 'Materialized match links per rule stage (+ blocked Informatica group bridges)';
-- Existing deployments: the engine writes with mergeSchema=true, so the three blocked-*
-- columns are added automatically on the first run; or run
-- ALTER TABLE pds_auroradsar_prod.schema_informatica.MDMRuleResults
--   ADD COLUMNS (blocked_source_group_merge BOOLEAN, src_source_golden_id BIGINT, dst_source_golden_id BIGINT);

-- -----------------------------------------------------------------------------
-- MDMRuleEvaluations — fuzzy candidate evidence (matched and non-matched)
-- Similarity columns are stored as INT percentages (0–100) by the engine.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pds_auroradsar_prod.schema_informatica.MDMRuleEvaluations (
  CountryCode                     STRING,
  RuleType                        STRING,
  RuleStageName                   STRING,
  RuleExecutionOrder              INT,
  src                             BIGINT,
  dst                             BIGINT,
  SrcOperatorConcatId             STRING,
  DstOperatorConcatId             STRING,
  SrcName                         STRING,
  DstName                         STRING,
  SrcAddress                      STRING,
  DstAddress                      STRING,
  SrcCity                         STRING,
  DstCity                         STRING,
  SrcState                        STRING,
  DstState                        STRING,
  SrcZip                          STRING,
  DstZip                          STRING,
  SrcComparedValues               STRING,
  DstComparedValues               STRING,
  match_rule                      STRING,
  rule_priority                   INT,
  edge_type                       STRING,
  block_name                      STRING,
  match_key                       STRING,
  src_is_rule_subject             BOOLEAN,
  dst_is_rule_subject             BOOLEAN,
  NameLevenshteinSimilarity       INT,
  AddressLevenshteinSimilarity    INT,
  AddressTokenJaccardSimilarity   INT,
  AddressBestSimilarity           INT,
  CityLevenshteinSimilarity       INT,
  StateLevenshteinSimilarity      INT,
  ZipExactMatch                   BOOLEAN,
  NameConditionPassed             BOOLEAN,
  AddressConditionPassed          BOOLEAN,
  CityConditionPassed             BOOLEAN,
  StateConditionPassed            BOOLEAN,
  ZipConditionPassed              BOOLEAN,
  IsMatched                       BOOLEAN
) USING DELTA
COMMENT 'Fuzzy rule candidate evaluations with similarity evidence';

-- -----------------------------------------------------------------------------
-- MDMMatchExclusions — stewardship exclusions from matching
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pds_auroradsar_prod.schema_informatica.MDMMatchExclusions (
  CountryCode        STRING        NOT NULL,
  OperatorConcatId   STRING        NOT NULL,
  ExclusionDate      TIMESTAMP,
  ExclusionReason    STRING
) USING DELTA
COMMENT 'Records excluded from match (stewardship)';

-- -----------------------------------------------------------------------------
-- MDMMatchingState — intermediate record-id sets for waterfall / grouping
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pds_auroradsar_prod.schema_informatica.MDMMatchingState (
  CountryCode   STRING,
  StateName     STRING,
  StateType     STRING,
  record_id     BIGINT
) USING DELTA
COMMENT 'Ephemeral matching state slices (active subjects, matched ids, etc.)';

-- -----------------------------------------------------------------------------
-- MDMMatchLinks — final undirected match edges for a country run
-- Includes the Source_GoldenRecordId edges (edge_type 'source_golden') and excludes
-- edges dropped as Informatica group bridges (see MDMRuleResults 999_Blocked_* stage).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pds_auroradsar_prod.schema_informatica.MDMMatchLinks (
  CountryCode           STRING,
  src                   BIGINT,
  dst                   BIGINT,
  match_rule            STRING,
  rule_priority         INT,
  edge_type             STRING,
  block_name            STRING,
  match_key             STRING,
  src_is_rule_subject   BOOLEAN,
  dst_is_rule_subject   BOOLEAN
) USING DELTA
COMMENT 'Country-scoped match link graph edges';

-- -----------------------------------------------------------------------------
-- MDMComponentLabels — connected-component label iterations
-- LabelStageName: labels_initial / labels_iter_NNN (min-label propagation);
--   source_group_labels_initial / source_group_labels_iter_NNN (seeded propagation of
--   Informatica ids inside components that bridged several groups; golden_id = the
--   Informatica id label, NULL until reached) and labels_source_groups_resolved
--   (IterationNumber 9999; final labels after dropping the bridging edges). The
--   source_group_* stages only exist when allow_source_golden_group_merge = false
--   and a bridge was actually detected.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pds_auroradsar_prod.schema_informatica.MDMComponentLabels (
  CountryCode       STRING,
  LabelStageName    STRING,
  IterationNumber   INT,
  record_id         BIGINT,
  golden_id         BIGINT
) USING DELTA
COMMENT 'Iterative min-label propagation checkpoints';

-- -----------------------------------------------------------------------------
-- MDMMatchedResults — country-partitioned matched output (mergeSchema=true)
-- Engine overwrites WHERE CountryCode = 'XX' and may widen schema from source.
-- Minimal required column: CountryCode. Other columns come from the source
-- plus engine-added match attributes.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pds_auroradsar_prod.schema_informatica.MDMMatchedResults (
  CountryCode                   STRING,
  record_id                     BIGINT,
  MDMRowId                      BIGINT,
  OperatorConcatId              STRING,
  SourceGoldenRecordId          BIGINT,   -- Informatica id known for the row (from registry)
  golden_id                     BIGINT,
  golden_id_source              STRING,   -- 'INFORMATICA' | 'ENGINE'
  golden_id_is_new              BOOLEAN,  -- minted in this run
  previous_golden_id            BIGINT,   -- engine assignment from the prior run (NULL on first run)
  previous_golden_id_source     STRING,
  golden_id_changed             BOOLEAN,  -- previous_golden_id IS NOT NULL AND <> golden_id
  golden_id_differs_from_source BOOLEAN,  -- SourceGoldenRecordId IS NOT NULL AND <> golden_id
                                          -- (always false when preserve_source_golden_groups
                                          --  and allow_source_golden_group_merge = false)
  final_match_rule              STRING,   -- rules that linked the record; includes
                                          -- 'Source_GoldenRecordId' for Informatica groups,
                                          -- 'Excluded from Match[, Source_GoldenRecordId]'
                                          -- for excluded records, 'Self/No Match' for singles
  is_matched                    BOOLEAN,
  is_group_anchor               BOOLEAN,
  match_group_size              BIGINT,
  -- Standardization inputs, always written (null-filled when a source lacks them). Declared
  -- here rather than left to mergeSchema so the comparison views below are valid on a fresh
  -- environment, before the first run has widened the table.
  OperatorName                  STRING,
  HouseNumberText               STRING,
  StreetText                    STRING,
  CityText                      STRING,
  StateText                     STRING,
  ZipCode                       STRING,
  engine_match_id               BIGINT,   -- component id from OUR RULES ALONE, before the
                                          -- Informatica groupings are hard-linked in. This is
                                          -- what the over/undermatch views compare against:
                                          -- golden_id cannot disagree with Informatica once
                                          -- preserve_source_golden_groups forces those groups in.
  MatchRunTimestamp             TIMESTAMP -- when this row was produced (drives changelog "previous run")
) USING DELTA
COMMENT 'Matched results per country (schema widened at write via mergeSchema)';
-- Existing deployments: mergeSchema=true adds the new columns on the next run.

-- -----------------------------------------------------------------------------
-- Externally populated (not created here):
--   sl_bdl_processed_cd_prod.cd.vw_ufsoperator      — source operators (population)
--   sl_bdl_processed_cd_prod.cd.vw_ufsoperatorgoden — golden masters (already matched)
--   pds_auroradsar_prod.schema_informatica.mdmenrichedoperators
--       Expected join key: OperatorConcatId
--       Columns used when EnrichDate=true (see ENRICHMENT_COLUMN_MAPPINGS):
--         OperatorName, HouseNumberText, StreetText, CityText, StateText,
--         CountryName, latitude, longitude, ZipCode
--       Optional filter column: match_found
-- -----------------------------------------------------------------------------

-- =============================================================================
-- Informatica comparison views (AUDIT — hand these to the Informatica team)
-- =============================================================================
-- Both compare Informatica's SourceGoldenRecordId against `engine_match_id`, the
-- grouping OUR RULES produced on their own.
--
-- Why not golden_id? With preserve_source_golden_groups = true (the default),
-- Informatica's groupings are hard-linked into the graph before components run, so
-- golden_id can never disagree with Informatica — comparing it would always report
-- zero differences. engine_match_id is the engine's independent opinion, so it is the
-- only honest basis for the comparison.
--
-- Records the engine deliberately skipped (invalid / dummy names, MDMMatchExclusions)
-- have no engine links and so sit alone in engine_match_id. They are kept in the views
-- and flagged by engine_skipped_record, because "Informatica matched a record we refuse
-- to match" is usually worth seeing — filter it out when you only want rule disagreements.

-- -----------------------------------------------------------------------------
-- vw_informatica_undermatch — WE matched them, Informatica did NOT.
-- Informatica is missing a match: these operators belong together.
--   INFORMATICA_SPLIT_GROUPS  our group spans two or more Informatica golden ids
--   INFORMATICA_MISSING_MEMBER  our group has one Informatica id plus records it never
--                               gave an id to
--   INFORMATICA_NEVER_MATCHED   our group is entirely unknown to Informatica
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW pds_auroradsar_prod.schema_informatica.vw_informatica_undermatch AS
WITH engine_groups AS (
  SELECT
    CountryCode,
    engine_match_id,
    COUNT(*)                                                            AS engine_group_size,
    COUNT(DISTINCT SourceGoldenRecordId)                                AS informatica_ids_in_group,
    SUM(CASE WHEN SourceGoldenRecordId IS NULL THEN 1 ELSE 0 END)       AS records_without_informatica_id
  FROM pds_auroradsar_prod.schema_informatica.MDMMatchedResults
  GROUP BY CountryCode, engine_match_id
)
SELECT
  r.CountryCode,
  r.engine_match_id,
  g.engine_group_size,
  g.informatica_ids_in_group,
  CASE
    WHEN g.informatica_ids_in_group > 1 THEN 'INFORMATICA_SPLIT_GROUPS'
    WHEN g.informatica_ids_in_group = 1 THEN 'INFORMATICA_MISSING_MEMBER'
    ELSE 'INFORMATICA_NEVER_MATCHED'
  END                                                                   AS undermatch_type,
  r.OperatorConcatId,
  r.SourceGoldenRecordId,
  r.golden_id,
  r.OperatorName,
  r.StreetText,
  r.CityText,
  r.ZipCode,
  r.StateText,
  r.final_match_rule,
  r.final_match_rule LIKE 'Excluded from Match%'                        AS engine_skipped_record,
  r.MatchRunTimestamp
FROM pds_auroradsar_prod.schema_informatica.MDMMatchedResults r
JOIN engine_groups g
  ON r.CountryCode = g.CountryCode
 AND r.engine_match_id = g.engine_match_id
WHERE g.engine_group_size > 1
  AND (g.informatica_ids_in_group > 1 OR g.records_without_informatica_id > 0);

-- -----------------------------------------------------------------------------
-- vw_informatica_overmatch — Informatica matched them, WE did NOT.
-- Informatica has grouped operators our rules found no evidence to link.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW pds_auroradsar_prod.schema_informatica.vw_informatica_overmatch AS
WITH informatica_groups AS (
  SELECT
    CountryCode,
    SourceGoldenRecordId,
    COUNT(*)                          AS informatica_group_size,
    COUNT(DISTINCT engine_match_id)   AS engine_groups_in_informatica_group
  FROM pds_auroradsar_prod.schema_informatica.MDMMatchedResults
  WHERE SourceGoldenRecordId IS NOT NULL
  GROUP BY CountryCode, SourceGoldenRecordId
)
SELECT
  r.CountryCode,
  r.SourceGoldenRecordId,
  g.informatica_group_size,
  g.engine_groups_in_informatica_group,
  r.engine_match_id,
  r.OperatorConcatId,
  r.golden_id,
  r.OperatorName,
  r.StreetText,
  r.CityText,
  r.ZipCode,
  r.StateText,
  r.final_match_rule,
  r.final_match_rule LIKE 'Excluded from Match%'   AS engine_skipped_record,
  r.MatchRunTimestamp
FROM pds_auroradsar_prod.schema_informatica.MDMMatchedResults r
JOIN informatica_groups g
  ON r.CountryCode = g.CountryCode
 AND r.SourceGoldenRecordId = g.SourceGoldenRecordId
WHERE g.informatica_group_size > 1
  AND g.engine_groups_in_informatica_group > 1;
