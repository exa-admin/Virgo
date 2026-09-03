-- =============================================================================
-- MDM Match engine — Delta table DDL
-- =============================================================================
-- Schema placeholder matches Python defaults in matching.config.DEFAULT_TARGET_SCHEMA.
-- Parameterize catalog/schema per environment before running, e.g.:
--   REPLACE pds_auroradsar_prod.schema_informatica WITH <catalog>.<schema>
--
-- Source table (sources_informatica.ufsoperator) and enrichment table
-- (mdmenrichedoperators) are externally populated and are NOT created here.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS pds_auroradsar_prod.schema_informatica;

-- -----------------------------------------------------------------------------
-- MDMRowRegistry
-- Stable per-country surrogate keys (MDMRowId) for OperatorConcatId, plus the
-- golden id crosswalk used for Informatica -> engine continuity:
--   SourceGoldenRecordId     last NON-NULL Informatica GoldenRecordId seen for the
--                            row (never downgraded to NULL once Informatica stops
--                            populating it for a migrated country)
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
-- Seed the row at or above the configured golden_id_floor (default 1000000000);
-- the engine also raises the base above max(SourceGoldenRecordId) automatically.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pds_auroradsar_prod.schema_informatica.MDMGoldenIdSequence (
  SequenceName   STRING     NOT NULL,
  NextValue      BIGINT     NOT NULL,
  DateUpdated    TIMESTAMP
) USING DELTA
COMMENT 'Golden id allocator high-water mark (next unassigned value)';

-- Idempotent seed (optional: the engine inserts the row itself on first allocation).
MERGE INTO pds_auroradsar_prod.schema_informatica.MDMGoldenIdSequence AS t
USING (SELECT 'MDMGoldenId' AS SequenceName, CAST(1000000000 AS BIGINT) AS NextValue) AS s
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
-- MDMRuleResults — per-rule match links (stewardship / audit)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pds_auroradsar_prod.schema_informatica.MDMRuleResults (
  CountryCode           STRING,
  RuleType              STRING,
  RuleStageName         STRING,
  RuleExecutionOrder    INT,
  src                   BIGINT,
  dst                   BIGINT,
  SrcOperatorConcatId   STRING,
  DstOperatorConcatId   STRING,
  match_rule            STRING,
  rule_priority         INT,
  edge_type             STRING,
  block_name            STRING,
  match_key             STRING,
  src_is_rule_subject   BOOLEAN,
  dst_is_rule_subject   BOOLEAN
) USING DELTA
COMMENT 'Materialized match links per rule stage';

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
  final_match_rule              STRING,
  is_matched                    BOOLEAN,
  is_group_anchor               BOOLEAN,
  match_group_size              BIGINT
) USING DELTA
COMMENT 'Matched results per country (schema widened at write via mergeSchema)';
-- Existing deployments: mergeSchema=true adds the new golden id columns on the next run.

-- -----------------------------------------------------------------------------
-- Externally populated (not created here):
--   sources_informatica.ufsoperator          — source operators
--   pds_auroradsar_prod.schema_informatica.mdmenrichedoperators
--       Expected join key: OperatorConcatId
--       Columns used when EnrichDate=true (see ENRICHMENT_COLUMN_MAPPINGS):
--         OperatorName, HouseNumberText, StreetText, CityText, StateText,
--         CountryName, latitude, longitude, ZipCode
--       Optional filter column: match_found
-- -----------------------------------------------------------------------------
