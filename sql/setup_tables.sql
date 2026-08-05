-- =============================================================================
-- MDM Match engine — Delta table DDL
-- =============================================================================
-- Schema placeholder matches Python defaults in mdm.config.DEFAULT_TARGET_SCHEMA.
-- Parameterize catalog/schema per environment before running, e.g.:
--   REPLACE pds_auroradsar_prod.schema_informatica WITH <catalog>.<schema>
--
-- Source table (sources_informatica.ufsoperator) and enrichment table
-- (mdmenrichedoperators) are externally populated and are NOT created here.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS pds_auroradsar_prod.schema_informatica;

-- -----------------------------------------------------------------------------
-- MDMRowRegistry
-- Stable per-country surrogate keys (MDMRowId) for OperatorConcatId.
-- MERGE in the engine inserts new keys and updates SourceGoldenRecordId.
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
  DateCreated           TIMESTAMP,
  DateUpdated           TIMESTAMP
) USING DELTA
COMMENT 'Stable MDM row keys per country / OperatorConcatId';

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
  CountryCode           STRING,
  record_id             BIGINT,
  MDMRowId              BIGINT,
  OperatorConcatId      STRING,
  golden_id             BIGINT,
  final_match_rule      STRING,
  is_matched            BOOLEAN,
  is_group_anchor       BOOLEAN,
  match_group_size      BIGINT
) USING DELTA
COMMENT 'Matched results per country (schema widened at write via mergeSchema)';

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
