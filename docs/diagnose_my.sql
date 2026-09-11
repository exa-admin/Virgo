-- ============================================================================
-- MDM Malaysia diagnostics.  Read-only: no writes, no temp tables.
-- Standardization below mirrors src/matching/standardize.py exactly.
-- Paste each query separately and send back the result.
-- ============================================================================

-- Shared CTE used by every query below.
WITH std AS (
  SELECT
    OperatorConcatId,
    GoldenRecordId,
    trim(regexp_replace(regexp_replace(lower(coalesce(OperatorName,'')), '[^\\p{L}0-9 ]', ' '), '\\s+', ' ')) AS c_name,
    trim(regexp_replace(regexp_replace(lower(coalesce(CityText,'')),     '[^\\p{L}0-9 ]', ' '), '\\s+', ' ')) AS c_city,
    trim(regexp_replace(regexp_replace(lower(coalesce(StateText,'')),    '[^\\p{L}0-9 ]', ' '), '\\s+', ' ')) AS c_state,
    trim(regexp_replace(regexp_replace(lower(concat_ws(' ', HouseNumberText, HouseNumberExtensionText, StreetText)),
                                       '[^\\p{L}0-9 ]', ' '), '\\s+', ' '))                                  AS c_address,
    regexp_replace(coalesce(ZipCode,''), '[^0-9]', '')                                                        AS c_zip,
    SAPCustomerId, OTMText
  FROM sl_bdl_processed_cd_prod.cd.vw_ufsoperator
  WHERE CountryCode = 'MY'
),
keyed AS (
  SELECT *,
         substring(c_name, 1, 4) AS c_name_prefix4,
         CASE WHEN soundex(c_name) IS NULL OR length(soundex(c_name)) = 0
              THEN substring(c_name, 1, 4) ELSE soundex(c_name) END AS c_name_soundex
  FROM std
)

-- ---------------------------------------------------------------- Q1: data profile
-- Which rules can physically fire?  A rule cannot match on a column that is empty.
SELECT
  count(*)                                                        AS total_rows,
  count(DISTINCT OperatorConcatId)                                AS distinct_operators,
  sum(CASE WHEN c_name    <> '' THEN 1 ELSE 0 END)                AS has_name,
  sum(CASE WHEN c_zip     <> '' THEN 1 ELSE 0 END)                AS has_zip,
  sum(CASE WHEN c_city    <> '' THEN 1 ELSE 0 END)                AS has_city,
  sum(CASE WHEN c_state   <> '' THEN 1 ELSE 0 END)                AS has_state,
  sum(CASE WHEN c_address <> '' THEN 1 ELSE 0 END)                AS has_address,
  sum(CASE WHEN SAPCustomerId IS NOT NULL THEN 1 ELSE 0 END)      AS has_sap_id,
  sum(CASE WHEN OTMText IS NULL THEN 1 ELSE 0 END)                AS otm_is_null,
  sum(CASE WHEN GoldenRecordId IS NOT NULL
            AND trim(GoldenRecordId) <> '' THEN 1 ELSE 0 END)     AS has_informatica_id
FROM keyed;


-- ------------------------------------------------- Q2: blocking block-size distribution
-- THE key number.  A block bigger than its cap is dropped WHOLE and its records get no
-- fuzzy comparison on that key at all.  Caps: zip_name_prefix 1000, city_* 300, state_* 300.
-- Re-paste the WITH block above, then:
SELECT
  block_name,
  cap,
  count(*)                                                         AS total_blocks,
  sum(CASE WHEN n = 1        THEN 1 ELSE 0 END)                    AS singleton_blocks,
  sum(CASE WHEN n > cap      THEN 1 ELSE 0 END)                    AS blocks_over_cap,
  sum(CASE WHEN n > cap THEN n ELSE 0 END)                         AS records_skipped,
  max(n)                                                           AS largest_block,
  sum(CASE WHEN n BETWEEN 2 AND cap THEN n * (n - 1) ELSE 0 END)   AS candidate_pairs_kept
FROM (
  SELECT 'zip_name_prefix'   AS block_name, 1000 AS cap, count(*) AS n
    FROM keyed WHERE c_zip <> '' AND c_name_prefix4 <> '' GROUP BY c_zip, c_name_prefix4
  UNION ALL
  SELECT 'city_name_soundex', 300, count(*)
    FROM keyed WHERE c_city <> '' AND c_name_soundex <> '' GROUP BY c_city, c_name_soundex
  UNION ALL
  SELECT 'city_name_prefix',  300, count(*)
    FROM keyed WHERE c_city <> '' AND c_name_prefix4 <> '' GROUP BY c_city, c_name_prefix4
  UNION ALL
  SELECT 'state_name_soundex', 300, count(*)
    FROM keyed WHERE c_state <> '' AND c_name_soundex <> '' GROUP BY c_state, c_name_soundex
  UNION ALL
  SELECT 'state_name_prefix',  300, count(*)
    FROM keyed WHERE c_state <> '' AND c_name_prefix4 <> '' GROUP BY c_state, c_name_prefix4
) b
GROUP BY block_name, cap
ORDER BY records_skipped DESC;


-- ------------------------------------------------- Q3: the 10 worst oversized blocks
-- Shows WHICH blocks are being dropped, so we can judge whether a narrower key fixes it.
SELECT c_city, c_name_prefix4, count(*) AS n
FROM keyed
WHERE c_city <> '' AND c_name_prefix4 <> ''
GROUP BY c_city, c_name_prefix4
HAVING count(*) > 300
ORDER BY n DESC
LIMIT 10;


-- ------------------------------------------------- Q4: Informatica grouping profile
-- Sizes the source-golden-group work and the components iteration count.
SELECT
  count(DISTINCT GoldenRecordId)                          AS distinct_informatica_ids,
  count(*)                                                AS rows_with_an_id,
  max(n)                                                  AS largest_informatica_group,
  sum(CASE WHEN n > 1 THEN 1 ELSE 0 END)                  AS multi_record_groups,
  sum(CASE WHEN n > 1 THEN n * (n - 1) / 2 ELSE 0 END)    AS star_edges_implied
FROM (
  SELECT GoldenRecordId, count(*) AS n
  FROM keyed
  WHERE GoldenRecordId IS NOT NULL AND trim(GoldenRecordId) <> ''
  GROUP BY GoldenRecordId
) g;


-- ------------------------------------------------- Q5: is GoldenRecordId always numeric?
-- A non-numeric value fails the current engine fast, by design.  Check before running it.
SELECT count(*) AS non_numeric_golden_ids
FROM keyed
WHERE GoldenRecordId IS NOT NULL
  AND trim(GoldenRecordId) <> ''
  AND trim(GoldenRecordId) NOT RLIKE '^[0-9]+$';
