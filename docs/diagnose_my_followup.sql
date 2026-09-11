-- ============================================================================
-- MDM Malaysia — follow-up diagnostics.  Read-only.
--
-- The old-engine run wrote MDM_Matched_Results with golden_id derived from OUR RULES
-- ALONE (that build had no Informatica preservation).  That makes it a stand-in for
-- engine_match_id, so the over/undermatch picture can be measured from data already
-- on disk, before the current engine is run at all.
--
-- Adjust the table name if yours differs.
-- ============================================================================

-- ---------------------------------------------------------------- Q6: engine vs Informatica
-- The headline: how far do our rules and Informatica actually disagree?
WITH r AS (
  SELECT OperatorConcatId, golden_id AS engine_group, GoldenRecordId AS informatica_group
  FROM pds_auroradsar_prod.schema_informatica.MDM_Matched_Results
  WHERE CountryCode = 'MY'
),
engine AS (
  SELECT engine_group,
         count(*) AS n_records,
         count(DISTINCT informatica_group) AS n_informatica_ids
  FROM r GROUP BY engine_group
),
informatica AS (
  SELECT informatica_group,
         count(*) AS n_records,
         count(DISTINCT engine_group) AS n_engine_groups
  FROM r GROUP BY informatica_group
)
SELECT
  (SELECT count(*) FROM engine)                                              AS engine_groups,
  (SELECT count(*) FROM engine WHERE n_records > 1)                          AS engine_multi_groups,
  (SELECT count(*) FROM engine WHERE n_informatica_ids > 1)                  AS UNDERMATCH_engine_groups,
  (SELECT sum(n_records) FROM engine WHERE n_informatica_ids > 1)            AS UNDERMATCH_records,
  (SELECT count(*) FROM informatica)                                         AS informatica_groups,
  (SELECT count(*) FROM informatica WHERE n_records > 1)                     AS informatica_multi_groups,
  (SELECT count(*) FROM informatica WHERE n_engine_groups > 1)               AS OVERMATCH_informatica_groups,
  (SELECT sum(n_records) FROM informatica WHERE n_engine_groups > 1)         AS OVERMATCH_records;


-- ---------------------------------------------------------------- Q7: worst undermatches
-- Engine groups holding the most distinct Informatica ids — the strongest evidence that
-- Informatica split something it should not have.  Eyeball these for false positives.
WITH r AS (
  SELECT OperatorConcatId, OperatorName, CityText, ZipCode,
         golden_id AS engine_group, GoldenRecordId AS informatica_group
  FROM pds_auroradsar_prod.schema_informatica.MDM_Matched_Results
  WHERE CountryCode = 'MY'
)
SELECT engine_group,
       count(*)                          AS n_records,
       count(DISTINCT informatica_group) AS n_informatica_ids,
       min(OperatorName)                 AS sample_name,
       min(CityText)                     AS sample_city
FROM r
GROUP BY engine_group
HAVING count(DISTINCT informatica_group) > 1
ORDER BY n_informatica_ids DESC
LIMIT 20;


-- ---------------------------------------------------------------- Q8: the 6,013-record group
-- One Informatica group holds 6,013 records - 94% of all clique edges in the country.
-- Is it a real chain or a junk bucket?  If junk it should be excluded, not matched.
SELECT OperatorName, CityText, StateText, ZipCode, count(*) AS n
FROM sl_bdl_processed_cd_prod.cd.vw_ufsoperator
WHERE CountryCode = 'MY'
  AND GoldenRecordId = (
    SELECT GoldenRecordId
    FROM sl_bdl_processed_cd_prod.cd.vw_ufsoperator
    WHERE CountryCode = 'MY' AND GoldenRecordId IS NOT NULL
    GROUP BY GoldenRecordId ORDER BY count(*) DESC LIMIT 1
  )
GROUP BY OperatorName, CityText, StateText, ZipCode
ORDER BY n DESC
LIMIT 20;
