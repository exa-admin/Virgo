# Tables in `pds_auroradsar_prod.schema_informatica`

Everything the engine creates, split by why it exists. All are Delta, all are written as
country slices (`replaceWhere` / `DELETE WHERE CountryCode`), except the two append-only
audit tables.

## A. Internal / process

Used by the engine to do its job. Nothing downstream should read these.

| Table | Role | Written | Safe to purge? |
|---|---|---|---|
| `mdm_row_registry` | Stable `MDMRowId` per (country, `OperatorConcatId`) + the golden id crosswalk (`SourceGoldenRecordId`, `MDMGoldenId`, source, assigned date) | MERGE, every run | **No — never.** This is what makes golden ids stable across runs. Losing it re-mints every id and breaks every downstream reference. |
| `mdm_golden_id_sequence` | High-water mark for engine-minted ids (one global row) | MERGE, on each allocation | **No.** Losing it risks re-issuing ids already handed out. Rebuildable only from `max()` across the registry. |
| `mdm_match_links` | The final match graph (edges) for the last run of each country | Overwrite per country | Yes — rebuilt every run. Useful for debugging a grouping. |
| `mdm_component_labels` | Label-propagation checkpoints, one slice per iteration. Stages: `labels_*` (final graph), `engine_labels_*` (engine-only pass), `source_group_labels_*` / `labels_source_groups_resolved` (Informatica bridge resolution) | Overwrite per stage | Yes — pure working state. The largest low-value table; purge freely. |
| `mdm_matching_state` | Intermediate record-id sets for the priority waterfall (active subjects, matched-so-far) | Overwrite per stage | Yes — pure working state. |

## B. Audit / stewardship

Kept for review, reconciliation and handing findings to the Informatica team.

| Table / view | Role | Written | Retention |
|---|---|---|---|
| `mdm_matched_results` | The output: one row per operator with `golden_id`, `golden_id_source`, `engine_match_id`, change flags, `final_match_rule` | Overwrite per country | Current state only — **the previous run is gone after an overwrite**, which is exactly why the changelog below exists. |
| `operator_golden_changelog` | Record-level trace: every `OperatorConcatId` whose golden id changed, with the previous run's matching data, this run's, and the cause | **Append** | Keep indefinitely — this is the audit trail. |
| `mdm_golden_id_history` | Id-level remaps (`OldGoldenId` → `NewGoldenId`, `MERGE`/`SPLIT`, record count) | **Append** | Keep indefinitely — downstream crosswalks depend on it. |
| `mdm_rule_results` | Accepted edges per rule stage, plus stage `000` (Informatica groupings) and stage `999` (bridges dropped to protect Informatica ids) | Overwrite per stage | Last run per country. Stage `999` is a direct Informatica-undermatch signal. |
| `mdm_rule_evaluations` | Every fuzzy candidate pair with its similarity scores, matched or not — the evidence for why a pair did or did not match | Overwrite per stage | Last run per country. **Largest table by far** (candidate pairs, not records) — the first thing to consider for retention limits. |
| `mdm_match_exclusions` | Stewardship "do not match" keys | **You populate it** — the engine only reads it | Permanent, human-owned. |
| `vw_informatica_undermatch` | View: operators our rules grouped that Informatica did not | View over `mdm_matched_results` | — |
| `vw_informatica_overmatch` | View: operators Informatica grouped that our rules did not | View over `mdm_matched_results` | — |

## C. External — read, never created here

| Object | Role |
|---|---|
| `sl_bdl_processed_cd_prod.cd.vw_ufsoperator` | Operator population to match |
| `sl_bdl_processed_cd_prod.cd.vw_ufsoperatorgolden` | Informatica golden masters |
| `pds_auroradsar_prod.schema_informatica.mdmenrichedoperators` | Reviewed address enrichment, overlaid when `EnrichDate: true` |

## Purging working state

Group A's purgeable tables are rebuilt on the next run, so this is safe at any time:

```sql
DELETE FROM pds_auroradsar_prod.schema_informatica.mdm_component_labels WHERE CountryCode = 'MY';
DELETE FROM pds_auroradsar_prod.schema_informatica.mdm_matching_state   WHERE CountryCode = 'MY';
```

Never truncate `mdm_row_registry` or `mdm_golden_id_sequence`.
