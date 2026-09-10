"""End-to-end match scenarios against a local Spark + Delta warehouse.

Each test loads a small source population, runs the real ``run_country`` with the real MY
rules, and asserts on mdm_matched_results. The two things that matter most are covered from
both sides: records that **must** end up together, and records that **must not**.
"""
from __future__ import annotations

import pytest

from conftest import operator

GOLDEN_ID_FLOOR = 100_000_000


# --------------------------------------------------------------- matches we must make


def test_exact_name_and_zip_matches(mdm):
    mdm.run([
        operator("OP1", "Pizza Palace", ZipCode="50000", CityText="Kuala Lumpur", StreetText="Jalan Bukit", HouseNumberText="10"),
        operator("OP2", "Pizza Palace", ZipCode="50000", CityText="Kuala Lumpur", StreetText="Jalan Bukit", HouseNumberText="10"),
    ])
    assert mdm.grouped("OP1", "OP2")
    assert "Exact_OperatorName_Zip" in mdm.rows_by_key()["OP1"].final_match_rule


def test_fuzzy_matches_a_name_typo(mdm):
    """One transposed character must not create a duplicate customer."""
    mdm.run([
        operator("OP1", "Burger King", ZipCode="50450", CityText="Kuala Lumpur", StreetText="Jalan Ampang", HouseNumberText="12"),
        operator("OP2", "Burger Kingg", ZipCode="50450", CityText="Kuala Lumpur", StreetText="Jalan Ampang", HouseNumberText="12"),
    ])
    assert mdm.grouped("OP1", "OP2")
    assert "Fuzzy" in mdm.rows_by_key()["OP1"].final_match_rule


def test_matches_chain_transitively(mdm):
    """A~B on name+zip and B~C on SAP id must land all three in one group."""
    mdm.run([
        operator("OP1", "Chain One", ZipCode="58000", CityText="Cheras", StreetText="Jalan Chain", OTMText="STD"),
        operator("OP2", "Chain One", ZipCode="58000", CityText="Cheras", StreetText="Jalan Chain", SAPCustomerId="SAP999", OTMText="STD"),
        operator("OP3", "Totally Other Name", ZipCode="58999", CityText="Ampang", StreetText="Jalan Other", SAPCustomerId="SAP999", OTMText="STD"),
    ])
    assert mdm.grouped("OP1", "OP2", "OP3")


# ----------------------------------------------------------- matches we must NOT make


def test_null_in_an_exclusion_column_silently_drops_the_record_from_that_rule(mdm):
    """MY's SAP rule carries match_exclusion_filter ["OTMText IN ('A++','DUMMY')"].

    That becomes ``NOT (OTMText IN (...))``, and in SQL a NULL OTMText makes the whole
    predicate NULL, not TRUE — so records with no OTMText never reach the rule at all.
    Pinned here because it looks exactly like a matching bug. If every operator should be
    considered, the filter needs ``coalesce(OTMText, '')``.
    """
    shared = dict(ZipCode="58200", CityText="Cheras", StreetText="Jalan Sap")
    mdm.run([
        operator("OP1", "Alpha Widgets", SAPCustomerId="SAP555", **shared),
        operator("OP2", "Beta Gadgets", SAPCustomerId="SAP555", **shared),
    ])
    assert not mdm.grouped("OP1", "OP2"), "NULL OTMText no longer excludes rows — filter was fixed"

    mdm.run([
        operator("OP1", "Alpha Widgets", SAPCustomerId="SAP555", OTMText="STD", **shared),
        operator("OP2", "Beta Gadgets", SAPCustomerId="SAP555", OTMText="STD", **shared),
    ])
    assert mdm.grouped("OP1", "OP2")


def test_different_businesses_are_not_matched(mdm):
    """Same city and zip is not evidence. This is the overmatch guard."""
    mdm.run([
        operator("OP1", "Sushi Zen", ZipCode="50100", CityText="Kuala Lumpur", StreetText="Jalan Alor", HouseNumberText="1"),
        operator("OP2", "Taco Loco", ZipCode="50100", CityText="Kuala Lumpur", StreetText="Jalan Imbi", HouseNumberText="99"),
    ])
    assert not mdm.grouped("OP1", "OP2")
    assert all(not row.is_matched for row in mdm.rows_by_key().values())


def test_junk_names_do_not_match_each_other(mdm):
    """'test' is in invalid_values, so it must never act as a shared key."""
    mdm.run([
        operator("OP1", "TEST", ZipCode="57000", CityText="Klang", StreetText="Jalan A"),
        operator("OP2", "TEST", ZipCode="57000", CityText="Klang", StreetText="Jalan A"),
    ])
    assert not mdm.grouped("OP1", "OP2")


def test_excluded_records_are_not_matched_but_keep_their_group(mdm):
    """Exclusions stop NEW matching; they never undo what Informatica already decided."""
    mdm.run([
        operator("OP1", "INVALID OPERATOR", GoldenRecordId="5000005", ZipCode="55000", CityText="Klang"),
        operator("OP2", "INVALID OPERATOR", GoldenRecordId="5000005", ZipCode="55000", CityText="Klang"),
        operator("OP3", "INVALID OPERATOR", ZipCode="55000", CityText="Klang"),
    ])
    rows = mdm.rows_by_key()
    assert all("Excluded from Match" in rows[k].final_match_rule for k in ("OP1", "OP2", "OP3"))
    assert mdm.grouped("OP1", "OP2")            # Informatica's grouping survives
    assert not mdm.grouped("OP1", "OP3")        # but no new match is made
    assert rows["OP1"].golden_id == 5000005


# ------------------------------------------------------- Informatica golden id continuity


def test_informatica_group_is_never_split_or_renamed(mdm):
    """Two records Informatica grouped that our rules would never link."""
    mdm.run([
        operator("OP1", "Alpha Trading", GoldenRecordId="5000001", ZipCode="51000", CityText="Kuala Lumpur", StreetText="Jalan A"),
        operator("OP2", "Zeta Holdings", GoldenRecordId="5000001", ZipCode="59000", CityText="Ampang", StreetText="Jalan Z"),
    ])
    assert mdm.grouped("OP1", "OP2")
    assert mdm.golden("OP1")[0] == 5000001
    assert not mdm.engine_grouped("OP1", "OP2")   # our rules disagree, and say so


def test_new_record_inherits_the_informatica_id(mdm):
    mdm.run([
        operator("OP1", "Cafe Mocha", GoldenRecordId="5000002", ZipCode="52000", CityText="Petaling Jaya", StreetText="Jalan M"),
        operator("OP2", "Cafe Mocha", ZipCode="52000", CityText="Petaling Jaya", StreetText="Jalan M"),
    ])
    rows = mdm.rows_by_key()
    assert mdm.grouped("OP1", "OP2")
    assert rows["OP2"].golden_id == 5000002
    assert rows["OP2"].golden_id_source == "INFORMATICA"


def test_brand_new_cluster_is_minted_above_the_floor(mdm):
    mdm.run([
        operator("OP1", "Nasi Lemak House", ZipCode="53000", CityText="Shah Alam", StreetText="Jalan N"),
        operator("OP2", "Nasi Lemak House", ZipCode="53000", CityText="Shah Alam", StreetText="Jalan N"),
    ])
    rows = mdm.rows_by_key()
    assert mdm.grouped("OP1", "OP2")
    assert rows["OP1"].golden_id >= GOLDEN_ID_FLOOR
    assert rows["OP1"].golden_id_source == "ENGINE"
    assert rows["OP1"].golden_id_is_new


def test_engine_rule_may_not_merge_two_informatica_groups(mdm):
    """The bridging edge is dropped and recorded; neither Informatica id moves."""
    mdm.run([
        operator("OP1", "Bridge Cafe", GoldenRecordId="5000003", ZipCode="54000", CityText="Kajang", StreetText="Jalan B"),
        operator("OP2", "Bridge Cafe", GoldenRecordId="5000004", ZipCode="54000", CityText="Kajang", StreetText="Jalan B"),
    ])
    rows = mdm.rows_by_key()
    assert rows["OP1"].golden_id == 5000003
    assert rows["OP2"].golden_id == 5000004
    blocked = mdm.table("mdm_rule_results").where("RuleStageName = '999_Blocked_Source_GoldenRecordId_Merge'")
    assert blocked.count() >= 1


def test_no_informatica_id_ever_changes(mdm):
    """The migration invariant, asserted over a mixed population."""
    mdm.run([
        operator("OP1", "Alpha Trading", GoldenRecordId="5000001", ZipCode="51000", CityText="Kuala Lumpur"),
        operator("OP2", "Zeta Holdings", GoldenRecordId="5000001", ZipCode="59000", CityText="Ampang"),
        operator("OP3", "Cafe Mocha", GoldenRecordId="5000002", ZipCode="52000", CityText="Petaling Jaya"),
        operator("OP4", "Cafe Mocha", ZipCode="52000", CityText="Petaling Jaya"),
        operator("OP5", "Solo Diner", ZipCode="56000", CityText="Klang"),
    ])
    for row in mdm.rows_by_key().values():
        if row.SourceGoldenRecordId is not None:
            assert row.golden_id == row.SourceGoldenRecordId, row.OperatorConcatId
        assert not row.golden_id_changed


def test_non_numeric_informatica_id_fails_the_run(mdm):
    """Better to stop than to silently NULL the id and re-mint the record."""
    with pytest.raises(ValueError, match="non-numeric"):
        mdm.run([operator("OP1", "Bad Id Cafe", GoldenRecordId="ABC123", ZipCode="50000")])


# ------------------------------------------------------------------ stability and tracing


def test_rerunning_unchanged_data_changes_nothing(mdm):
    rows = [
        operator("OP1", "Steady Cafe", ZipCode="50000", CityText="Kuala Lumpur", StreetText="Jalan S"),
        operator("OP2", "Steady Cafe", ZipCode="50000", CityText="Kuala Lumpur", StreetText="Jalan S"),
        operator("OP3", "Lonely Cafe", GoldenRecordId="5000009", ZipCode="51111", CityText="Ampang"),
    ]
    mdm.run(rows)
    first = {k: r.golden_id for k, r in mdm.rows_by_key().items()}
    mdm.run(rows)
    assert {k: r.golden_id for k, r in mdm.rows_by_key().items()} == first
    assert mdm.table("operator_golden_changelog").count() == 0


def test_a_golden_id_change_is_traced_with_its_cause(mdm):
    """OP2 is renamed so it now matches OP1's Informatica group; the move must be logged."""
    mdm.run([
        operator("OP1", "Group Diner", GoldenRecordId="5000006", ZipCode="56000", CityText="Klang", StreetText="Jalan G"),
        operator("OP2", "Solo Diner", ZipCode="56000", CityText="Klang", StreetText="Jalan S"),
    ])
    minted = mdm.rows_by_key()["OP2"].golden_id
    assert minted >= GOLDEN_ID_FLOOR

    mdm.run([
        operator("OP1", "Group Diner", GoldenRecordId="5000006", ZipCode="56000", CityText="Klang", StreetText="Jalan G"),
        operator("OP2", "Group Diner", ZipCode="56000", CityText="Klang", StreetText="Jalan G"),
    ])
    assert mdm.rows_by_key()["OP2"].golden_id == 5000006

    log = mdm.table("operator_golden_changelog").where("OperatorConcatId = 'OP2'").collect()
    assert len(log) == 1
    entry = log[0]
    assert entry.PreviousGoldenId == minted
    assert entry.NewGoldenId == 5000006
    assert entry.ChangeReason == "SOURCE_DATA_CHANGED"
    assert entry.PreviousName == "solo diner"      # previous matching data
    assert entry.NewName == "group diner"          # current matching data


# -------------------------------------------------------- Informatica comparison views


def test_undermatch_view_lists_what_informatica_missed(mdm):
    mdm.run([
        # We link these two; Informatica only knows the first.
        operator("OP1", "Cafe Mocha", GoldenRecordId="5000002", ZipCode="52000", CityText="Petaling Jaya", StreetText="Jalan M"),
        operator("OP2", "Cafe Mocha", ZipCode="52000", CityText="Petaling Jaya", StreetText="Jalan M"),
        # Unrelated and correctly left alone.
        operator("OP3", "Sushi Zen", GoldenRecordId="5000007", ZipCode="50100", CityText="Kuala Lumpur", StreetText="Jalan Alor"),
    ])
    flagged = {r.OperatorConcatId: r for r in mdm.table("vw_informatica_undermatch").collect()}
    assert {"OP1", "OP2"} <= set(flagged)
    assert "OP3" not in flagged
    assert flagged["OP2"].undermatch_type == "INFORMATICA_MISSING_MEMBER"


def test_undermatch_view_flags_a_pair_informatica_never_saw(mdm):
    mdm.run([
        operator("OP1", "Nasi Lemak House", ZipCode="53000", CityText="Shah Alam", StreetText="Jalan N"),
        operator("OP2", "Nasi Lemak House", ZipCode="53000", CityText="Shah Alam", StreetText="Jalan N"),
    ])
    flagged = {r.OperatorConcatId: r for r in mdm.table("vw_informatica_undermatch").collect()}
    assert {"OP1", "OP2"} == set(flagged)
    assert flagged["OP1"].undermatch_type == "INFORMATICA_NEVER_MATCHED"


def test_overmatch_view_lists_what_informatica_wrongly_joined(mdm):
    mdm.run([
        # Informatica says same entity; our rules find no evidence.
        operator("OP1", "Alpha Trading", GoldenRecordId="5000001", ZipCode="51000", CityText="Kuala Lumpur", StreetText="Jalan A"),
        operator("OP2", "Zeta Holdings", GoldenRecordId="5000001", ZipCode="59000", CityText="Ampang", StreetText="Jalan Z"),
        # Informatica says same entity, and our rules agree.
        operator("OP3", "Cafe Mocha", GoldenRecordId="5000002", ZipCode="52000", CityText="Petaling Jaya", StreetText="Jalan M"),
        operator("OP4", "Cafe Mocha", GoldenRecordId="5000002", ZipCode="52000", CityText="Petaling Jaya", StreetText="Jalan M"),
    ])
    flagged = {r.OperatorConcatId for r in mdm.table("vw_informatica_overmatch").collect()}
    assert flagged == {"OP1", "OP2"}


def test_views_are_empty_when_the_engine_and_informatica_agree(mdm):
    mdm.run([
        operator("OP1", "Cafe Mocha", GoldenRecordId="5000002", ZipCode="52000", CityText="Petaling Jaya", StreetText="Jalan M"),
        operator("OP2", "Cafe Mocha", GoldenRecordId="5000002", ZipCode="52000", CityText="Petaling Jaya", StreetText="Jalan M"),
        operator("OP3", "Sushi Zen", GoldenRecordId="5000007", ZipCode="50100", CityText="Kuala Lumpur", StreetText="Jalan Alor"),
    ])
    assert mdm.table("vw_informatica_undermatch").count() == 0
    assert mdm.table("vw_informatica_overmatch").count() == 0
