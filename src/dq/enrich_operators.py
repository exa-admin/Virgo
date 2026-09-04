"""
Batch-enrich operators from a source table into multi-candidate review rows.

For each operator, Google Places may return up to N address candidates.
Each candidate is appended as its own row (same OperatorConcatId, distinct
match_order) so stewards can review and pick the correct address.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Optional, Sequence

import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from dq.address_enrichment import enrich_restaurant

ADDRESS_PARTS = (
    "HouseNumberText",
    "HouseNumberExtensionText",
    "StreetText",
    "CityText",
    "ZipCode",
    "StateText",
    "CountryName",
)


def build_full_address_column(df: SparkDataFrame) -> SparkDataFrame:
    """Add ``full_address`` by concatenating common operator address fields."""
    return df.withColumn(
        "full_address",
        F.concat_ws(", ", *[F.col(c) for c in ADDRESS_PARTS if c in df.columns]),
    )


def _coalesce(enriched_value: Any, original_value: Any) -> Any:
    if enriched_value is None:
        return original_value
    if isinstance(enriched_value, float) and pd.isna(enriched_value):
        return original_value
    if isinstance(enriched_value, str) and not enriched_value.strip():
        return original_value
    return enriched_value


def _row_get(row: Mapping[str, Any], key: str) -> Any:
    try:
        return row[key]
    except Exception:
        return None


def map_candidate_to_operator_row(
    source_row: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> dict[str, Any]:
    """
    Map one Places candidate onto operator column names.

    Produces one review row: originals preserved, enriched values coalesced
    onto the working address columns, plus match_order / place metadata.
    """
    match_found = bool(candidate.get("match_found"))

    return {
        "match_found": match_found,
        "match_order": candidate.get("match_order"),
        "OperatorConcatId": _row_get(source_row, "OperatorConcatId"),
        # --- Original columns (as-is from source) ---
        "original_OperatorName": _row_get(source_row, "OperatorName"),
        "original_StreetText": _row_get(source_row, "StreetText"),
        "original_HouseNumberText": _row_get(source_row, "HouseNumberText"),
        "original_HouseNumberExtensionText": _row_get(
            source_row, "HouseNumberExtensionText"
        ),
        "original_CityText": _row_get(source_row, "CityText"),
        "original_ZipCode": _row_get(source_row, "ZipCode"),
        "original_StateText": _row_get(source_row, "StateText"),
        "original_CountryName": _row_get(source_row, "CountryName"),
        # --- Enriched values (COALESCE enriched, else original) ---
        "OperatorName": _coalesce(
            candidate.get("name"), _row_get(source_row, "OperatorName")
        ),
        "StreetText": _coalesce(
            candidate.get("street"), _row_get(source_row, "StreetText")
        ),
        "HouseNumberText": _coalesce(
            candidate.get("street_number"), _row_get(source_row, "HouseNumberText")
        ),
        "HouseNumberExtensionText": _row_get(source_row, "HouseNumberExtensionText"),
        "CityText": _coalesce(candidate.get("city"), _row_get(source_row, "CityText")),
        "ZipCode": _coalesce(
            candidate.get("postal_code"), _row_get(source_row, "ZipCode")
        ),
        "StateText": _coalesce(
            candidate.get("state"), _row_get(source_row, "StateText")
        ),
        "CountryName": _coalesce(
            candidate.get("country"), _row_get(source_row, "CountryName")
        ),
        "formatted_address": candidate.get("formatted_address"),
        # --- Extra enrichment columns ---
        "latitude": candidate.get("latitude"),
        "longitude": candidate.get("longitude"),
        "place_id": candidate.get("place_id"),
        "phone": candidate.get("phone"),
        "website": candidate.get("website"),
        "rating": candidate.get("rating"),
        "primary_type": candidate.get("primary_type"),
        "google_maps_uri": candidate.get("google_maps_uri"),
        "error": candidate.get("error"),
    }


def enrich_operator_rows(
    rows: Sequence[Mapping[str, Any]] | Iterable[Mapping[str, Any]],
    *,
    country_code: Optional[str] = "MY",
    max_results: int = 10,
    name_col: str = "OperatorName",
    address_col: str = "full_address",
    api_key: Optional[str] = None,
) -> pd.DataFrame:
    """
    Enrich collected operator rows; append one output row per Places candidate.

    Same ``OperatorConcatId`` can appear multiple times with ``match_order``
    1..N for manual review.
    """
    results: list[dict[str, Any]] = []

    for row in rows:
        name = _row_get(row, name_col)
        address = _row_get(row, address_col)
        candidates = enrich_restaurant(
            name,
            address,
            country_code=country_code,
            max_results=max_results,
            api_key=api_key,
        )

        # Always emit at least the no-match / error row from enrich_restaurant
        for _, cand in candidates.iterrows():
            results.append(map_candidate_to_operator_row(row, cand.to_dict()))

    return pd.DataFrame(results)


def enrich_operators_spark(
    df_operators: SparkDataFrame,
    *,
    country_code: Optional[str] = "MY",
    max_results: int = 10,
    api_key: Optional[str] = None,
) -> pd.DataFrame:
    """
    Spark DataFrame in → pandas multi-candidate table out.

    Builds ``full_address``, collects rows (driver-side API calls), expands
    each operator to up to ``max_results`` candidate rows.
    """
    df_input = build_full_address_column(df_operators)
    rows = df_input.collect()
    return enrich_operator_rows(
        rows,
        country_code=country_code,
        max_results=max_results,
        api_key=api_key,
    )
