"""
Google Places address enrichment for restaurant matching.

Returns the top N candidate matches (default 10) as a ranked table so a
reviewer can inspect and pick the correct address.
"""

from __future__ import annotations

import os
from typing import Optional

import pandas as pd
import requests

PLACES_URL = "https://places.googleapis.com/v1/places:searchText"

FIELD_MASK = ",".join(
    [
        "places.id",
        "places.displayName",
        "places.formattedAddress",
        "places.addressComponents",
        "places.location",
        "places.types",
        "places.primaryType",
        "places.nationalPhoneNumber",
        "places.internationalPhoneNumber",
        "places.websiteUri",
        "places.rating",
        "places.userRatingCount",
        "places.priceLevel",
        "places.regularOpeningHours",
        "places.googleMapsUri",
    ]
)

CANDIDATE_COLUMNS = [
    "input_name",
    "input_address",
    "match_order",
    "match_found",
    "place_id",
    "name",
    "formatted_address",
    "street_number",
    "street",
    "city",
    "state",
    "postal_code",
    "country",
    "country_code",
    "latitude",
    "longitude",
    "primary_type",
    "phone",
    "website",
    "rating",
    "user_rating_count",
    "google_maps_uri",
    "error",
]


def _get_api_key(api_key: Optional[str] = None) -> str:
    """Explicit argument first, then GOOGLE_PLACES_API_KEY. Never hardcode a key here."""
    key = api_key or os.environ.get("GOOGLE_PLACES_API_KEY")
    if not key:
        raise ValueError(
            "Google Places API key required. Pass api_key=... or set "
            "GOOGLE_PLACES_API_KEY in the environment (on Databricks, from a secret scope)."
        )
    return key


def _address_components(place: dict) -> dict:
    """Map address component type -> longText (and shortText for country)."""
    long_text = {}
    short_text = {}
    for c in place.get("addressComponents", []) or []:
        types = c.get("types") or []
        if not types:
            continue
        primary = types[0]
        long_text[primary] = c.get("longText")
        short_text[primary] = c.get("shortText")
    return {"long": long_text, "short": short_text}


def _place_to_row(
    place: dict,
    *,
    name: str,
    address: str,
    match_order: int,
) -> dict:
    comps = _address_components(place)
    long_c = comps["long"]
    short_c = comps["short"]

    return {
        "input_name": name,
        "input_address": address,
        "match_order": match_order,
        "match_found": True,
        "place_id": place.get("id"),
        "name": (place.get("displayName") or {}).get("text"),
        "formatted_address": place.get("formattedAddress"),
        "street_number": long_c.get("street_number"),
        "street": long_c.get("route"),
        "city": long_c.get("locality") or long_c.get("postal_town"),
        "state": long_c.get("administrative_area_level_1"),
        "postal_code": long_c.get("postal_code"),
        "country": long_c.get("country"),
        "country_code": short_c.get("country"),
        "latitude": (place.get("location") or {}).get("latitude"),
        "longitude": (place.get("location") or {}).get("longitude"),
        "primary_type": place.get("primaryType"),
        "phone": place.get("internationalPhoneNumber"),
        "website": place.get("websiteUri"),
        "rating": place.get("rating"),
        "user_rating_count": place.get("userRatingCount"),
        "google_maps_uri": place.get("googleMapsUri"),
        "error": None,
    }


def _empty_result(
    name: str,
    address: str,
    *,
    match_found: bool = False,
    error: Optional[str] = None,
) -> list[dict]:
    return [
        {
            "input_name": name,
            "input_address": address,
            "match_order": None,
            "match_found": match_found,
            "place_id": None,
            "name": None,
            "formatted_address": None,
            "street_number": None,
            "street": None,
            "city": None,
            "state": None,
            "postal_code": None,
            "country": None,
            "country_code": None,
            "latitude": None,
            "longitude": None,
            "primary_type": None,
            "phone": None,
            "website": None,
            "rating": None,
            "user_rating_count": None,
            "google_maps_uri": None,
            "error": error,
        }
    ]


def enrich_restaurant_candidates(
    name: str,
    address: str,
    country_code: Optional[str] = None,
    max_results: int = 10,
    timeout: int = 10,
    api_key: Optional[str] = None,
) -> pd.DataFrame:
    """
    Enrich a restaurant via Google Places Text Search (New).

    Returns up to ``max_results`` candidates ordered by Google relevance
    (``match_order`` 1 = best). Reviewers can pick the correct row.

    Set GOOGLE_PLACES_API_KEY or pass ``api_key``.
    """
    if max_results < 1 or max_results > 20:
        raise ValueError("max_results must be between 1 and 20 (Places API limit).")

    query = ", ".join(p for p in [name, address] if p and str(p).strip())
    key = _get_api_key(api_key)

    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": key,
        "X-Goog-FieldMask": FIELD_MASK,
    }
    payload: dict = {
        "textQuery": query,
        "maxResultCount": max_results,
    }
    if country_code:
        payload["regionCode"] = country_code.strip().upper()

    try:
        r = requests.post(PLACES_URL, json=payload, headers=headers, timeout=timeout)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        return pd.DataFrame(_empty_result(name, address, error=str(e)), columns=CANDIDATE_COLUMNS)

    places = data.get("places") or []
    if not places:
        return pd.DataFrame(
            _empty_result(name, address, match_found=False),
            columns=CANDIDATE_COLUMNS,
        )

    expected_country = country_code.strip().upper() if country_code else None
    rows: list[dict] = []
    order = 0

    for place in places:
        comps = _address_components(place)
        result_country = (comps["short"].get("country") or "").upper()

        # Skip results from a different country when a country filter is set
        if expected_country and result_country and result_country != expected_country:
            continue

        order += 1
        rows.append(
            _place_to_row(place, name=name, address=address, match_order=order)
        )

    if not rows:
        return pd.DataFrame(
            _empty_result(name, address, match_found=False),
            columns=CANDIDATE_COLUMNS,
        )

    return pd.DataFrame(rows, columns=CANDIDATE_COLUMNS)


def select_candidate(candidates: pd.DataFrame, match_order: int) -> pd.Series:
    """Return the candidate row chosen by the reviewer (by match_order)."""
    if candidates.empty or not candidates["match_found"].any():
        raise ValueError("No candidates available to select.")

    selected = candidates.loc[candidates["match_order"] == match_order]
    if selected.empty:
        raise ValueError(f"No candidate with match_order={match_order}")
    return selected.iloc[0]


# Backward-compatible alias: single call still returns candidates as a table
def enrich_restaurant(
    name: str,
    address: str,
    country_code: Optional[str] = None,
    timeout: int = 10,
    max_results: int = 10,
    api_key: Optional[str] = None,
) -> pd.DataFrame:
    """Same as enrich_restaurant_candidates (top-N ranked table)."""
    return enrich_restaurant_candidates(
        name,
        address,
        country_code=country_code,
        max_results=max_results,
        timeout=timeout,
        api_key=api_key,
    )


if __name__ == "__main__":
    # Example — requires GOOGLE_PLACES_API_KEY
    df = enrich_restaurant(
        "McDonalalds",
        "Chineham,United Kingdom",
        country_code="GB",
        max_results=10,
    )
    # Databricks notebooks expose display(); fall back for local CLI
    try:
        display(df)
    except NameError:
        print(df.to_string(index=False))
