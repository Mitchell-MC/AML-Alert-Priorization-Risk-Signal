"""Name/country/date normalization for entity matching.

Deliberately kept as plain string functions (no Spark dependency) so the same code runs
identically in a local unit test, a Databricks Python notebook, or a PySpark UDF — see
docs/03_schema_contracts.md's note on silver.entity_alias.alias_normalized.
"""
from __future__ import annotations

import re
import unicodedata

_LEGAL_SUFFIXES = (
    "LLC", "LTD", "LIMITED", "CO", "CORP", "CORPORATION", "INC", "INCORPORATED",
    "GROUP", "HOLDINGS", "HOLDING", "PLC", "GMBH", "SA", "SRL", "BV", "NV", "AG",
    "OJSC", "PJSC", "JSC", "LLP", "LP", "PTY", "PTE",
)
_LEGAL_SUFFIX_PATTERN = re.compile(
    r"\b(" + "|".join(_LEGAL_SUFFIXES) + r")\b\.?", flags=re.IGNORECASE
)

_PUNCTUATION_PATTERN = re.compile(r"[^\w\s]", flags=re.UNICODE)
_WHITESPACE_PATTERN = re.compile(r"\s+")

# Partial alias table covering the country forms actually seen in OFAC/OpenSanctions data
# plus other common variants. Falls back to a cleaned-up version of the input when a country
# isn't in the table, rather than raising.
_COUNTRY_ALIASES: dict[str, str] = {
    "US": "US", "USA": "US", "UNITED STATES": "US", "UNITED STATES OF AMERICA": "US",
    "UK": "GB", "UNITED KINGDOM": "GB", "GREAT BRITAIN": "GB", "BRITAIN": "GB",
    "RUSSIA": "RU", "RUSSIAN FEDERATION": "RU",
    "CUBA": "CU",
    "IRAN": "IR", "IRAN ISLAMIC REPUBLIC OF": "IR",
    "NORTH KOREA": "KP", "KOREA NORTH": "KP", "DPRK": "KP",
    "SOUTH KOREA": "KR", "KOREA SOUTH": "KR", "REPUBLIC OF KOREA": "KR",
    "SYRIA": "SY", "SYRIAN ARAB REPUBLIC": "SY",
    "CHINA": "CN", "PEOPLES REPUBLIC OF CHINA": "CN",
    "SWITZERLAND": "CH",
    "SPAIN": "ES",
    "JAPAN": "JP",
    "GERMANY": "DE",
    "FRANCE": "FR",
    "ITALY": "IT",
    "UKRAINE": "UA",
    "VENEZUELA": "VE", "VENEZUELA BOLIVARIAN REPUBLIC OF": "VE",
    "MYANMAR": "MM", "BURMA": "MM",
    "AFGHANISTAN": "AF",
    "BELARUS": "BY",
    "MEXICO": "MX",
    "PANAMA": "PA",
    "UNITED ARAB EMIRATES": "AE", "UAE": "AE",
}


def normalize_name(name: str | None) -> str:
    """Upper-case, strip diacritics/punctuation, collapse whitespace.

    This is the form used for fuzzy comparison. It intentionally keeps legal suffixes
    (LLC, LTD, ...) — use strip_legal_suffixes() separately when comparing organization
    "core" names, since suffix presence/absence is itself a weak signal worth preserving.
    """
    if not name:
        return ""
    decomposed = unicodedata.normalize("NFKD", name)
    without_diacritics = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    no_punctuation = _PUNCTUATION_PATTERN.sub(" ", without_diacritics)
    return _WHITESPACE_PATTERN.sub(" ", no_punctuation).strip().upper()


def strip_legal_suffixes(normalized_name: str) -> str:
    """Remove common legal-entity suffixes from an already-normalized name."""
    stripped = _LEGAL_SUFFIX_PATTERN.sub(" ", normalized_name)
    return _WHITESPACE_PATTERN.sub(" ", stripped).strip()


def normalize_country(country: str | None) -> str | None:
    """Map a free-text country to a 2-letter code where known, else a cleaned-up upper form."""
    if not country:
        return None
    cleaned = _WHITESPACE_PATTERN.sub(" ", country.strip().upper())
    return _COUNTRY_ALIASES.get(cleaned, cleaned)


_DATE_PATTERNS = (
    "%d %b %Y",   # 01 Jan 1970 (OFAC style)
    "%Y-%m-%d",   # ISO (OpenSanctions style)
    "%Y",         # year only (OpenSanctions sometimes has just a year)
    "%Y-%m",      # year-month
    "%m/%d/%Y",
)


def normalize_dob(raw: str | None) -> str | None:
    """Return an ISO (YYYY, YYYY-MM, or YYYY-MM-DD) date string, or None if unparseable.

    OFAC sometimes lists multiple candidate DOBs separated by ';' for one individual —
    callers that need all of them should split on ';' before calling this, since this
    function normalizes a single date value.
    """
    from datetime import datetime

    if not raw:
        return None
    candidate = raw.strip()
    if not candidate or candidate == "-0-":
        return None
    for pattern in _DATE_PATTERNS:
        try:
            parsed = datetime.strptime(candidate, pattern)
        except ValueError:
            continue
        if pattern == "%Y":
            return f"{parsed.year:04d}"
        if pattern == "%Y-%m":
            return f"{parsed.year:04d}-{parsed.month:02d}"
        return parsed.date().isoformat()
    return None
