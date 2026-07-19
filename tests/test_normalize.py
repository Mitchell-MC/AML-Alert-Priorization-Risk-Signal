from aml_lakehouse.matching.normalize import (
    normalize_country,
    normalize_dob,
    normalize_name,
    strip_legal_suffixes,
)


def test_normalize_name_basic_uppercasing_and_whitespace():
    assert normalize_name("  John   Smith  ") == "JOHN SMITH"


def test_normalize_name_strips_punctuation():
    assert normalize_name("O'Brien, Patrick") == "O BRIEN PATRICK"


def test_normalize_name_strips_diacritics():
    assert normalize_name("José Núñez") == "JOSE NUNEZ"


def test_normalize_name_empty_and_none():
    assert normalize_name("") == ""
    assert normalize_name(None) == ""


def test_normalize_name_ofac_null_sentinel_is_not_special_cased_here():
    # Bronze->Silver is responsible for converting "-0-" to real NULL before this is called;
    # this function just normalizes whatever string it's given.
    assert normalize_name("-0-") == "0"


def test_strip_legal_suffixes():
    assert strip_legal_suffixes("ACME TRADING LLC") == "ACME TRADING"
    assert strip_legal_suffixes("BANCO NACIONAL DE CUBA") == "BANCO NACIONAL DE CUBA"
    assert strip_legal_suffixes("GLOBEX CORP") == "GLOBEX"


def test_normalize_country_known_aliases():
    assert normalize_country("USA") == "US"
    assert normalize_country("United States of America") == "US"
    assert normalize_country("Russian Federation") == "RU"
    assert normalize_country("uk") == "GB"


def test_normalize_country_unknown_falls_back_to_cleaned_upper():
    assert normalize_country("Freedonia") == "FREEDONIA"


def test_normalize_country_none():
    assert normalize_country(None) is None
    assert normalize_country("") is None


def test_normalize_dob_ofac_style():
    assert normalize_dob("01 Jan 1970") == "1970-01-01"


def test_normalize_dob_iso_style():
    assert normalize_dob("1970-01-01") == "1970-01-01"


def test_normalize_dob_year_only():
    assert normalize_dob("1970") == "1970"


def test_normalize_dob_null_sentinel_and_empty():
    assert normalize_dob("-0-") is None
    assert normalize_dob("") is None
    assert normalize_dob(None) is None


def test_normalize_dob_unparseable_returns_none():
    assert normalize_dob("circa 1970s") is None
