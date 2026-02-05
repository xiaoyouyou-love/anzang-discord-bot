import datetime

import pytest
import requests

from src import build_almanac


def test_fetch_parse_2026_03_09():
    target_date = datetime.date(2026, 3, 9)
    source = build_almanac.SOURCES["huangli123"]
    cache_dir = build_almanac.Path("data") / "test-cache" / source.name
    session = requests.Session()
    session.headers.update({"User-Agent": build_almanac.USER_AGENT})

    try:
        record = build_almanac.fetch_almanac_for_date(
            target_date, source, cache_dir, session
        )
    except build_almanac.FetchError as exc:
        pytest.skip(f"Network unavailable or blocked: {exc}")

    assert "安葬" in record["yi"]
    assert record["zhishen"]
