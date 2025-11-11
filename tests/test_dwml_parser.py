from zoneinfo import ZoneInfo

from weatherfusion.config import SiteSettings
from weatherfusion.ingest.dwml import parse_dwml


def test_parse_dwml_extracts_daily_fields():
    xml = open("tests/fixtures/dwml_sample.xml", "r", encoding="utf-8").read()
    site = SiteSettings(name="Home", latitude=39.3, longitude=-77.7)
    rows = parse_dwml(xml, site, days=3, tzinfo=ZoneInfo("America/New_York"))
    assert len(rows) == 3
    first = rows[0]
    assert first.high_f == 78
    assert first.low_f == 58
    assert first.pop_pct == 40  # max of first two periods
    assert "Rain" in (first.precip_type or "")
    assert first.wind_phrase and "breezy" in first.wind_phrase.lower()
    second = rows[1]
    assert second.precip_type.startswith("Chance Light Snow") or "Snow" in (second.precip_type or "")
