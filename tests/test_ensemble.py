from datetime import date

from weatherfusion.models import SourceDailyRecord
from weatherfusion.processing.ensemble import build_site_ensembles


def make_record(source: str, high: float | None, low: float | None, pop: float | None, precip: str | None, notes: str = ""):
    return SourceDailyRecord(
        site_name="Home",
        date=date(2024, 5, 1),
        label="Wed May 01",
        source=source,
        high_f=high,
        low_f=low,
        pop_pct=pop,
        precip_type=precip,
        precip_notes=notes,
        wind_phrase=notes,
    )


def test_build_site_ensembles_merges_sources():
    recs = [
        make_record("nbm_grib", 82, 60, 40, "Rain"),
        make_record("nws_rss", 84, 59, 60, "Snow", notes="Breezy north winds"),
    ]
    rows = build_site_ensembles("Home", recs, days=1)
    assert rows[0].high_f == 83.0
    assert rows[0].pop_pct == 60
    assert rows[0].heat_category == "Caution"
    assert rows[0].low_confidence is False
    assert rows[0].freeze_risk_badge is None


def test_build_site_ensembles_skips_invalid_rows():
    recs = [make_record("nbm_grib", None, None, None, None)]
    rows = build_site_ensembles("Home", recs, days=1)
    assert rows == []
