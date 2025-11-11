from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Tuple

from dateutil import parser as dtparser

from ..config import SiteSettings
from ..models import SourceDailyRecord
from ..util.time import format_day_label
from .cache import CacheManager

LOGGER = logging.getLogger(__name__)
POINTS_URL = "https://api.weather.gov/points"
DURATION_RE = re.compile(r"P(?:(?P<days>\d+)D)?(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?)?")


def c_to_f(value: float | None) -> float | None:
    if value is None:
        return None
    return value * 9.0 / 5.0 + 32.0


def mm_to_inches(value: float | None) -> float | None:
    if value is None:
        return None
    return value * 0.0393701


def _parse_duration(value: str) -> timedelta:
    match = DURATION_RE.fullmatch(value)
    if not match:
        return timedelta(hours=1)
    days = int(match.group("days") or 0)
    hours = int(match.group("hours") or 0)
    minutes = int(match.group("minutes") or 0)
    return timedelta(days=days, hours=hours, minutes=minutes)


def _parse_period(value: str, tzinfo) -> Tuple[datetime, datetime]:
    if "/" in value:
        start_raw, duration_raw = value.split("/", 1)
        start = dtparser.isoparse(start_raw).astimezone(tzinfo)
        end = start + _parse_duration(duration_raw)
    else:
        start = dtparser.isoparse(value).astimezone(tzinfo)
        end = start + timedelta(hours=1)
    return start, end


def _slug(site: SiteSettings) -> str:
    return f"{site.latitude:.4f}_{site.longitude:.4f}".replace("-", "m").replace(".", "d")


def _weather_phrase(entry: Dict[str, str]) -> str | None:
    coverage_map = {
        "chance": "Chance",
        "slight_chance": "Slight chance",
        "likely": "Likely",
        "definite": "Definite",
        "occasional": "Occasional",
        "periods": "Periods of",
        "areas": "Areas of",
        "patchy": "Patchy",
    }
    weather = entry.get("weather")
    if not weather:
        return None
    parts: List[str] = []
    coverage = entry.get("coverage")
    if coverage and coverage in coverage_map:
        parts.append(coverage_map[coverage])
    intensity = entry.get("intensity")
    if intensity and intensity not in {"none"}:
        parts.append(intensity.title())
    parts.append(weather.replace("_", " ").title())
    attrs = entry.get("attributes", [])
    if attrs:
        parts.append("+".join(attr.title() for attr in attrs))
    return " ".join(parts)


def _ensure_record(bucket: Dict[date, SourceDailyRecord], site: SiteSettings, day: date) -> SourceDailyRecord:
    if day not in bucket:
        bucket[day] = SourceDailyRecord(
            site_name=site.name,
            date=day,
            label=format_day_label(day),
            source="nws_gridpoint",
        )
    return bucket[day]


class GridpointIngestor:
    source_name = "nws_gridpoint"

    def __init__(self, session, cache: CacheManager, days: int, tzinfo) -> None:
        self.session = session
        self.cache = cache
        self.days = days
        self.tzinfo = tzinfo

    def _download(self, url: str) -> bytes:
        resp = self.session.get(url, timeout=60)
        resp.raise_for_status()
        return resp.content

    def _point_metadata(self, site: SiteSettings) -> dict:
        slug = _slug(site)
        text = self.cache.read_text(
            "gridpoint/meta",
            f"{slug}.json",
            lambda: self._download(f"{POINTS_URL}/{site.latitude},{site.longitude}"),
        )
        return json.loads(text)

    def _grid_data(self, grid_url: str, site: SiteSettings) -> dict:
        slug = _slug(site)
        text = self.cache.read_text(
            "gridpoint/data",
            f"{slug}.json",
            lambda: self._download(grid_url),
        )
        return json.loads(text)

    def _iter_periods(self, values: Iterable[dict]) -> Iterable[Tuple[datetime, datetime, float | dict]]:
        for item in values:
            valid = item.get("validTime")
            value = item.get("value")
            if not valid:
                continue
            try:
                start, end = _parse_period(valid, self.tzinfo)
            except Exception as exc:  # pragma: no cover - defensive
                LOGGER.debug("Unable to parse validTime %s: %s", valid, exc)
                continue
            yield start, end, value

    def _bucket_numeric(self, values: Iterable[dict], agg: str = "max", transform=None) -> Dict[datetime.date, float]:
        bucket: Dict[datetime.date, List[float]] = defaultdict(list)
        for start, _end, raw in self._iter_periods(values):
            if raw is None:
                continue
            val = transform(raw) if transform else raw
            bucket[start.date()].append(val)
        summary: Dict[datetime.date, float] = {}
        for day, items in bucket.items():
            if not items:
                continue
            if agg == "sum":
                summary[day] = round(sum(items), 2)
            else:
                summary[day] = round(max(items), 1)
        return summary

    def _bucket_weather(self, values: Iterable[dict]) -> Dict[datetime.date, Tuple[str | None, str]]:
        phrases: Dict[datetime.date, List[str]] = defaultdict(list)
        for start, _end, payload in self._iter_periods(values):
            if not payload:
                continue
            for entry in payload:
                phrase = _weather_phrase(entry)
                if phrase:
                    phrases[start.date()].append(phrase)
        summary: Dict[datetime.date, Tuple[str | None, str]] = {}
        for day, items in phrases.items():
            if not items:
                continue
            unique = list(dict.fromkeys(items))
            summary[day] = (unique[0], ", ".join(unique))
        return summary

    def fetch(self, site: SiteSettings) -> List[SourceDailyRecord]:
        meta = self._point_metadata(site)
        grid_url = meta["properties"]["forecastGridData"]
        data = self._grid_data(grid_url, site)
        props = data.get("properties", {})

        highs = self._bucket_numeric(props.get("maxTemperature", {}).get("values", []), transform=c_to_f)
        lows = self._bucket_numeric(props.get("minTemperature", {}).get("values", []), transform=c_to_f)
        pops = self._bucket_numeric(props.get("probabilityOfPrecipitation", {}).get("values", []))
        qpf = self._bucket_numeric(
            props.get("quantitativePrecipitation", {}).get("values", []),
            agg="sum",
            transform=mm_to_inches,
        )
        weather = self._bucket_weather(props.get("weather", {}).get("values", []))

        bucket: Dict[date, SourceDailyRecord] = {}
        for day in sorted(set(highs) | set(lows) | set(pops) | set(qpf) | set(weather)):
            record = _ensure_record(bucket, site, day)
            if day in highs:
                record.high_f = highs[day]
            if day in lows:
                record.low_f = lows[day]
            if day in pops:
                record.pop_pct = max(record.pop_pct or 0, pops[day])
            if day in qpf and qpf[day] > 0:
                inches = qpf[day]
                note = f"NWS QPF {inches:.2f}\""
                record.precip_notes = f"{record.precip_notes} | {note}".strip(" |")
            if day in weather:
                primary, notes = weather[day]
                if primary:
                    record.precip_type = primary
                if notes:
                    existing = record.precip_notes
                    record.precip_notes = " | ".join(filter(None, [existing, notes]))

        ordered = sorted(bucket.keys())[: self.days]
        return [bucket[d] for d in ordered]
