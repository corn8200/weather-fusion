from __future__ import annotations

import calendar
import logging
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, Iterable, List, Tuple
from xml.etree import ElementTree as ET

import feedparser
from dateutil import parser as dtparser

from ..config import AppSettings, SiteSettings
from ..models import SourceDailyRecord
from ..util.time import format_day_label
from .cache import CacheManager

LOGGER = logging.getLogger(__name__)
RSS_URL = "https://forecast.weather.gov/MapClick.php"


def _slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower())


@dataclass
class DwmlSeries:
    times: List[datetime]
    values: List[str]


def _parse_time_layouts(root: ET.Element, tzinfo) -> Dict[str, List[datetime]]:
    layouts: Dict[str, List[datetime]] = {}
    for layout in root.findall(".//time-layout"):
        key = layout.findtext("layout-key")
        if not key:
            continue
        times = []
        for node in layout.findall("start-valid-time"):
            try:
                dt = dtparser.isoparse(node.text)
            except (TypeError, ValueError):
                continue
            times.append(dt.astimezone(tzinfo))
        layouts[key] = times
    return layouts


def _ensure_record(bucket: Dict[date, SourceDailyRecord], site: SiteSettings, day: date) -> SourceDailyRecord:
    if day not in bucket:
        bucket[day] = SourceDailyRecord(
            site_name=site.name,
            date=day,
            label=format_day_label(day),
            source="nws_rss",
        )
    return bucket[day]


PRECIP_PRIORITY = [
    "Freezing Rain",
    "Ice Pellets",
    "Snow",
    "Sleet",
    "Rain",
    "Showers",
    "Drizzle",
    "Thunderstorms",
]


def _summarize_precip(types: Iterable[str]) -> Tuple[str | None, str]:
    seen = list(dict.fromkeys([t for t in types if t]))
    if not seen:
        return None, ""
    for preferred in PRECIP_PRIORITY:
        if preferred in seen:
            primary = preferred
            break
    else:  # pragma: no cover - defensive
        primary = seen[0]
    notes = ", ".join(seen)
    return primary, notes


def parse_dwml(xml_text: str, site: SiteSettings, days: int, tzinfo) -> List[SourceDailyRecord]:
    root = ET.fromstring(xml_text)
    layouts = _parse_time_layouts(root, tzinfo)
    daily: Dict[date, SourceDailyRecord] = {}

    for temp_node in root.findall(".//temperature"):
        temp_type = temp_node.get("type")
        layout_key = temp_node.get("time-layout")
        if not layout_key or layout_key not in layouts:
            continue
        times = layouts[layout_key]
        values = [node.text for node in temp_node.findall("value")]
        for ts, val in zip(times, values, strict=False):
            day = ts.date()
            record = _ensure_record(daily, site, day)
            try:
                num = float(val)
            except (TypeError, ValueError):
                continue
            if temp_type == "maximum":
                record.high_f = num
            elif temp_type == "minimum":
                record.low_f = num

    for pop_node in root.findall(".//probability-of-precipitation"):
        layout_key = pop_node.get("time-layout")
        if not layout_key or layout_key not in layouts:
            continue
        times = layouts[layout_key]
        values = [node.text for node in pop_node.findall("value")]
        for ts, val in zip(times, values, strict=False):
            day = ts.date()
            record = _ensure_record(daily, site, day)
            try:
                num = float(val) if val not in (None, "") else None
            except ValueError:
                num = None
            if num is None:
                continue
            if record.pop_pct is None:
                record.pop_pct = num
            else:
                record.pop_pct = max(record.pop_pct, num)

    weather_notes: Dict[date, List[str]] = defaultdict(list)
    weather_types: Dict[date, List[str]] = defaultdict(list)
    for weather_node in root.findall(".//weather"):
        layout_key = weather_node.get("time-layout")
        if not layout_key or layout_key not in layouts:
            continue
        times = layouts[layout_key]
        value_nodes = weather_node.findall("value")
        for ts, value_node in zip(times, value_nodes, strict=False):
            day = ts.date()
            summary = value_node.get("weather-summary")
            if summary:
                weather_notes[day].append(summary)
            for condition in value_node.findall("weather-conditions"):
                wtype = condition.get("weather-type")
                if not wtype or wtype == "none":
                    continue
                normalized = wtype.replace("_", " ").title()
                coverage = condition.get("coverage")
                intensity = condition.get("intensity")
                descriptor = normalized
                if intensity and intensity not in {"none", "moderate"}:
                    descriptor = f"{intensity.title()} {descriptor}"
                if coverage and coverage not in {"definite"}:
                    descriptor = f"{coverage.title()} {descriptor}"
                weather_types[day].append(descriptor)

    for wf_node in root.findall(".//wordedForecast"):
        layout_key = wf_node.get("time-layout")
        if not layout_key or layout_key not in layouts:
            continue
        times = layouts[layout_key]
        texts = [node.text or "" for node in wf_node.findall("text")]
        for ts, text in zip(times, texts, strict=False):
            day = ts.date()
            record = _ensure_record(daily, site, day)
            normalized = text.strip()
            if not normalized:
                continue
            if record.notes:
                record.notes += " | " + normalized
            else:
                record.notes = normalized
            lowered = normalized.lower()
            if any(token in lowered for token in ("breezy", "wind", "gust")):
                record.wind_phrase = normalized

    for day, record in daily.items():
        ptype, notes = _summarize_precip(weather_types.get(day, []))
        if ptype:
            record.precip_type = ptype
        if notes or weather_notes.get(day):
            fragments = weather_notes.get(day, [])
            if notes:
                fragments.insert(0, notes)
            record.precip_notes = "; ".join(dict.fromkeys(fragments))

    ordered_days = sorted(daily.keys())[:days]
    return [daily[d] for d in ordered_days]


def parse_rss(text: str, site: SiteSettings, days: int, tzinfo) -> List[SourceDailyRecord]:
    feed = feedparser.parse(text)
    daily: Dict[date, SourceDailyRecord] = {}
    temp_pattern = re.compile(r"(High|Low)\s*:?\s*(\-?\d+)\s*°?F", re.IGNORECASE)

    for entry in feed.entries:
        ts_raw = entry.get("published") or entry.get("updated")
        if ts_raw:
            ts = dtparser.isoparse(ts_raw).astimezone(tzinfo)
        elif entry.get("published_parsed"):
            ts = datetime.fromtimestamp(calendar.timegm(entry.published_parsed), tzinfo)
        else:
            continue
        day = ts.date()
        record = _ensure_record(daily, site, day)
        text = " ".join(filter(None, [entry.get("title") or "", entry.get("summary") or ""]))
        for match in temp_pattern.finditer(text):
            kind, value = match.groups()
            deg = float(value)
            if kind.lower() == "high":
                record.high_f = deg
            else:
                record.low_f = deg
        pop_match = re.search(r"(\d+)%", text)
        if pop_match:
            pop = float(pop_match.group(1))
            record.pop_pct = max(record.pop_pct or 0, pop)
        # precipitation keywords
        for keyword, label in {
            "snow": "Snow",
            "freezing": "Freezing Rain",
            "sleet": "Sleet",
            "ice": "Ice Pellets",
            "rain": "Rain",
        }.items():
            if keyword in text.lower():
                record.precip_type = label
                break
        record.precip_notes = text.strip()
        if any(token in text.lower() for token in ("breezy", "wind", "gust")):
            record.wind_phrase = text.strip()

    ordered_days = sorted(daily.keys())[:days]
    return [daily[d] for d in ordered_days]


class RSSIngestor:
    source_name = "nws_rss"

    def __init__(self, settings: AppSettings, session, cache: CacheManager) -> None:
        self.settings = settings
        self.session = session
        self.cache = cache

    def _download_feed(self, site: SiteSettings) -> str:
        params = {
            "lat": site.latitude,
            "lon": site.longitude,
            "FcstType": "rss",
        }
        slug = _slug(site.name)
        cached = self.cache.fetch(
            "rss",
            f"{slug}.xml",
            lambda: self._http_get(params),
        )
        text = cached.path.read_text()
        if "<rss" not in text.lower():
            LOGGER.warning("MapClick RSS unavailable for %s, falling back to DWML", site.name)
            dwml = self.session.get(
                RSS_URL,
                params={"lat": site.latitude, "lon": site.longitude, "FcstType": "dwml"},
                timeout=60,
            )
            dwml.raise_for_status()
            text = dwml.text
            cached.path.write_text(text)
        return text

    def _http_get(self, params: Dict[str, str]) -> bytes:
        resp = self.session.get(RSS_URL, params=params, timeout=60)
        resp.raise_for_status()
        return resp.text.encode("utf-8")

    def fetch(self, site: SiteSettings, days: int, tzinfo) -> List[SourceDailyRecord]:
        payload = self._download_feed(site)
        if "<rss" in payload.lower():
            return parse_rss(payload, site, days, tzinfo)
        return parse_dwml(payload, site, days, tzinfo)
