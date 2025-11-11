from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Dict, Iterable, List, Tuple
from xml.etree import ElementTree as ET

from dateutil import parser as dtparser

from ..config import SiteSettings
from ..models import SourceDailyRecord
from ..util.time import format_day_label

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


def _parse_time_layouts(root: ET.Element, tzinfo) -> Dict[str, List]:
    layouts: Dict[str, List] = {}
    for layout in root.findall(".//time-layout"):
        key = layout.findtext("layout-key")
        if not key:
            continue
        times: List = []
        for node in layout.findall("start-valid-time"):
            try:
                dt = dtparser.isoparse(node.text)
            except (TypeError, ValueError):
                continue
            times.append(dt.astimezone(tzinfo))
        layouts[key] = times
    return layouts


def _ensure_record(bucket: Dict[date, SourceDailyRecord], site: SiteSettings, day: date, source: str) -> SourceDailyRecord:
    if day not in bucket:
        bucket[day] = SourceDailyRecord(
            site_name=site.name,
            date=day,
            label=format_day_label(day),
            source=source,
        )
    return bucket[day]


def _summarize_precip(types: Iterable[str]) -> Tuple[str | None, str]:
    seen = list(dict.fromkeys([t for t in types if t]))
    if not seen:
        return None, ""
    for preferred in PRECIP_PRIORITY:
        if preferred in seen:
            primary = preferred
            break
    else:
        primary = seen[0]
    notes = ", ".join(seen)
    return primary, notes


def _convert_amount(value: str | None, units: str | None) -> float | None:
    if value is None or value.strip() == "":
        return None
    try:
        numeric = float(value)
    except ValueError:
        return None
    units = (units or "").lower()
    if units in {"inches", "inch", "in"}:
        return round(numeric, 2)
    if units in {"mm", "millimeters"}:
        return round(numeric * 0.0393701, 2)
    if units in {"kg/m^2", "kg/m2"}:
        return round(numeric * 0.0393701, 2)
    if units in {"m"}:
        return round(numeric * 39.3701, 2)
    return round(numeric, 2)


def parse_dwml(xml_text: str, site: SiteSettings, days: int, tzinfo, source_name: str = "nws_rss") -> List[SourceDailyRecord]:
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
            record = _ensure_record(daily, site, day, source_name)
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
            record = _ensure_record(daily, site, day, source_name)
            try:
                num = float(val) if val not in (None, "") else None
            except ValueError:
                num = None
            if num is None:
                continue
            record.pop_pct = max(record.pop_pct or 0, num)

    def _accumulate_amount(tag: str, attr_type: str, field: str):
        for node in root.findall(f".//{tag}"):
            node_type = (node.get("type") or "").lower()
            if attr_type and node_type != attr_type:
                continue
            layout_key = node.get("time-layout")
            if not layout_key or layout_key not in layouts:
                continue
            units = node.get("units")
            values = [child.text for child in node.findall("value")]
            for ts, val in zip(layouts[layout_key], values, strict=False):
                day = ts.date()
                record = _ensure_record(daily, site, day, source_name)
                amount = _convert_amount(val, units)
                if amount is None or amount <= 0:
                    continue
                current = getattr(record, field)
                setattr(record, field, round((current or 0) + amount, 2))

    _accumulate_amount("precipitation", "liquid", "qpf_inches")
    _accumulate_amount("precipitation", "snow", "snow_inches")
    _accumulate_amount("precipitation", "ice", "ice_inches")
    _accumulate_amount("snow-amount", "", "snow_inches")
    _accumulate_amount("ice-accumulation", "", "ice_inches")

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
            record = _ensure_record(daily, site, day, source_name)
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
            record = _ensure_record(daily, site, day, source_name)
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
            record.precip_notes = "; ".join(dict.fromkeys(filter(None, fragments)))

    ordered_days = sorted(daily.keys())[:days]
    return [daily[d] for d in ordered_days]
