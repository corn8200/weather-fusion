from __future__ import annotations

import calendar
import logging
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, Iterable, List, Tuple

import feedparser
from dateutil import parser as dtparser

from ..config import AppSettings, SiteSettings
from ..models import SourceDailyRecord
from ..util.time import format_day_label
from .cache import CacheManager
from .dwml import parse_dwml

LOGGER = logging.getLogger(__name__)
RSS_URL = "https://forecast.weather.gov/MapClick.php"


def _slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower())



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
        self.days = settings.days
        self.tzinfo = settings.tzinfo

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

    def fetch(self, site: SiteSettings) -> List[SourceDailyRecord]:
        payload = self._download_feed(site)
        if "<rss" in payload.lower():
            return parse_rss(payload, site, self.days, self.tzinfo)
        return parse_dwml(payload, site, self.days, self.tzinfo, self.source_name)
