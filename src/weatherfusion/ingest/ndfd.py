from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict

from ..config import AppSettings, SiteSettings
from .cache import CacheManager
from .dwml import parse_dwml

NDFD_URL = "https://graphical.weather.gov/xml/SOAP_server/ndfdBrowserClientByLatLon.php"


class NdfdIngestor:
    source_name = "nws_ndfd"

    def __init__(self, settings: AppSettings, session, cache: CacheManager) -> None:
        self.settings = settings
        self.session = session
        self.cache = cache

    def _download(self, params: Dict[str, str]) -> bytes:
        resp = self.session.get(NDFD_URL, params=params, timeout=60)
        resp.raise_for_status()
        return resp.content

    def fetch(self, site: SiteSettings):
        now = datetime.now(self.settings.tzinfo)
        end = now + timedelta(days=self.settings.days + 1)
        params = {
            "whichClient": "NDFDgenLatLonList",
            "lat": f"{site.latitude:.4f}",
            "lon": f"{site.longitude:.4f}",
            "product": "time-series",
            "begin": now.strftime("%Y-%m-%dT%H:%M:%S"),
            "end": end.strftime("%Y-%m-%dT%H:%M:%S"),
            "Unit": "e",
            "pop12": "pop12",
            "qpf": "qpf",
            "snow": "snow",
            "iceaccum": "iceaccum",
            "wspd": "wspd",
            "wgust": "wgust",
        }
        slug = f"{site.latitude:.4f}_{site.longitude:.4f}".replace("-", "m").replace(".", "d")
        text = self.cache.read_text(
            "ndfd",
            f"{slug}.xml",
            lambda: self._download(params),
        )
        return parse_dwml(text, site, self.settings.days, self.settings.tzinfo, self.source_name)
