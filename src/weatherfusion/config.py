from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Optional
from urllib.parse import parse_qs, urlparse

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError

from zoneinfo import ZoneInfo

DEFAULT_USER_AGENT = "ForecastAggregator/1.0 (contact: you@example.com)"
ZIPCITY_URL = "https://forecast.weather.gov/zipcity.php"


class EmailSettings(BaseModel):
    sender: Optional[str] = Field(default=None, alias="MAIL_FROM")
    recipient: Optional[str] = Field(default=None, alias="MAIL_TO")
    host: Optional[str] = Field(default=None, alias="SMTP_HOST")
    port: int = Field(default=587, alias="SMTP_PORT")
    username: Optional[str] = Field(default=None, alias="SMTP_USER")
    password: Optional[str] = Field(default=None, alias="SMTP_PASS")

    model_config = {
        "populate_by_name": True,
    }

    @property
    def enabled(self) -> bool:
        return all([self.sender, self.recipient, self.host, self.username, self.password])


class SiteSettings(BaseModel):
    name: str
    latitude: float
    longitude: float
    address: Optional[str] = None


class AppSettings(BaseModel):
    days: int = Field(default=10)
    primary_ingest: Literal["PUBLIC_FILES", "RSS"] = Field(default="PUBLIC_FILES")
    rss_fallback: bool = Field(default=True)
    cache_ttl_hours: int = Field(default=3)
    user_agent: str = Field(default=DEFAULT_USER_AGENT)
    tz: str = Field(default="America/New_York")
    out_dir: Path = Field(default=Path("out"))
    logs_dir: Path = Field(default=Path("logs"))
    no_cache: bool = Field(default=False)
    html_only: bool = Field(default=False)
    home: SiteSettings
    work: SiteSettings
    email: EmailSettings

    @property
    def tzinfo(self):  # pragma: no cover - thin helper
        return ZoneInfo(self.tz)


def _env_bool(key: str, default: bool = False) -> bool:
    value = os.getenv(key)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def _env_int(key: str, default: int) -> int:
    value = os.getenv(key)
    if value is None:
        return default
    return int(value)


def _env_float(key: str, default: float) -> float:
    value = os.getenv(key)
    if value is None or value.strip() == "":
        return default
    return float(value)


def _cli_or_env_float(cli_value: Any | None, env_key: str, default: float) -> float:
    if cli_value is not None:
        return float(cli_value)
    return _env_float(env_key, default)


def _maybe_read_cached_coords(path: Path) -> Optional[tuple[float, float]]:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text())
        return float(payload["lat"]), float(payload["lon"])
    except Exception:
        return None


def _write_cached_coords(path: Path, lat: float, lon: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"lat": lat, "lon": lon}, indent=2))


def _resolve_work_coords(address: str, out_dir: Path, session: requests.Session) -> tuple[float, float]:
    cache_path = out_dir / "work_coords.json"
    cached = _maybe_read_cached_coords(cache_path)
    if cached:
        return cached

    query = quote_plus(address)
    resp = session.get(ZIPCITY_URL, params={"inputstring": address}, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    for anchor in soup.find_all("a"):
        href = anchor.get("href", "")
        if "MapClick.php" in href and "lat=" in href and "lon=" in href:
            parsed = urlparse(href)
            qs = parse_qs(parsed.query)
            try:
                lat = float(qs.get("lat", [None])[0])
                lon = float(qs.get("lon", [None])[0])
            except (TypeError, ValueError):
                continue
            _write_cached_coords(cache_path, lat, lon)
            return lat, lon
    raise RuntimeError("Unable to resolve work coordinates from NWS zipcity search")


def load_settings(cli_args: dict[str, Any] | None = None) -> AppSettings:
    load_dotenv()
    cli_args = cli_args or {}

    out_dir = Path(cli_args.get("out_dir") or os.getenv("OUT_DIR", "out")).expanduser()
    logs_dir = Path(cli_args.get("logs_dir") or os.getenv("LOGS_DIR", "logs")).expanduser()

    session = requests.Session()
    session.headers.update({"User-Agent": os.getenv("USER_AGENT", DEFAULT_USER_AGENT)})

    home = SiteSettings(
        name=cli_args.get("place_home") or os.getenv("PLACE_HOME", "Home"),
        latitude=_cli_or_env_float(cli_args.get("home_lat"), "HOME_LAT", 39.3381),
        longitude=_cli_or_env_float(cli_args.get("home_lon"), "HOME_LON", -77.7925),
    )

    work_lat = cli_args.get("work_lat") or os.getenv("WORK_LAT")
    work_lon = cli_args.get("work_lon") or os.getenv("WORK_LON")
    work_address = cli_args.get("work_address") or os.getenv("WORK_ADDRESS", "1042 Development Drive, Inwood, WV")

    if work_lat and work_lon:
        lat, lon = float(work_lat), float(work_lon)
        _write_cached_coords(out_dir / "work_coords.json", lat, lon)
    else:
        lat, lon = _resolve_work_coords(work_address, out_dir, session)

    work = SiteSettings(
        name=cli_args.get("place_work") or os.getenv("PLACE_WORK", work_address),
        latitude=lat,
        longitude=lon,
        address=work_address,
    )

    data: dict[str, Any] = {
        "days": int(cli_args.get("days") or os.getenv("DAYS", 10)),
        "primary_ingest": (cli_args.get("primary") or os.getenv("PRIMARY_INGEST", "PUBLIC_FILES")).upper(),
        "rss_fallback": cli_args.get("rss_fallback") if cli_args.get("rss_fallback") is not None else _env_bool("RSS_FALLBACK", True),
        "cache_ttl_hours": int(cli_args.get("cache_ttl_hours") or os.getenv("CACHE_TTL_HOURS", 3)),
        "user_agent": cli_args.get("user_agent") or os.getenv("USER_AGENT", DEFAULT_USER_AGENT),
        "tz": cli_args.get("tz") or os.getenv("TZ", "America/New_York"),
        "out_dir": out_dir,
        "logs_dir": logs_dir,
        "no_cache": bool(cli_args.get("no_cache")),
        "html_only": bool(cli_args.get("html_only")),
        "home": home,
        "work": work,
        "email": EmailSettings(
            MAIL_FROM=os.getenv("MAIL_FROM"),
            MAIL_TO=os.getenv("MAIL_TO"),
            SMTP_HOST=os.getenv("SMTP_HOST"),
            SMTP_PORT=int(os.getenv("SMTP_PORT", 587)),
            SMTP_USER=os.getenv("SMTP_USER"),
            SMTP_PASS=os.getenv("SMTP_PASS"),
        ),
    }

    try:
        settings = AppSettings(**data)
    except ValidationError as exc:  # pragma: no cover - startup guard
        raise RuntimeError(f"Invalid configuration: {exc}")

    settings.out_dir.mkdir(parents=True, exist_ok=True)
    settings.logs_dir.mkdir(parents=True, exist_ok=True)
    return settings
