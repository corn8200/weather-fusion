from __future__ import annotations

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

NY_TZ = ZoneInfo("America/New_York")


def now_utc() -> datetime:
    return datetime.now(UTC)


def to_local(dt: datetime, tz: ZoneInfo = NY_TZ) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(tz)


def local_date_from_midpoint(start: datetime, end: datetime, tz: ZoneInfo = NY_TZ) -> datetime.date:
    midpoint = start + (end - start) / 2
    return to_local(midpoint, tz).date()


def format_day_label(day: datetime.date) -> str:
    return day.strftime("%a %b %d")


def iter_cycle_candidates(now: datetime, depth: int = 6) -> list[datetime]:
    rounded_hour = (now.hour // 6) * 6
    base = datetime(now.year, now.month, now.day, rounded_hour, tzinfo=UTC)
    return [base - timedelta(hours=6 * i) for i in range(depth)]
