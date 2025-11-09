from __future__ import annotations

from collections import Counter, defaultdict
from statistics import mean
from typing import Iterable, List

from ..models import DailyEnsemble, SourceDailyRecord
from .ehs import LIGHTNING_NOTE, classify_freeze, classify_heat

TEMP_LIMITS = {
    "high": (-40.0, 130.0),
    "low": (-60.0, 95.0),
}
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


def _sanitize(value: float | None, key: str) -> float | None:
    if value is None:
        return None
    lo, hi = TEMP_LIMITS[key]
    if not (lo <= value <= hi):
        return None
    return value


def _mean(values: Iterable[float | None]) -> float | None:
    filtered = [v for v in values if v is not None]
    if not filtered:
        return None
    return round(mean(filtered), 1)


def _dominant_precip(types: Iterable[str | None]) -> str | None:
    filtered = [t for t in types if t]
    if not filtered:
        return None
    counter = Counter(filtered)
    for label in PRECIP_PRIORITY:
        if label in counter:
            return label
    return counter.most_common(1)[0][0]


def build_site_ensembles(site_name: str, records: List[SourceDailyRecord], days: int) -> List[DailyEnsemble]:
    grouped = defaultdict(list)
    for record in records:
        grouped[record.date].append(record)
    output: List[DailyEnsemble] = []
    for day in sorted(grouped.keys()):
        bucket = grouped[day]
        highs = [_sanitize(rec.high_f, "high") for rec in bucket]
        lows = [_sanitize(rec.low_f, "low") for rec in bucket]
        high = _mean(highs)
        low = _mean(lows)
        if high is not None and low is not None and low > high:
            low = None
        if high is None and low is None:
            continue
        pop_values = [rec.pop_pct for rec in bucket if rec.pop_pct is not None]
        pop_pct = round(max(pop_values), 1) if pop_values else None
        precip_type = _dominant_precip([rec.precip_type for rec in bucket])
        precip_notes = " | ".join(
            dict.fromkeys(filter(None, [rec.precip_notes for rec in bucket]))
        )
        breezy = any(
            (
                (rec.wind_phrase and any(token in rec.wind_phrase.lower() for token in ("breezy", "wind", "gust")))
                or (rec.notes and any(token in rec.notes.lower() for token in ("breezy", "wind", "gust")))
            )
            for rec in bucket
        )
        heat_category, heat_guidance = classify_heat(high)
        freeze_badge, freeze_guidance = classify_freeze(low, breezy)
        sources = sorted({rec.source for rec in bucket})
        label = bucket[0].label or day.strftime("%a %b %d")
        output.append(
            DailyEnsemble(
                site_name=site_name,
                date=day,
                label=label,
                high_f=high,
                low_f=low,
                pop_pct=pop_pct,
                precip_type=precip_type,
                precip_notes=precip_notes,
                heat_category=heat_category,
                heat_guidance=heat_guidance,
                freeze_risk_badge=freeze_badge,
                freeze_guidance=freeze_guidance,
                sources=sources,
                sources_count=len(sources),
                low_confidence=len(sources) < 2,
                lightning_note=LIGHTNING_NOTE,
            )
        )
        if len(output) >= days:
            break
    return output
