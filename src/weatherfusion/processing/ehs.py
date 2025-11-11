from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Tuple


LIGHTNING_NOTE = "Cease outdoor work when thunder is heard; resume 30 min after last lightning."


@dataclass(frozen=True)
class HeatBand:
    name: str
    threshold_f: float
    guidance: Dict[str, str]


HEAT_BANDS = [
    HeatBand(
        name="Extreme Danger",
        threshold_f=125,
        guidance={
            "continuous_heavy_work_min": "0",
            "hydration_cups_per_min": "≥1/10",
            "work_rest_min": "10/20/10",
            "supervisor_assessments_per_hr": "4",
            "radio_checkins": "q15m",
        },
    ),
    HeatBand(
        name="Danger",
        threshold_f=100,
        guidance={
            "continuous_heavy_work_min": "10",
            "hydration_cups_per_min": "1/10–15",
            "work_rest_min": "20/30/10",
            "supervisor_assessments_per_hr": "2",
            "radio_checkins": "q30m",
        },
    ),
    HeatBand(
        name="Extreme Caution",
        threshold_f=90,
        guidance={
            "continuous_heavy_work_min": "15",
            "hydration_cups_per_min": "1/15–20",
            "work_rest_min": "30/40/10",
            "supervisor_assessments_per_hr": "1",
            "radio_checkins": "start+q1h",
        },
    ),
    HeatBand(
        name="Caution",
        threshold_f=80,
        guidance={
            "continuous_heavy_work_min": "30",
            "hydration_cups_per_min": "1/20",
            "work_rest_min": "Normal",
            "supervisor_assessments_per_hr": "0 (periodic)",
            "radio_checkins": "start+q2h",
        },
    ),
]

DEFAULT_HEAT_GUIDANCE = {
    "continuous_heavy_work_min": "Normal",
    "hydration_cups_per_min": "Baseline",
    "work_rest_min": "Normal",
    "supervisor_assessments_per_hr": "0",
    "radio_checkins": "start",
}


def classify_heat(high_f: Optional[float]) -> Tuple[Optional[str], Dict[str, str]]:
    if high_f is None:
        return None, DEFAULT_HEAT_GUIDANCE
    for band in HEAT_BANDS:
        if high_f >= band.threshold_f:
            return band.name, band.guidance
    return None, DEFAULT_HEAT_GUIDANCE


FREEZE_GUIDANCE = {
    "None": "",
    "Frost": "Cover exposed sensors; monitor slick surfaces; plan extra footing checks.",
    "Freeze": "Limit time on elevated surfaces; stage warm shelters; confirm cold-weather PPE/buddy checks.",
    "Hard Freeze": "Pause non-essential outdoor handling; enforce short outdoor rotations; keep warming shelter within reach.",
}


def classify_freeze(low_f: Optional[float], breezy: bool) -> Tuple[Optional[str], Optional[str]]:
    if low_f is None:
        return None, None
    if low_f <= 28:
        badge = "Hard Freeze"
    elif low_f <= 32:
        badge = "Freeze"
    elif low_f <= 36:
        badge = "Frost"
    else:
        badge = None
    if not badge:
        return None, None
    guidance = FREEZE_GUIDANCE[badge]
    if breezy and low_f <= 32:
        guidance += " Wind-chill risk: add face/hand protection."
    return badge, guidance
