from __future__ import annotations

from pathlib import Path
from typing import List

import pandas as pd

from ..models import DailyEnsemble


COMMON_COLUMNS = [
    "date",
    "label",
    "high_f",
    "low_f",
    "pop_pct",
    "precip_type",
    "precip_notes",
    "heat_category",
    "continuous_heavy_work_min",
    "hydration_cups_per_min",
    "work_rest_min",
    "supervisor_assessments_per_hr",
    "radio_checkins",
    "sources_count",
]


def _row_payload(row: DailyEnsemble) -> dict:
    return {
        "date": row.date.isoformat(),
        "label": row.label,
        "high_f": row.high_f,
        "low_f": row.low_f,
        "pop_pct": row.pop_pct,
        "precip_type": row.precip_type,
        "precip_notes": row.precip_notes,
        "heat_category": row.heat_category or "",
        "continuous_heavy_work_min": row.heat_guidance.get("continuous_heavy_work_min", ""),
        "hydration_cups_per_min": row.heat_guidance.get("hydration_cups_per_min", ""),
        "work_rest_min": row.heat_guidance.get("work_rest_min", ""),
        "supervisor_assessments_per_hr": row.heat_guidance.get("supervisor_assessments_per_hr", ""),
        "radio_checkins": row.heat_guidance.get("radio_checkins", ""),
        "sources_count": row.sources_count,
    }


def write_home_csv(rows: List[DailyEnsemble], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame([_row_payload(row) for row in rows], columns=COMMON_COLUMNS)
    df.to_csv(path, index=False)
    return path


def write_work_csv(rows: List[DailyEnsemble], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    records = []
    for row in rows:
        payload = _row_payload(row)
        payload.update(
            {
                "freeze_risk_badge": row.freeze_risk_badge or "",
                "freeze_guidance": row.freeze_guidance or "",
            }
        )
        records.append(payload)
    columns = COMMON_COLUMNS + ["freeze_risk_badge", "freeze_guidance"]
    df = pd.DataFrame(records, columns=columns)
    df.to_csv(path, index=False)
    return path
