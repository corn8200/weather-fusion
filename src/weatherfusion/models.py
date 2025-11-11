from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import List, Optional


@dataclass(slots=True)
class SourceDailyRecord:
    site_name: str
    date: date
    label: str
    source: str
    high_f: Optional[float] = None
    low_f: Optional[float] = None
    pop_pct: Optional[float] = None
    precip_type: Optional[str] = None
    precip_notes: str = ""
    wind_phrase: Optional[str] = None
    notes: str = ""


@dataclass(slots=True)
class DailyEnsemble:
    site_name: str
    date: date
    label: str
    high_f: Optional[float]
    low_f: Optional[float]
    pop_pct: Optional[float]
    precip_type: Optional[str]
    precip_notes: str
    heat_category: Optional[str]
    heat_guidance: dict[str, str]
    freeze_risk_badge: Optional[str] = None
    freeze_guidance: Optional[str] = None
    sources: List[str] = field(default_factory=list)
    sources_count: int = 0
    low_confidence: bool = False
    lightning_note: str | None = None


@dataclass(slots=True)
class RunSummary:
    generated_at: datetime
    sources_ok: dict[str, List[str]]
    sources_failed: dict[str, List[str]]
    html_report: str
    csv_paths: dict[str, str]
    email_sent: bool
    png_report: Optional[str] = None
