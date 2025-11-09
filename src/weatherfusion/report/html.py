from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Sequence

from jinja2 import Environment, FileSystemLoader, select_autoescape

from ..models import DailyEnsemble

TEMPLATE_DIR = Path(__file__).parent / "templates"


@dataclass
class Sparkline:
    d: str
    min_value: float | None
    max_value: float | None


def _sparkline(values: Sequence[float | None], width: int = 240, height: int = 56) -> Sparkline:
    points = [v for v in values if v is not None]
    if len(points) < 2:
        return Sparkline("", None, None)
    min_v = min(points)
    max_v = max(points)
    span = max(max_v - min_v, 1e-3)
    step = width / (len(values) - 1)
    cmds: List[str] = []
    for idx, value in enumerate(values):
        if value is None:
            continue
        x = round(idx * step, 1)
        y = round(height - ((value - min_v) / span) * height, 1)
        cmd = "M" if not cmds else "L"
        cmds.append(f"{cmd}{x},{y}")
    return Sparkline(" ".join(cmds), round(min_v, 1), round(max_v, 1))


def _temp_style(value: float | None, kind: str) -> str:
    if value is None:
        return ""
    clamp_min, clamp_max = (-10, 110)
    pct = (value - clamp_min) / (clamp_max - clamp_min)
    pct = max(0.0, min(1.0, pct))
    color = "rgba(255, 105, 97, 0.35)" if kind == "high" else "rgba(65, 147, 255, 0.35)"
    return f"background: linear-gradient(90deg, {color} {pct * 100:.1f}%, transparent {pct * 100:.1f}%);"


def _format_temp(value: float | None) -> str:
    return f"{value:.0f}°" if value is not None else "—"


def _format_pop(value: float | None) -> str:
    return f"{value:.0f}%" if value is not None else "—"


def _env() -> Environment:
    loader = FileSystemLoader(TEMPLATE_DIR)
    return Environment(loader=loader, autoescape=select_autoescape(["html", "xml"]))


def render_report(
    generated_at: datetime,
    home_rows: List[DailyEnsemble],
    work_rows: List[DailyEnsemble],
    metadata: dict,
) -> str:
    env = _env()
    template = env.get_template("report.html.j2")
    context = {
        "generated_at": generated_at,
        "home": {
            "rows": home_rows,
            "spark_high": _sparkline([row.high_f for row in home_rows]),
            "spark_low": _sparkline([row.low_f for row in home_rows]),
        },
        "work": {
            "rows": work_rows,
            "spark_high": _sparkline([row.high_f for row in work_rows]),
            "spark_low": _sparkline([row.low_f for row in work_rows]),
        },
        "meta": metadata,
        "format_temp": _format_temp,
        "format_pop": _format_pop,
        "temp_style": _temp_style,
    }
    return template.render(**context)
