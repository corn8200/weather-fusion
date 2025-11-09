from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from .config import AppSettings
from .ingest.cache import CacheManager
from .ingest.grib import NBMIngestor
from .ingest.rss import RSSIngestor
from .models import RunSummary
from .processing.ensemble import build_site_ensembles
from .report.csv import write_home_csv, write_work_csv
from .report.html import render_report
from .util.emailer import EmailClient
from .util.http import create_session
from .util.logging import setup_logging

LOGGER = logging.getLogger(__name__)


def _ingestor_order(settings: AppSettings, nbm: NBMIngestor, rss: RSSIngestor) -> List:
    order: List = []
    if settings.primary_ingest == "PUBLIC_FILES":
        order.append(nbm)
        if settings.rss_fallback:
            order.append(rss)
    else:
        order.append(rss)
        if settings.rss_fallback:
            order.append(nbm)
    if len(order) == 1 and order[0] is rss and settings.primary_ingest == "PUBLIC_FILES":
        order.append(nbm)
    return order


def run_pipeline(settings: AppSettings) -> RunSummary:
    setup_logging(settings.logs_dir)
    session = create_session(settings.user_agent)
    cache_root = Path(".cache")
    cache = CacheManager(cache_root, 0 if settings.no_cache else settings.cache_ttl_hours)

    nbm = NBMIngestor(session, cache, settings.days, settings.tzinfo)
    rss = RSSIngestor(settings, session, cache)

    site_map = {
        settings.home.name: settings.home,
        settings.work.name: settings.work,
    }

    records: Dict[str, List] = {settings.home.name: [], settings.work.name: []}
    sources_ok: Dict[str, List[str]] = {settings.home.name: [], settings.work.name: []}
    sources_failed: Dict[str, List[str]] = {settings.home.name: [], settings.work.name: []}

    for ingestor in _ingestor_order(settings, nbm, rss):
        for site_name, site in site_map.items():
            try:
                site_data = ingestor.fetch(site)
                if site_data:
                    records[site_name].extend(site_data)
                    if ingestor.source_name not in sources_ok[site_name]:
                        sources_ok[site_name].append(ingestor.source_name)
                else:
                    sources_failed[site_name].append(f"{ingestor.source_name}: no data")
            except Exception as exc:  # pragma: no cover - network failure path
                LOGGER.exception("%s ingest failed for %s", ingestor.source_name, site_name)
                sources_failed[site_name].append(f"{ingestor.source_name}: {exc}")

    home_rows = build_site_ensembles(settings.home.name, records[settings.home.name], settings.days)
    work_rows = build_site_ensembles(settings.work.name, records[settings.work.name], settings.days)

    generated_at = datetime.now(settings.tzinfo)
    stamp = generated_at.strftime("%Y%m%d")
    html_path = settings.out_dir / f"report_{stamp}.html"
    home_csv = settings.out_dir / f"home_best_{stamp}.csv"
    work_csv = settings.out_dir / f"work_best_{stamp}.csv"
    settings.out_dir.mkdir(parents=True, exist_ok=True)

    metadata = {
        "sources_ok": {k: ", ".join(v) or "—" for k, v in sources_ok.items()},
        "sources_failed": {k: "; ".join(v) or "—" for k, v in sources_failed.items()},
    }
    metadata["sources_ok_display"] = " | ".join(f"{k}: {v}" for k, v in metadata["sources_ok"].items())
    metadata["sources_failed_display"] = " | ".join(f"{k}: {v}" for k, v in metadata["sources_failed"].items())

    html = render_report(generated_at, home_rows, work_rows, metadata)
    html_path.write_text(html, encoding="utf-8")
    write_home_csv(home_rows, home_csv)
    write_work_csv(work_rows, work_csv)

    email_sent = False
    if settings.email.enabled and not settings.html_only:
        email_client = EmailClient(settings)
        attachments = {
            "home": home_csv,
            "work": work_csv,
        }
        subject = "EHS 10-Day Forecast — Home & Work (Martinsburg / Inwood)"
        email_sent = email_client.send(subject, html, attachments)

    return RunSummary(
        generated_at=generated_at,
        sources_ok=sources_ok,
        sources_failed=sources_failed,
        html_report=str(html_path),
        csv_paths={"home": str(home_csv), "work": str(work_csv)},
        email_sent=email_sent,
    )
