# Weather Fusion Architecture

## Overview

The application ingests forecast data along two paths:

1. **Public-file ingest (NBM GRIB2 slices)** — downloads TMAX/TMIN slices via `.idx` offsets from NOAA's NBM S3 bucket, samples the nearest grid cell with `cfgrib/xarray`, and caches byte ranges under `.cache/`.
2. **RSS/DWML ingest** — requests the MapClick RSS feed; if the endpoint returns HTML instead of XML (current behavior), it transparently falls back to the DWML daily XML product and parses temperatures, PoP12, weather summaries, and narrative forecasts.

Both sources emit `SourceDailyRecord` rows that carry highs/lows, PoP, precip descriptors, wind phrases, and metadata. The ensemble stage merges per-day values, enforces sanity checks, averages temperatures, selects dominant precipitation based on severity, and computes EHS badges (heat and freeze guidance).

Outputs include polished HTML (with sparklines, chips, and accessibility affordances), CSVs for Home/Work, and optional Gmail SMTP delivery (CSV attachments). GitHub Actions runs the CLI twice daily with cron scheduling tuned for EST/EDT.

## Key Modules

- `weatherfusion.config`: Loads `.env`/CLI settings, resolves/caches work coordinates via MapClick HTML scraping.
- `weatherfusion.ingest.grib`: Handles `.idx` parsing, HTTP range downloads, and xarray sampling for TMAX/TMIN.
- `weatherfusion.ingest.rss`: Parses RSS (when available) or DWML XML, normalizing PoP, precipitation text, and wind clues.
- `weatherfusion.processing.ensemble`: Aligns sources, applies EHS rules, flags low-confidence days, and assembles `DailyEnsemble` records.
- `weatherfusion.report.html` / `report/csv`: Render HTML email/report and persist CSV artifacts.
- `weatherfusion.pipeline`: Ties everything together, coordinating caching, logging, email delivery, and run summaries.

## Caching & Retries

HTTP requests share a session with retry/backoff, and every network artifact is cached beneath `.cache/` with TTL-driven freshness. CLI `--no-cache` disables reuse.

## Testing

Fixture-based tests exercise the DWML parser, ensemble alignment, and EHS classifiers without making network calls, enabling CI execution.
