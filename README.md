# Weather Fusion — Dual-Path Forecast Emailer

This project builds a twice-daily, EHS-focused 10-day forecast email that blends public NOAA gridded files (NBM/NDFD GRIB2 slices) with the NWS MapClick RSS feed as a fallback.

## Highlights

- Primary ingest from National Blend of Models CONUS GRIB2 slices with byte-range downloads via `.idx` offsets.
- Automatic fallback to MapClick RSS/DWML feeds and ensemble averaging across sources (always-on PoP + precip-type tracking).
- Categorizes daily heat/cold risk with tailored worker guidance and freeze badges specific to the Work site.
- Renders a polished HTML email with sparklines, zebra tables, chips, dark-mode friendly palette, and accessibility helpers; also writes CSV artifacts per site.
- Supports caching, retries with exponential backoff, Gmail SMTP delivery, and GitHub Actions automation for 06:00/18:00 ET runs.

## Local usage

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
cp .env.example .env  # fill in secrets / overrides
ehs_forecast --out out --logs-dir logs
```

Key CLI flags:

- `--home-lat/--home-lon` and `--work-lat/--work-lon` override env coordinates.
- `--work-address` triggers a fresh MapClick lookup + caches under `out/work_coords.json`.
- `--no-cache` forces all GRIB/RSS fetches even if cached versions exist.
- `--html-only` skips email delivery even when SMTP credentials are present.

Artifacts land in `out/` (`report_YYYYMMDD.html`, `home_best_*.csv`, `work_best_*.csv`) and logs rotate under `logs/app.log`.

## CI

`.github/workflows/forecast.yml` schedules four cron windows so both 06:00 and 18:00 ET runs fire regardless of DST. A lightweight guard file prevents duplicate invocations, and the workflow uploads the `out/` and `logs/` directories for every run.

See `docs/ARCHITECTURE.md` for module-by-module details.
