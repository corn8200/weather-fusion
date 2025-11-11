from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, NamedTuple, Tuple

try:  # pragma: no cover - optional heavy dependency
    import xarray as xr
except ImportError:  # pragma: no cover
    xr = None

from ..config import SiteSettings
from ..models import SourceDailyRecord
from ..util.time import format_day_label
from .cache import CacheManager

LOGGER = logging.getLogger(__name__)
BASE_URL = "https://noaa-nbm-grib2-pds.s3.amazonaws.com"
DOMAIN = "co"
FIELD_WINDOW_HOURS = 12
TMP_SAMPLE_STEP = 3
MM_TO_INCH = 0.0393701


class GribIndexEntry(NamedTuple):
    number: int
    offset: int
    description: str


@dataclass
class CycleInfo:
    when: datetime
    ymd: str
    cycle_hour: str


def kelvin_to_f(value: float) -> float:
    return (value - 273.15) * 9.0 / 5.0 + 32.0


def mm_to_inches(value: float) -> float:
    return value * MM_TO_INCH


def meters_to_inches(value: float) -> float:
    return value * 39.3701


def parse_index(text: str) -> List[GribIndexEntry]:
    entries: List[GribIndexEntry] = []
    for line in filter(None, text.splitlines()):
        try:
            number_str, offset_str, *rest = line.split(":")
            entries.append(GribIndexEntry(int(number_str), int(offset_str), ":".join(rest)))
        except ValueError:  # pragma: no cover - defensive
            continue
    return entries


class NBMIngestor:
    source_name = "nbm_grib"

    def __init__(self, session, cache: CacheManager, days: int, tzinfo) -> None:
        self.session = session
        self.cache = cache
        self.days = days
        self.tzinfo = tzinfo
        self._cycle: CycleInfo | None = None
        self._field_cache: Dict[Tuple[str, int], Any] = {}

    def _build_candidate_cycles(self) -> Iterable[datetime]:
        now = datetime.now(UTC)
        rounded = (now.hour // 6) * 6
        base = datetime(now.year, now.month, now.day, rounded, tzinfo=UTC)
        for step in range(0, 48, 6):
            yield base - timedelta(hours=step)

    def _select_cycle(self) -> CycleInfo:
        if self._cycle:
            return self._cycle
        for candidate in self._build_candidate_cycles():
            ymd = candidate.strftime("%Y%m%d")
            hour = candidate.strftime("%H")
            test_url = self._idx_url(ymd, hour, 24)
            resp = self.session.head(test_url, timeout=30)
            if resp.status_code == 200:
                self._cycle = CycleInfo(candidate, ymd, hour)
                LOGGER.info("Selected NBM cycle %s %sz", ymd, hour)
                return self._cycle
        raise RuntimeError("Unable to find a recent NBM cycle with CONUS data")

    def _idx_url(self, ymd: str, hour: str, fhour: int) -> str:
        return f"{BASE_URL}/blend.{ymd}/{hour}/core/blend.t{hour}z.core.f{fhour:03d}.{DOMAIN}.grib2.idx"

    def _grib_url(self, ymd: str, hour: str, fhour: int) -> str:
        return f"{BASE_URL}/blend.{ymd}/{hour}/core/blend.t{hour}z.core.f{fhour:03d}.{DOMAIN}.grib2"

    def _cache_namespace(self, cycle: CycleInfo) -> str:
        return f"nbm/{cycle.ymd}/{cycle.cycle_hour}"

    def _load_index(self, cycle: CycleInfo, fhour: int) -> List[GribIndexEntry]:
        namespace = self._cache_namespace(cycle)
        def _getter() -> bytes:
            resp = self.session.get(self._idx_url(cycle.ymd, cycle.cycle_hour, fhour), timeout=60)
            resp.raise_for_status()
            return resp.content

        idx_file = self.cache.fetch(namespace, f"f{fhour:03d}.idx", _getter)
        return parse_index(idx_file.path.read_text())

    def _download_slice(self, cycle: CycleInfo, fhour: int, start: int, end: int | None, tag: str) -> Path:
        namespace = self._cache_namespace(cycle)
        headers = {"Range": f"bytes={start}-{end}" if end is not None else f"bytes={start}-"}
        def _getter() -> bytes:
            resp = self.session.get(self._grib_url(cycle.ymd, cycle.cycle_hour, fhour), headers=headers, timeout=120)
            resp.raise_for_status()
            return resp.content

        slice_file = self.cache.fetch(namespace, f"f{fhour:03d}_{tag}.grib2", _getter)
        return slice_file.path

    def _find_entry(self, entries: List[GribIndexEntry], token: str) -> Tuple[int, int | None]:
        for idx, entry in enumerate(entries):
            if token not in entry.description or "std dev" in entry.description:
                continue
            start = entry.offset
            end = entries[idx + 1].offset - 1 if idx + 1 < len(entries) else None
            return start, end
        raise RuntimeError(f"Field {token} not present in GRIB index")

    def _load_data(self, cycle: CycleInfo, fhour: int, short_name: str):
        if xr is None:  # pragma: no cover - import-time guard
            raise RuntimeError(
                "xarray/cfgrib are required for GRIB ingest. Install optional deps: pip install xarray cfgrib eccodes"
            )
        key = (short_name, fhour)
        if key in self._field_cache:
            return self._field_cache[key]
        entries = self._load_index(cycle, fhour)
        start, end = self._find_entry(entries, f":{short_name}:")
        grib_path = self._download_slice(cycle, fhour, start, end, short_name.lower())
        ds = xr.open_dataset(
            grib_path,
            engine="cfgrib",
            backend_kwargs={"filter_by_keys": {"shortName": short_name}},
        )
        var_name = next(iter(ds.data_vars))
        data = ds[var_name].load()
        ds.close()
        self._field_cache[key] = data
        return data

    def _extract_value(self, data: xr.DataArray, lat: float, lon: float) -> float:
        point = data.sel(latitude=lat, longitude=lon, method="nearest")
        return float(point.values)

    def _sample_field(
        self,
        sites: Iterable[SiteSettings],
        fhour: int,
        short_name: str,
        converter: Callable[[float], float] | None = None,
    ) -> Dict[str, float]:
        cycle = self._select_cycle()
        data = self._load_data(cycle, fhour, short_name)
        samples: Dict[str, float] = {}
        for site in sites:
            value = self._extract_value(data, site.latitude, site.longitude)
            if converter:
                value = converter(value)
            samples[site.name] = value
        return samples

    def _sample_optional(
        self,
        site: SiteSettings,
        fhour: int,
        short_name: str,
        converter: Callable[[float], float] | None = None,
    ) -> float | None:
        try:
            return self._sample_field([site], fhour, short_name, converter=converter)[site.name]
        except Exception as exc:  # pragma: no cover - best effort ancillary fields
            LOGGER.debug(
                "NBM sample failed for %s %s fhour=%s: %s: %s",
                short_name,
                site.name,
                fhour,
                type(exc).__name__,
                exc,
            )
            return None

    def _derive_daily_temp(self, site: SiteSettings, day_idx: int, mode: str) -> float | None:
        """Approximate daily highs/lows using 3-hour TMP fields when max/min slices are missing."""
        start_hour = day_idx * 24
        end_hour = (day_idx + 1) * 24
        temps: List[float] = []
        hours: List[int] = []
        if start_hour == 0:
            hours.append(0)
        hours.extend(range(max(3, start_hour + 3), end_hour + 1, TMP_SAMPLE_STEP))
        for fhour in hours:
            value = self._sample_optional(site, fhour, "TMP", converter=kelvin_to_f)
            if value is not None:
                temps.append(value)
        if not temps:
            return None
        return max(temps) if mode == "high" else min(temps)

    @staticmethod
    def _append_note(record: SourceDailyRecord, fragment: str) -> None:
        fragment = fragment.strip()
        if not fragment:
            return
        if record.precip_notes:
            record.precip_notes += " | " + fragment
        else:
            record.precip_notes = fragment

    def fetch(self, site: SiteSettings) -> List[SourceDailyRecord]:
        cycle = self._select_cycle()
        LOGGER.info("Fetching NBM slices for %s", site.name)
        records: Dict[date, SourceDailyRecord] = {}
        base_day = cycle.when.astimezone(self.tzinfo).date()
        for day_idx in range(self.days):
            # High temps from 12h windows ending at multiples of 24h
            high_hour = (day_idx + 1) * 24
            low_hour = day_idx * 24 + FIELD_WINDOW_HOURS
            target_day = base_day + timedelta(days=day_idx)

            # Ensure the record exists so we can still use PoP/QPF/Snow even if highs/lows fail
            rec = records.setdefault(
                target_day,
                SourceDailyRecord(
                    site_name=site.name,
                    date=target_day,
                    label=format_day_label(target_day),
                    source=self.source_name,
                ),
            )

            # High temperature with fallbacks
            high_value = None
            try:
                high_value = self._sample_field([site], high_hour, "TMAX", converter=kelvin_to_f)[site.name]
            except Exception as exc:
                LOGGER.warning("Unable to sample TMAX for day %s: %s", day_idx, exc)
                try:
                    high_value = self._sample_field([site], high_hour, "MAXT", converter=kelvin_to_f)[site.name]
                    LOGGER.info("NBM fallback MAXT used for day %s", day_idx)
                except Exception as exc2:
                    LOGGER.debug("NBM fallback MAXT failed for day %s: %s", day_idx, exc2)
            if high_value is not None:
                rec.high_f = high_value
            else:
                derived_high = self._derive_daily_temp(site, day_idx, "high")
                if derived_high is not None:
                    rec.high_f = derived_high
                    LOGGER.info("NBM derived TMP high used for day %s", day_idx)

            # Low temperature with fallbacks
            try:
                low_value = self._sample_field([site], low_hour, "TMIN", converter=kelvin_to_f)[site.name]
                rec.low_f = low_value
            except Exception as exc:
                LOGGER.warning("Unable to sample TMIN for day %s: %s", day_idx, exc)
                try:
                    low_value = self._sample_field([site], low_hour, "MINT", converter=kelvin_to_f)[site.name]
                    rec.low_f = low_value
                    LOGGER.info("NBM fallback MINT used for day %s", day_idx)
                except Exception as exc2:
                    LOGGER.debug("NBM fallback MINT failed for day %s: %s", day_idx, exc2)
                    derived_low = self._derive_daily_temp(site, day_idx, "low")
                    if derived_low is not None:
                        rec.low_f = derived_low
                        LOGGER.info("NBM derived TMP low used for day %s", day_idx)
            pop_hours = {max(day_idx * 24 + FIELD_WINDOW_HOURS, FIELD_WINDOW_HOURS), (day_idx + 1) * 24}
            pop_values: List[float] = []
            qpf_total_inches = 0.0
            snow_total_inches = 0.0
            for fhour in pop_hours:
                pop_val = self._sample_optional(site, fhour, "POP12")
                if pop_val is not None:
                    pop_values.append(pop_val)
                qpf_mm = self._sample_optional(site, fhour, "APCP")
                if qpf_mm is not None:
                    qpf_total_inches += mm_to_inches(qpf_mm)
                snow_m = self._sample_optional(site, fhour, "ASNOW")
                if snow_m is not None:
                    snow_total_inches += meters_to_inches(snow_m)
            if pop_values:
                best_pop = max(pop_values)
                rec.pop_pct = max(rec.pop_pct or 0, best_pop)
            note_frags: List[str] = []
            if qpf_total_inches > 0:
                rec.qpf_inches = round(qpf_total_inches, 2)
                note_frags.append(f"NBM QPF {rec.qpf_inches:.2f}\"")
            if snow_total_inches > 0:
                rec.snow_inches = round(snow_total_inches, 2)
                note_frags.append(f"NBM Snow {rec.snow_inches:.2f}\"")
            if note_frags:
                self._append_note(rec, "; ".join(note_frags))
        ordered = sorted(records.keys())[: self.days]
        return [records[d] for d in ordered]
