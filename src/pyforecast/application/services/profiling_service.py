from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable

from pyforecast.domain.timefreq import FrequencyResult, TimeFrequency, infer_frequency
from pyforecast.infrastructure.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class ProfileResult:
    shape: str  # "long" | "wide"
    date_candidates: list[str]
    inferred_date_column: str | None
    frequency: FrequencyResult | None
    notes: str | None = None


def require_frequency(profile: ProfileResult) -> FrequencyResult:
    if profile.frequency is None:
        raise ValueError("Frequency is not available in the profile.")
    return profile.frequency


class ProfilingService:
    """
    Profiles a dataset from preview-only data.

    Key improvement:
    - If the dataset looks WIDE and dates appear in headers, infer frequency from header dates.
    """

    def __init__(self, sample_limit: int = 200) -> None:
        self._sample_limit = sample_limit

    def profile(self, columns: list[str], preview_rows: list[dict[str, object]]) -> ProfileResult:
        shape = self._infer_shape(columns, preview_rows)

        date_candidates = self._find_date_candidates(columns, preview_rows)
        inferred_date_col = date_candidates[0] if date_candidates else None

        freq: FrequencyResult | None = None
        notes: str | None = None

        # LONG: infer from date column values (existing behaviour)
        if shape == "long" and inferred_date_col:
            dates = self._extract_dates_from_rows(preview_rows, inferred_date_col)
            if len(dates) >= 3:
                freq = infer_frequency(dates)

        # WIDE: if we can't reliably infer from a date column, try parsing header dates
        if shape == "wide" and (freq is None):
            header_dates = self._extract_dates_from_headers(columns)
            if len(header_dates) >= 3:
                freq = infer_frequency(header_dates)
                notes = "Frequency inferred from date-like column headers (wide format)."

        return ProfileResult(
            shape=shape,
            date_candidates=date_candidates,
            inferred_date_column=inferred_date_col if shape == "long" else inferred_date_col,
            frequency=freq,
            notes=notes,
        )

    # ---------- shape & candidates ----------

    def _infer_shape(self, columns: list[str], preview_rows: list[dict[str, object]]) -> str:
        """
        Heuristic:
        - If we have a clear date column candidate AND a clear value-like column candidate => long
        - Otherwise assume wide (common: many period columns).
        """
        # If many columns and many look date-like in header, it's probably wide.
        header_dates = self._extract_dates_from_headers(columns)
        if len(header_dates) >= 3:
            return "wide"

        # Fallback: look for a date column in data values
        date_cands = self._find_date_candidates(columns, preview_rows)
        return "long" if date_cands else "wide"

    def _find_date_candidates(self, columns: list[str], preview_rows: list[dict[str, object]]) -> list[str]:
        """
        Return columns that look like they contain date values.
        Conservative: only returns candidates that parse in at least ~30% of sampled rows.
        """
        if not preview_rows:
            return []

        sample = preview_rows[: self._sample_limit]
        out: list[str] = []

        for col in columns:
            ok = 0
            seen = 0
            for r in sample:
                v = r.get(col)
                if v is None or (isinstance(v, str) and not v.strip()):
                    continue
                seen += 1
                if self._parse_any_date(v) is not None:
                    ok += 1
            if seen >= 6 and ok >= max(3, int(seen * 0.30)):
                out.append(col)

        return out

    # ---------- date extraction ----------

    def _extract_dates_from_rows(self, rows: list[dict[str, object]], date_col: str) -> list[date]:
        sample = rows[: self._sample_limit]
        dates: list[date] = []
        for r in sample:
            d = self._parse_any_date(r.get(date_col))
            if d is not None:
                dates.append(d)
        # unique + sorted
        return sorted(set(dates))

    def _extract_dates_from_headers(self, columns: Iterable[str]) -> list[date]:
        dates: list[date] = []
        for c in columns:
            d = self._parse_header_date(str(c))
            if d is not None:
                dates.append(d)
        return sorted(set(dates))

    # ---------- parsing ----------

    _HEADER_DATE_FORMATS = (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%Y%m%d",
        "%Y-%m",
        "%Y/%m",
        "%Y%m",
        "%b-%Y",  # Jan-2024
        "%b/%Y",
        "%Y-%b",
    )

    def _parse_header_date(self, s: str) -> date | None:
        """
        Parse dates from header labels.
        Supports common monthly headers (YYYY-MM, YYYYMM) by mapping to day=1.
        """
        s = s.strip()
        if not s:
            return None

        for fmt in self._HEADER_DATE_FORMATS:
            try:
                dt = datetime.strptime(s, fmt)
                # If format lacks day, strptime sets day=1 already
                return dt.date()
            except ValueError:
                continue

        return None

    def _parse_any_date(self, v: object) -> date | None:
        """
        Parse date from cell values.
        Handles:
        - python date/datetime
        - strings in common formats
        - Excel serials (best-effort)
        """
        if v is None:
            return None
        if isinstance(v, datetime):
            return v.date()
        if isinstance(v, date):
            return v

        # Excel serial date (common): days since 1899-12-30
        if isinstance(v, (int, float)):
            iv = int(v)
            if 20_000 <= iv <= 80_000:
                base = date(1899, 12, 30)
                try:
                    return base.fromordinal(base.toordinal() + iv)
                except Exception:
                    return None

        if isinstance(v, str):
            s = v.strip()
            if not s:
                return None
            for fmt in self._HEADER_DATE_FORMATS:
                try:
                    return datetime.strptime(s, fmt).date()
                except ValueError:
                    continue

        return None