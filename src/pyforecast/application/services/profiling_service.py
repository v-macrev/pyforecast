from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Iterable

from pyforecast.domain.errors import DateInferenceError, SchemaInferenceError
from pyforecast.domain.timefreq import FrequencyResult, infer_frequency
from pyforecast.infrastructure.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class ProfileResult:
    shape: str  # "long" | "wide" | "unknown"
    date_candidates: list[str]
    inferred_date_column: str | None
    frequency: FrequencyResult | None
    notes: str | None = None


def _looks_like_date(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, (date, datetime)):
        return True
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return False
        
        return any(ch.isdigit() for ch in s) and any(sep in s for sep in ("/", "-", ".", " "))
    return False


def _try_parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d", "%d-%m-%Y", "%m-%d-%Y"):
            try:
                return datetime.strptime(s[:10], fmt).date()
            except Exception:
                continue
    return None


def _non_null_values(rows: Iterable[dict[str, Any]], col: str, limit: int = 200) -> list[Any]:
    out: list[Any] = []
    for r in rows:
        if col in r and r[col] is not None and str(r[col]).strip() != "":
            out.append(r[col])
        if len(out) >= limit:
            break
    return out


class ProfilingService:

    def __init__(self, sample_limit: int = 200) -> None:
        self._sample_limit = max(50, sample_limit)

    def profile(self, columns: list[str], preview_rows: list[dict[str, object]]) -> ProfileResult:
        if not columns:
            raise SchemaInferenceError("No columns detected.")

        date_candidates = self._find_date_candidates(columns, preview_rows)
        inferred_date = date_candidates[0] if date_candidates else None

        freq: FrequencyResult | None = None
        notes: str | None = None

        if inferred_date is not None:
            parsed = self._parse_dates(preview_rows, inferred_date)
            if len(parsed) >= 3:
                freq = infer_frequency(parsed)
            else:
                notes = f"Could not parse enough dates from '{inferred_date}' (parsed={len(parsed)})."
        else:
            notes = "No date column candidate found."

        shape = self._infer_shape(columns, preview_rows, inferred_date)

        log.info(
            "profile_done",
            extra={
                "shape": shape,
                "date_candidates": date_candidates[:5],
                "inferred_date": inferred_date,
                "freq": (freq.frequency.value if freq else None),
                "freq_conf": (freq.confidence if freq else None),
            },
        )

        return ProfileResult(
            shape=shape,
            date_candidates=date_candidates,
            inferred_date_column=inferred_date,
            frequency=freq,
            notes=notes,
        )

    def _find_date_candidates(self, columns: list[str], rows: list[dict[str, object]]) -> list[str]:
        scored: list[tuple[str, float]] = []
        for col in columns:
            vals = _non_null_values(rows, col, limit=self._sample_limit)
            if not vals:
                continue
            
            looks = sum(1 for v in vals if _looks_like_date(v))
            ratio = looks / len(vals)
            header_bonus = 0.15 if any(tok in col.lower() for tok in ("date", "data", "dt", "dia", "mes", "ano")) else 0
            score = ratio + header_bonus
            if score >= 0.55:
                scored.append((col, score))

        scored.sort(key=lambda x: x[1], reverse=True)
        return [c for c, _ in scored]

    def _parse_dates(self, rows: list[dict[str, object]], date_col: str) -> list[date]:
        vals = _non_null_values(rows, date_col, limit=self._sample_limit)
        parsed: list[date] = []
        for v in vals:
            d = _try_parse_date(v)
            if d is not None:
                parsed.append(d)
        return parsed

    def _infer_shape(
        self,
        columns: list[str],
        rows: list[dict[str, object]],
        inferred_date_col: str | None,
    ) -> str:

        if not rows:
            return "unknown"

        header_dateish = sum(1 for c in columns if _looks_like_date(c))
        if header_dateish >= max(6, int(0.35 * len(columns))):
            return "wide"

        if inferred_date_col and len(columns) <= 15:
            return "long"

        return "unknown"


def require_frequency(profile: ProfileResult) -> FrequencyResult:
    if profile.frequency is None:
        raise DateInferenceError(profile.notes or "Could not infer frequency.")
    return profile.frequency