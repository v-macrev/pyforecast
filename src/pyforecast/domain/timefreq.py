from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Iterable, Sequence


class TimeFrequency(str, Enum):
    DAILY = "D"
    WEEKLY = "W"
    MONTHLY = "M"
    QUARTERLY = "Q"
    YEARLY = "Y"
    IRREGULAR = "IRREGULAR"


@dataclass(frozen=True)
class FrequencyResult:
    frequency: TimeFrequency
    confidence: float  # 0..1
    n_points: int
    median_delta_days: float | None
    notes: str | None = None


def _to_date_list(values: Iterable[date | datetime]) -> list[date]:
    out: list[date] = []
    for v in values:
        if isinstance(v, datetime):
            out.append(v.date())
        elif isinstance(v, date):
            out.append(v)
        else:
            raise TypeError(f"Unsupported date type: {type(v)}")
    return out


def _sorted_unique(dates: Sequence[date]) -> list[date]:
    return sorted(set(dates))


def _diff_days(a: date, b: date) -> int:
    return (b - a).days


def _median(nums: Sequence[int]) -> float | None:
    if not nums:
        return None
    s = sorted(nums)
    n = len(s)
    mid = n // 2
    if n % 2 == 1:
        return float(s[mid])
    return (s[mid - 1] + s[mid]) / 2.0


def infer_frequency(dates: Iterable[date | datetime]) -> FrequencyResult:
    dlist = _sorted_unique(_to_date_list(dates))
    n = len(dlist)
    if n < 3:
        return FrequencyResult(
            frequency=TimeFrequency.IRREGULAR,
            confidence=0.0,
            n_points=n,
            median_delta_days=None,
            notes="Too few points to infer frequency (need >= 3 unique dates).",
        )

    deltas = [_diff_days(dlist[i], dlist[i + 1]) for i in range(n - 1)]
    med = _median(deltas)
    if med is None:
        return FrequencyResult(
            frequency=TimeFrequency.IRREGULAR,
            confidence=0.0,
            n_points=n,
            median_delta_days=None,
            notes="No deltas available.",
        )

    def frac_within(tol: int) -> float:
        ok = sum(1 for d in deltas if abs(d - med) <= tol)
        return ok / len(deltas)

    candidates: list[tuple[TimeFrequency, float, int]] = [
        (TimeFrequency.DAILY, 1.0, 0),
        (TimeFrequency.WEEKLY, 7.0, 1),
        (TimeFrequency.MONTHLY, 30.0, 3),
        (TimeFrequency.QUARTERLY, 91.0, 7),
        (TimeFrequency.YEARLY, 365.0, 15),
    ]

    best: tuple[TimeFrequency, float, float, float] | None = None  # (freq, score, tol, target)
    for freq, target, tol in candidates:
        closeness = max(0.0, 1.0 - (abs(med - target) / max(target, 1.0)))
        consistency = frac_within(tol)
        score = 0.55 * consistency + 0.45 * closeness
        if best is None or score > best[1]:
            best = (freq, score, float(tol), float(target))

    assert best is not None
    freq, score, tol, target = best

    base_consistency = frac_within(int(tol))
    if base_consistency < 0.65:
        return FrequencyResult(
            frequency=TimeFrequency.IRREGULAR,
            confidence=min(0.49, score),
            n_points=n,
            median_delta_days=med,
            notes="Deltas are inconsistent; classified as IRREGULAR.",
        )

    max_delta = max(deltas) if deltas else 0
    if freq in (TimeFrequency.DAILY, TimeFrequency.WEEKLY):
        gap_threshold = target * 3
    else:
        gap_threshold = target * 2

    large_gaps = sum(1 for d in deltas if d > gap_threshold)
    if large_gaps >= 1:
        return FrequencyResult(
            frequency=TimeFrequency.IRREGULAR,
            confidence=min(0.55, score),
            n_points=n,
            median_delta_days=med,
            notes=f"Detected {large_gaps} large gap(s) (max_delta={max_delta}d) breaking cadence.",
        )

    confidence = max(0.0, min(1.0, score))
    return FrequencyResult(
        frequency=freq,
        confidence=confidence,
        n_points=n,
        median_delta_days=med,
        notes=None,
    )