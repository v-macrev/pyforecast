from __future__ import annotations

from datetime import date, timedelta

from pyforecast.domain.timefreq import TimeFrequency, infer_frequency


def _mk_dates(start: date, step_days: int, n: int) -> list[date]:
    return [start + timedelta(days=step_days * i) for i in range(n)]


def test_infer_daily() -> None:
    res = infer_frequency(_mk_dates(date(2024, 1, 1), 1, 20))
    assert res.frequency == TimeFrequency.DAILY
    assert res.confidence >= 0.7


def test_infer_weekly() -> None:
    res = infer_frequency(_mk_dates(date(2024, 1, 1), 7, 20))
    assert res.frequency == TimeFrequency.WEEKLY
    assert res.confidence >= 0.7


def test_infer_monthly_approx() -> None:
    res = infer_frequency(_mk_dates(date(2024, 1, 1), 30, 20))
    assert res.frequency == TimeFrequency.MONTHLY
    assert res.confidence >= 0.6


def test_infer_yearly() -> None:
    res = infer_frequency(_mk_dates(date(2010, 1, 1), 365, 10))
    assert res.frequency == TimeFrequency.YEARLY
    assert res.confidence >= 0.6


def test_irregular_when_sparse() -> None:
    res = infer_frequency([date(2024, 1, 1), date(2024, 2, 1)])
    assert res.frequency == TimeFrequency.IRREGULAR


def test_irregular_when_inconsistent() -> None:
    dates = [
        date(2024, 1, 1),
        date(2024, 1, 2),
        date(2024, 1, 10),
        date(2024, 1, 11),
        date(2024, 2, 20),
        date(2024, 2, 21),
    ]
    res = infer_frequency(dates)
    assert res.frequency == TimeFrequency.IRREGULAR