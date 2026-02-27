from __future__ import annotations

from pyforecast.application.services.profiling_service import ProfilingService
from pyforecast.domain.timefreq import TimeFrequency


def test_wide_headers_infers_shape_wide_and_frequency_daily() -> None:
    svc = ProfilingService(sample_limit=50)

    # Wide: dates are in headers (daily cadence)
    columns = ["sku", "store", "2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"]

    # Preview rows don't need actual date column values in wide-header case
    preview_rows = [
        {"sku": "A", "store": "1", "2024-01-01": 10, "2024-01-02": 11, "2024-01-03": 12, "2024-01-04": 13},
        {"sku": "B", "store": "1", "2024-01-01": 5, "2024-01-02": 6, "2024-01-03": 7, "2024-01-04": 8},
    ]

    profile = svc.profile(columns, preview_rows)

    assert profile.shape == "wide"
    assert profile.frequency is not None
    assert profile.frequency.frequency == TimeFrequency.DAILY
    assert profile.notes is not None
    assert "headers" in profile.notes.lower()


def test_wide_headers_infers_frequency_monthly() -> None:
    svc = ProfilingService(sample_limit=50)

    # Wide: monthly cadence in headers
    columns = ["product", "2024-01", "2024-02", "2024-03", "2024-04"]

    preview_rows = [
        {"product": "X", "2024-01": 100, "2024-02": 110, "2024-03": 120, "2024-04": 125},
        {"product": "Y", "2024-01": 50, "2024-02": 55, "2024-03": 60, "2024-04": 62},
    ]

    profile = svc.profile(columns, preview_rows)

    assert profile.shape == "wide"
    assert profile.frequency is not None
    assert profile.frequency.frequency == TimeFrequency.MONTHLY
    assert profile.notes is not None
    assert "headers" in profile.notes.lower()


def test_wide_headers_no_date_like_headers_yields_no_frequency() -> None:
    svc = ProfilingService(sample_limit=50)

    # Wide but headers are not dates -> should not guess a frequency
    columns = ["sku", "store", "wk1", "wk2", "wk3"]

    preview_rows = [
        {"sku": "A", "store": "1", "wk1": 10, "wk2": 11, "wk3": 12},
        {"sku": "B", "store": "1", "wk1": 5, "wk2": 6, "wk3": 7},
    ]

    profile = svc.profile(columns, preview_rows)

    # Shape could still be wide, but frequency should be unavailable
    assert profile.shape == "wide"
    assert profile.frequency is None