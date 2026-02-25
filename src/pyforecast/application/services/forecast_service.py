from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from pyforecast.domain.canonical_schema import CANON
from pyforecast.domain.errors import ForecastError, TransformationError
from pyforecast.domain.timefreq import TimeFrequency
from pyforecast.infrastructure.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class ForecastRequest:
    """
    Forecasts a canonical long dataset (cd_key, ds, y).

    Notes on scale:
    - This implementation writes ONE parquet per series (cd_key) to keep memory bounded.
    - A later optimisation can batch keys or write a single partitioned dataset.
    """
    canonical_path: Path  # parquet produced by transform_service
    frequency: TimeFrequency
    horizon: int  # periods ahead
    out_dir: Path | None = None
    max_series: int = 500  # safety guard for desktop
    min_points: int = 12   # skip very short series
    prophet_seasonality_mode: str = "additive"  # "additive" | "multiplicative"


@dataclass(frozen=True)
class ForecastResult:
    output_dir: Path
    series_forecast_files: list[Path]
    skipped_series: int
    notes: str | None = None


def _require_polars() -> "pl":  # type: ignore[name-defined]
    try:
        import polars as pl  # type: ignore
    except Exception as exc:
        raise ForecastError("Polars is required. Install with: pip install -e '.[data]'") from exc
    return pl


def _require_prophet() -> "Prophet":  # type: ignore[name-defined]
    try:
        from prophet import Prophet  # type: ignore
    except Exception as exc:
        raise ForecastError("Prophet is required. Install with: pip install -e '.[forecast]'") from exc
    return Prophet


def _freq_to_pandas(freq: TimeFrequency) -> str:
    """
    Pandas date_range freq string (Prophet future dataframe cadence).
    """
    if freq == TimeFrequency.DAILY:
        return "D"
    if freq == TimeFrequency.WEEKLY:
        return "W"
    if freq == TimeFrequency.MONTHLY:
        return "MS"   # month start
    if freq == TimeFrequency.QUARTERLY:
        return "QS"   # quarter start
    if freq == TimeFrequency.YEARLY:
        return "YS"   # year start
    raise ForecastError(f"Unsupported frequency for forecasting: {freq.name}")


def _safe_filename(s: str, max_len: int = 120) -> str:
    # filesystem-safe, deterministic
    cleaned = "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in s)
    return cleaned[:max_len] if len(cleaned) > max_len else cleaned


def _default_out_dir(canonical_path: Path, out_dir: Path | None) -> Path:
    base = out_dir or (Path.home() / ".pyforecast" / "outputs")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return base / "forecasts" / f"{canonical_path.stem}__{ts}"


def forecast_prophet(req: ForecastRequest) -> ForecastResult:
    if not req.canonical_path.exists():
        raise TransformationError(f"Canonical dataset not found: {req.canonical_path}")
    if req.horizon <= 0:
        raise ForecastError("horizon must be >= 1")
    if req.max_series <= 0:
        raise ForecastError("max_series must be >= 1")
    if req.min_points < 2:
        raise ForecastError("min_points must be >= 2")

    pl = _require_polars()
    Prophet = _require_prophet()
    pd_freq = _freq_to_pandas(req.frequency)

    out_dir = _default_out_dir(req.canonical_path, req.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Load key list (bounded) with lazy scan
    keys_df = (
        pl.scan_parquet(req.canonical_path)
        .select(pl.col(CANON.cd_key))
        .unique()
        .limit(req.max_series)
        .collect()
    )
    keys = keys_df[CANON.cd_key].to_list()  # type: ignore[no-any-return]

    if not keys:
        raise ForecastError("No cd_key values found in canonical dataset.")

    skipped = 0
    out_files: list[Path] = []

    log.info(
        "forecast_start",
        extra={
            "canonical_path": str(req.canonical_path),
            "frequency": req.frequency.value,
            "horizon": req.horizon,
            "max_series": req.max_series,
            "min_points": req.min_points,
            "out_dir": str(out_dir),
        },
    )

    # Iterate series one by one to keep memory bounded
    for i, key in enumerate(keys, start=1):
        try:
            lf = (
                pl.scan_parquet(req.canonical_path)
                .filter(pl.col(CANON.cd_key) == key)
                .select([CANON.ds, CANON.y])
                .drop_nulls([CANON.ds, CANON.y])
                .sort(CANON.ds)
            )

            df = lf.collect()
            if df.height < req.min_points:
                skipped += 1
                continue

            # Prophet wants pandas with ds as datetime64 and y float
            pdf = df.to_pandas()
            # ensure datetime
            pdf[CANON.ds] = pdf[CANON.ds].astype("datetime64[ns]")

            m = Prophet(seasonality_mode=req.prophet_seasonality_mode)
            m.fit(pdf.rename(columns={CANON.ds: "ds", CANON.y: "y"}))

            future = m.make_future_dataframe(periods=req.horizon, freq=pd_freq, include_history=True)
            fc = m.predict(future)

            # Standard output columns
            fc_out = fc[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
            fc_out[CANON.cd_key] = key
            fc_out["run_frequency"] = req.frequency.value
            fc_out["run_horizon"] = req.horizon

            out_path = out_dir / f"{i:05d}__{_safe_filename(str(key))}.parquet"

            # Write per-series parquet
            pl.from_pandas(fc_out).write_parquet(out_path)
            out_files.append(out_path)

            if i % 25 == 0 or i == len(keys):
                log.info(
                    "forecast_progress",
                    extra={"done": i, "total": len(keys), "written": len(out_files), "skipped": skipped},
                )

        except Exception as exc:
            skipped += 1
            log.warning(
                "forecast_series_failed",
                extra={"cd_key": str(key), "error": str(exc)},
            )

    notes = (
        "Outputs are written as one parquet per cd_key to keep memory bounded. "
        "If you need a single file or partitions, we can add a merge/export step."
    )

    log.info(
        "forecast_done",
        extra={
            "out_dir": str(out_dir),
            "written": len(out_files),
            "skipped": skipped,
        },
    )

    return ForecastResult(
        output_dir=out_dir,
        series_forecast_files=out_files,
        skipped_series=skipped,
        notes=notes,
    )