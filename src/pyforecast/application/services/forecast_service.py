from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pyforecast.domain.canonical_schema import CANON
from pyforecast.domain.errors import ForecastError, PyForecastError
from pyforecast.domain.timefreq import TimeFrequency
from pyforecast.infrastructure.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class ForecastRequest:
    canonical_path: Path
    frequency: TimeFrequency
    horizon: int
    out_dir: Path

    # ✅ New (backwards-compatible): also export CSV
    # - None => export both parquet + csv
    # - ("csv",) => only csv
    # - ("parquet",) => only parquet
    out_formats: tuple[str, ...] | None = None

    # Series quality gates
    min_points: int = 10  # skip series with less than this many observations


@dataclass(frozen=True)
class ForecastResult:
    output_dir: Path
    series_forecast_files: list[str]
    skipped_series: int
    notes: str | None = None


def _require_polars() -> "pl":  # type: ignore[name-defined]
    try:
        import polars as pl  # type: ignore
    except Exception as exc:
        raise ForecastError("Polars is required for forecasting. Install with: pip install -e '.[data]'") from exc
    return pl


def _require_prophet():
    """
    Tries both 'prophet' and legacy 'fbprophet' import paths.
    """
    try:
        from prophet import Prophet  # type: ignore
        return Prophet
    except Exception:
        try:
            from fbprophet import Prophet  # type: ignore
            return Prophet
        except Exception as exc:
            raise ForecastError("Prophet is required. Install with: pip install -e '.[ml]'") from exc


def _validate_req(req: ForecastRequest) -> None:
    if not req.canonical_path.exists():
        raise ForecastError(f"Canonical file not found: {req.canonical_path}")

    if req.horizon < 1:
        raise ForecastError("Horizon must be >= 1.")

    if not req.out_dir:
        raise ForecastError("out_dir is required.")

    req.out_dir.mkdir(parents=True, exist_ok=True)

    if req.out_formats is not None:
        bad = [x for x in req.out_formats if x not in ("parquet", "csv")]
        if bad:
            raise ForecastError(f"Invalid out_formats: {bad}. Allowed: parquet, csv")


def _prophet_freq(freq: TimeFrequency) -> str:
    """
    Prophet expects a pandas date_range freq string.
    We keep it simple and boring.
    """
    if freq == TimeFrequency.DAILY:
        return "D"
    if freq == TimeFrequency.WEEKLY:
        return "W"
    if freq == TimeFrequency.MONTHLY:
        return "MS"  # month start (consistent spacing)
    if freq == TimeFrequency.QUARTERLY:
        return "QS"  # quarter start
    if freq == TimeFrequency.YEARLY:
        return "YS"  # year start
    # Fallback for IRREGULAR or unknown: daily
    return "D"


def _formats(req: ForecastRequest) -> tuple[str, ...]:
    return req.out_formats if req.out_formats is not None else ("parquet", "csv")


def _series_out_paths(out_dir: Path, cd_key: str, formats: tuple[str, ...]) -> list[Path]:
    safe = _sanitize_filename(cd_key)
    paths: list[Path] = []
    if "parquet" in formats:
        paths.append(out_dir / f"forecast__{safe}.parquet")
    if "csv" in formats:
        paths.append(out_dir / f"forecast__{safe}.csv")
    return paths


def _sanitize_filename(s: str) -> str:
    # windows-safe-ish
    return "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in s)[:180]


def forecast_prophet(req: ForecastRequest) -> ForecastResult:
    """
    Reads canonical long (cd_key, ds, y) and writes forecast outputs.

    ✅ Exports BOTH parquet and csv by default.
    - One file per series: forecast__<cd_key>.parquet / .csv

    Output schema per file:
      cd_key, ds, yhat, yhat_lower, yhat_upper
    """
    _validate_req(req)
    pl = _require_polars()
    Prophet = _require_prophet()
    freq = _prophet_freq(req.frequency)
    formats = _formats(req)

    try:
        lf = pl.scan_parquet(req.canonical_path).select([CANON.cd_key, CANON.ds, CANON.y])

        # Collect unique keys (small)
        keys = lf.select(pl.col(CANON.cd_key).unique()).collect()[CANON.cd_key].to_list()

        out_files: list[str] = []
        skipped = 0

        for k in keys:
            # Load this series only
            df_k = (
                lf.filter(pl.col(CANON.cd_key) == k)
                .select(
                    pl.col(CANON.ds).cast(pl.Date, strict=False).alias("ds"),
                    pl.col(CANON.y).cast(pl.Float64, strict=False).alias("y"),
                )
                .drop_nulls(["ds"])
                .sort("ds")
                .collect()
            )

            if df_k.height < req.min_points:
                skipped += 1
                continue

            # Prophet expects pandas with columns ds/y
            pdf = df_k.to_pandas(use_pyarrow_extension_array=True)  # type: ignore[arg-type]

            m = Prophet()
            m.fit(pdf)

            future = m.make_future_dataframe(periods=req.horizon, freq=freq, include_history=False)
            fc = m.predict(future)

            # Keep only minimal useful columns
            out_pl = pl.from_pandas(fc[["ds", "yhat", "yhat_lower", "yhat_upper"]])  # type: ignore[index]
            out_pl = out_pl.with_columns(pl.lit(k).alias(CANON.cd_key)).select(
                [CANON.cd_key, pl.col("ds").cast(pl.Date, strict=False).alias(CANON.ds), "yhat", "yhat_lower", "yhat_upper"]
            )

            # Write all requested formats
            for p in _series_out_paths(req.out_dir, k, formats):
                if p.suffix.lower() == ".parquet":
                    out_pl.write_parquet(p)
                elif p.suffix.lower() == ".csv":
                    out_pl.write_csv(p)
                out_files.append(str(p))

        notes = None
        if req.frequency == TimeFrequency.IRREGULAR:
            notes = "Input frequency was IRREGULAR; Prophet used daily cadence (freq='D') as a fallback."

        log.info(
            "forecast_ok",
            extra={
                "canonical_path": str(req.canonical_path),
                "out_dir": str(req.out_dir),
                "formats": list(formats),
                "series": len(keys),
                "written_files": len(out_files),
                "skipped": skipped,
                "horizon": req.horizon,
                "freq": freq,
            },
        )

        return ForecastResult(
            output_dir=req.out_dir,
            series_forecast_files=out_files,
            skipped_series=skipped,
            notes=notes,
        )

    except PyForecastError:
        raise
    except Exception as exc:
        log.exception("forecast_failed", extra={"error": str(exc)})
        raise ForecastError("Failed to run Prophet forecast.") from exc