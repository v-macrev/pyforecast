from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pyforecast.domain.canonical_schema import CANON
from pyforecast.domain.errors import FileFormatError, TransformationError
from pyforecast.infrastructure.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class TransformRequest:
    path: Path
    file_type: str
    shape: str
    date_col: str
    value_col: str | None
    key_parts: list[str]
    key_separator: str = "|"
    out_dir: Path | None = None
    out_format: str = "parquet"


@dataclass(frozen=True)
class TransformResult:
    output_path: Path
    canonical_columns: list[str]
    notes: str | None = None


_DATE_FMTS_DATE = (
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%d-%m-%Y",
    "%m-%d-%Y",
    "%Y%m%d",
)

_DATE_FMTS_MONTH = (
    "%Y-%m",
    "%Y/%m",
    "%Y%m",
    "%b-%Y",
    "%b/%Y",
    "%Y-%b",
)


def _require_polars() -> "pl":  # type: ignore[name-defined]
    try:
        import polars as pl  # type: ignore
    except Exception as exc:
        raise TransformationError("Polars is required. Install with: pip install -e '.[data]'") from exc
    return pl


def _validate_input(req: TransformRequest) -> None:
    if not req.path.exists():
        raise FileFormatError(f"Input file not found: {req.path}")

    if req.shape not in {"long", "wide"}:
        raise TransformationError(f"Invalid shape '{req.shape}'. Expected 'long' or 'wide'.")

    if req.out_format not in {"parquet", "csv"}:
        raise TransformationError(f"Unsupported out_format '{req.out_format}' (parquet/csv).")

    if req.shape == "long" and not req.value_col:
        raise TransformationError("value_col is required when shape='long'.")

    if not req.key_parts:
        raise TransformationError("key_parts must not be empty.")


def _uniquify_headers(headers: list[str]) -> list[str]:
    """Ensure headers are non-empty and unique (Excel exports often contain blanks/duplicates)."""
    seen: dict[str, int] = {}
    out: list[str] = []
    for i, h in enumerate(headers):
        base = (h or "").strip()
        if not base:
            base = f"col_{i+1}"
        n = seen.get(base, 0) + 1
        seen[base] = n
        out.append(base if n == 1 else f"{base}__{n}")
    return out


def _normalise_cell(v: Any) -> str | None:
    """Normalise Excel cell values for stable schema construction."""
    if v is None:
        return None
    s = str(v).strip()
    if s == "" or s.lower() in {"null", "none", "nan", "n/a", "na", "-"}:
        return None
    return s


def _scan_input(pl: "pl", path: Path, file_type: str) -> "pl.LazyFrame":
    ft = file_type.lower()
    if ft == "csv":
        # keep this streaming-friendly; casting happens later in the pipeline
        return pl.scan_csv(path, infer_schema_length=10_000, ignore_errors=True)

    if ft == "xlsx":
        try:
            from python_calamine import CalamineWorkbook  # type: ignore
        except Exception as exc:
            raise FileFormatError("Excel support requires: pip install -e '.[excel]'") from exc

        wb = CalamineWorkbook.from_path(str(path))
        sheet_names = wb.sheet_names
        if not sheet_names:
            raise FileFormatError("Excel workbook has no sheets.")
        sheet = wb.get_sheet_by_name(sheet_names[0])
        rows = sheet.to_python()
        if not rows:
            raise FileFormatError("Excel sheet is empty.")

        raw_headers = [str(x).strip() for x in rows[0]]
        headers = _uniquify_headers(raw_headers)

        data_rows = rows[1:]
        width = len(headers)

        # Ensure all rows have same width and normalise values
        norm_rows: list[list[str | None]] = []
        for r in data_rows:
            rr = list(r)
            if len(rr) < width:
                rr = rr + [None] * (width - len(rr))
            elif len(rr) > width:
                rr = rr[:width]
            norm_rows.append([_normalise_cell(x) for x in rr])

        # Critical fix:
        # Force all columns to Utf8 so we never crash on mixed types (e.g., "null" in numeric column).
        schema_overrides = {h: pl.Utf8 for h in headers}

        df = pl.DataFrame(
            norm_rows,
            schema=headers,
            orient="row",
            schema_overrides=schema_overrides,
        )
        return df.lazy()

    raise FileFormatError(f"Unsupported file_type '{file_type}' (expected csv/xlsx).")


def _collect_columns_fast(lf: "pl.LazyFrame") -> list[str]:
    return lf.collect_schema().names()


def _build_cd_key_expr(pl: "pl", key_parts: list[str], sep: str) -> "pl.Expr":
    return pl.concat_str(
        [pl.col(c).cast(pl.Utf8, strict=False).fill_null("").str.strip_chars() for c in key_parts],
        separator=sep,
    ).alias(CANON.cd_key)


def _strptime_compat(expr_utf8: "pl.Expr", dtype: object, fmt: str) -> "pl.Expr":
    return expr_utf8.str.strptime(dtype, fmt, strict=False)


def _parse_ds_expr(pl: "pl", col_name: str) -> "pl.Expr":
    c = pl.col(col_name)

    # Already a Date? Keep it.
    cast_date = c.cast(pl.Date, strict=False)

    s = c.cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()

    # Full dates → Date
    date_tries = [_strptime_compat(s, pl.Date, fmt) for fmt in _DATE_FMTS_DATE]

    # Month headers → Datetime -> Date (day=1)
    month_dt_tries = [_strptime_compat(s, pl.Datetime, fmt) for fmt in _DATE_FMTS_MONTH]
    month_date_tries = [dt.dt.date() for dt in month_dt_tries]

    # Excel serial dates (best-effort): days since 1899-12-30
    serial = c.cast(pl.Int64, strict=False)
    excel_date = pl.when(serial.is_between(20_000, 80_000)).then(
        pl.date(1899, 12, 30) + pl.duration(days=serial)
    ).otherwise(None)

    return pl.coalesce([cast_date, *date_tries, *month_date_tries, excel_date]).alias(CANON.ds)


def _parse_y_expr(pl: "pl", col_name: str) -> "pl.Expr":
    """
    Optional improvement:
    Centralize numeric parsing so both CSV and XLSX mixed-types become Float64 safely.
    """
    c = pl.col(col_name)
    # Try direct numeric cast first
    direct = c.cast(pl.Float64, strict=False)
    # If it is a string with commas etc., attempt basic normalization
    s = c.cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
    norm = (
        s.str.replace_all(r"\s+", "")
        .str.replace_all(".", "", literal=True)  # remove thousand separators like 1.234,56 (pt-BR)
        .str.replace_all(",", ".", literal=True)  # decimal comma -> dot
        .cast(pl.Float64, strict=False)
    )
    return pl.coalesce([direct, norm]).alias(CANON.y)


def _default_output_path(req: TransformRequest) -> Path:
    out_dir = req.out_dir or (Path.home() / ".pyforecast" / "outputs")
    out_dir.mkdir(parents=True, exist_ok=True)
    suffix = ".parquet" if req.out_format == "parquet" else ".csv"
    return out_dir / f"{req.path.stem}__canonical_long{suffix}"


def transform_to_canonical_long(req: TransformRequest) -> TransformResult:
    pl = _require_polars()
    _validate_input(req)

    lf = _scan_input(pl, req.path, req.file_type)
    out_path = _default_output_path(req)

    try:
        cols = _collect_columns_fast(lf)

        if req.shape == "long":
            if req.date_col not in cols:
                raise TransformationError(f"Date column '{req.date_col}' not found.")
            if req.value_col is None or req.value_col not in cols:
                raise TransformationError(f"Value column '{req.value_col}' not found.")

            lf2 = (
                lf.with_columns(_build_cd_key_expr(pl, req.key_parts, req.key_separator))
                .with_columns(_parse_ds_expr(pl, req.date_col))
                .with_columns(_parse_y_expr(pl, req.value_col))
                .select([CANON.cd_key, CANON.ds, CANON.y])
                .drop_nulls([CANON.ds])
            )
            notes = None

        else:
            key_set = set(req.key_parts)
            period_cols = [c for c in cols if c not in key_set]

            if req.date_col in period_cols and req.date_col not in key_set:
                period_cols = [c for c in period_cols if c != req.date_col]

            if not period_cols:
                raise TransformationError(
                    "Wide transform requires at least one date-like period column besides key columns."
                )

            unpivoted = lf.unpivot(
                index=req.key_parts,
                on=period_cols,
                variable_name=CANON.ds,
                value_name=CANON.y,
            )

            lf2 = (
                unpivoted.with_columns(_build_cd_key_expr(pl, req.key_parts, req.key_separator))
                .with_columns(_parse_ds_expr(pl, CANON.ds))  # parse ds from header strings
                .with_columns(_parse_y_expr(pl, CANON.y))
                .select([CANON.cd_key, CANON.ds, CANON.y])
                .drop_nulls([CANON.ds])
            )
            notes = "Wide transform: ds inferred from column headers (supports YYYYMMDD and YYYY-MM)."

        if req.out_format == "parquet":
            lf2.sink_parquet(out_path)
        else:
            lf2.sink_csv(out_path)

        log.info(
            "transform_ok",
            extra={
                "in_path": str(req.path),
                "shape": req.shape,
                "out_path": str(out_path),
                "out_format": req.out_format,
            },
        )
        return TransformResult(
            output_path=out_path,
            canonical_columns=[CANON.cd_key, CANON.ds, CANON.y],
            notes=notes,
        )

    except Exception as exc:
        log.exception("transform_failed", extra={"in_path": str(req.path), "error": str(exc)})
        raise TransformationError(
            f"Failed to transform '{req.path.name}' to canonical long format. (input={req.path})"
        ) from exc