from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pyforecast.domain.canonical_schema import CANON
from pyforecast.domain.errors import FileFormatError, TransformationError
from pyforecast.infrastructure.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class TransformRequest:
    path: Path
    file_type: str  # "csv" | "xlsx"
    shape: str  # "long" | "wide"
    date_col: str
    value_col: str | None
    key_parts: list[str]
    key_separator: str = "|"
    out_dir: Path | None = None
    out_format: str = "parquet"  # "parquet" | "csv"


@dataclass(frozen=True)
class TransformResult:
    output_path: Path
    canonical_columns: list[str]
    notes: str | None = None


_DATE_FMTS = (
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%d-%m-%Y",
    "%m-%d-%Y",
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

    if not req.key_parts:
        raise TransformationError("No key_parts provided for cd_key.")

    if req.shape == "long" and not req.value_col:
        raise TransformationError("value_col is required when shape='long'.")

    if req.out_format not in {"parquet", "csv"}:
        raise TransformationError(f"Unsupported out_format '{req.out_format}' (parquet/csv).")


def _scan_input(pl: "pl", path: Path, file_type: str) -> "pl.LazyFrame":
    ft = file_type.lower()
    if ft == "csv":
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

        headers = [str(x).strip() for x in rows[0]]
        data_rows = rows[1:]
        df = pl.DataFrame(data_rows, schema=headers, orient="row")
        return df.lazy()

    raise FileFormatError(f"Unsupported file_type '{file_type}' (expected csv/xlsx).")


def _build_cd_key_expr(pl: "pl", key_parts: list[str], sep: str) -> "pl.Expr":
    return pl.concat_str(
        [pl.col(c).cast(pl.Utf8, strict=False).fill_null("").str.strip_chars() for c in key_parts],
        separator=sep,
    ).alias(CANON.cd_key)


def _parse_ds_expr(pl: "pl", col_name: str) -> "pl.Expr":
    c = pl.col(col_name)
    cast_try = c.cast(pl.Date, strict=False)
    str_try = pl.coalesce(
        [c.cast(pl.Utf8, strict=False).str.strptime(pl.Date, format=fmt, strict=False) for fmt in _DATE_FMTS]
    )
    return pl.coalesce([cast_try, str_try]).alias(CANON.ds)


def _cast_y_expr(pl: "pl", col_name: str) -> "pl.Expr":
    return pl.col(col_name).cast(pl.Float64, strict=False).alias(CANON.y)


def _default_output_path(req: TransformRequest) -> Path:
    out_dir = req.out_dir or (Path.home() / ".pyforecast" / "outputs")
    out_dir.mkdir(parents=True, exist_ok=True)

    stem = req.path.stem
    suffix = ".parquet" if req.out_format == "parquet" else ".csv"
    return out_dir / f"{stem}__canonical_long{suffix}"


def transform_to_canonical_long(req: TransformRequest) -> TransformResult:
    pl = _require_polars()
    _validate_input(req)

    lf = _scan_input(pl, req.path, req.file_type)
    out_path = _default_output_path(req)

    try:
        if req.shape == "long":
            lf2 = (
                lf.with_columns(_build_cd_key_expr(pl, req.key_parts, req.key_separator))
                .with_columns(_parse_ds_expr(pl, req.date_col))
                .with_columns(_cast_y_expr(pl, req.value_col or ""))
                .select([CANON.cd_key, CANON.ds, CANON.y])
                .drop_nulls([CANON.ds])
            )
            notes = None

        else:  # wide
            cols = lf.columns
            key_set = set(req.key_parts)
            period_cols = [c for c in cols if c not in key_set]
            if not period_cols:
                raise TransformationError("No period columns detected for wide-to-long transform.")

            melted = lf.melt(
                id_vars=req.key_parts,
                value_vars=period_cols,
                variable_name=CANON.ds,
                value_name=CANON.y,
            )

            lf2 = (
                melted.with_columns(_build_cd_key_expr(pl, req.key_parts, req.key_separator))
                .with_columns(_parse_ds_expr(pl, CANON.ds))
                .with_columns(pl.col(CANON.y).cast(pl.Float64, strict=False).alias(CANON.y))
                .select([CANON.cd_key, CANON.ds, CANON.y])
                .drop_nulls([CANON.ds])
            )
            notes = "Wide transform: ds inferred from column headers (best-effort date parsing)."

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
        return TransformResult(output_path=out_path, canonical_columns=[CANON.cd_key, CANON.ds, CANON.y], notes=notes)

    except Exception as exc:
        log.exception("transform_failed", extra={"in_path": str(req.path), "error": str(exc)})
        raise TransformationError(
            f"Failed to transform '{req.path.name}' to canonical long format. "
            f"(input={req.path})"
        ) from exc