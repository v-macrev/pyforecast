from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pyforecast.domain.errors import FileFormatError, SchemaInferenceError
from pyforecast.infrastructure.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class IngestedData:
    path: Path
    file_type: str  # "csv" | "xlsx"
    columns: list[str]
    preview_rows: list[dict[str, object]]
    row_count_estimate: int | None


class IngestService:

    def __init__(self, preview_n: int = 200) -> None:
        if preview_n <= 0:
            raise ValueError("preview_n must be > 0")
        self._preview_n = preview_n

    def ingest(self, path: Path) -> IngestedData:
        if not path.exists():
            raise FileFormatError(f"File not found: {path}")

        suffix = path.suffix.lower().lstrip(".")
        if suffix not in {"csv", "xlsx"}:
            raise FileFormatError(f"Unsupported file type: .{suffix} (supported: .csv, .xlsx)")

        if suffix == "csv":
            return self._ingest_csv(path)
        return self._ingest_xlsx(path)

    def _ingest_csv(self, path: Path) -> IngestedData:
        try:
            import polars as pl
        except Exception as exc:
            raise SchemaInferenceError(
                "Polars is not installed. Install with: pip install -e '.[data]'"
            ) from exc

        try:
            lf = pl.scan_csv(path, infer_schema_length=10_000, ignore_errors=True)
            cols = list(lf.columns)
            preview_df = lf.head(self._preview_n).collect()
            preview_rows = preview_df.to_dicts()
            row_est = None

            log.info("ingest_csv_ok", extra={"path": str(path), "cols": len(cols)})
            return IngestedData(
                path=path,
                file_type="csv",
                columns=cols,
                preview_rows=preview_rows,
                row_count_estimate=row_est,
            )
        except Exception as exc:
            log.exception("ingest_csv_failed", extra={"path": str(path), "error": str(exc)})
            raise FileFormatError(f"Failed to read CSV: {path.name}") from exc

    def _ingest_xlsx(self, path: Path) -> IngestedData:
        try:
            import polars as pl
        except Exception as exc:
            raise SchemaInferenceError(
                "Polars is not installed. Install with: pip install -e '.[data]'"
            ) from exc

        try:
            from python_calamine import CalamineWorkbook
        except Exception as exc:
            raise SchemaInferenceError(
                "python-calamine is not installed. Install with: pip install -e '.[excel]'"
            ) from exc

        try:
            wb = CalamineWorkbook.from_path(str(path))
            sheet_names = wb.sheet_names
            if not sheet_names:
                raise FileFormatError("Excel workbook has no sheets.")

            sheet = wb.get_sheet_by_name(sheet_names[0])
            rows = sheet.to_python()

            if not rows:
                raise FileFormatError("Excel sheet is empty.")

            headers = [str(x).strip() for x in rows[0]]
            data_rows = rows[1 : 1 + self._preview_n]

            preview_rows: list[dict[str, object]] = []
            for r in data_rows:
                rec: dict[str, object] = {}
                for i, h in enumerate(headers):
                    rec[h] = r[i] if i < len(r) else None
                preview_rows.append(rec)

            log.info(
                "ingest_xlsx_ok",
                extra={"path": str(path), "sheet": sheet_names[0], "cols": len(headers)},
            )
            return IngestedData(
                path=path,
                file_type="xlsx",
                columns=headers,
                preview_rows=preview_rows,
                row_count_estimate=None,
            )
        except FileFormatError:
            raise
        except Exception as exc:
            log.exception("ingest_xlsx_failed", extra={"path": str(path), "error": str(exc)})
            raise FileFormatError(f"Failed to read Excel: {path.name}") from exc