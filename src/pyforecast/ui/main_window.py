from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QLabel,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QStatusBar,
    QVBoxLayout,
    QWidget,
)

from pyforecast.application.services import (
    ForecastRequest,
    forecast_prophet,
    IngestService,
    IngestedData,
    ProfilingService,
    TransformRequest,
    transform_to_canonical_long,
)
from pyforecast.domain.canonical_schema import CANON
from pyforecast.domain.errors import PyForecastError
from pyforecast.domain.timefreq import TimeFrequency
from pyforecast.infrastructure.logging import get_logger
from pyforecast.ui.widgets import (
    ColumnMapper,
    ColumnMapping,
    FilePickerButton,
    ForecastConfig,
    ForecastPrompt,
    KeyBuilder,
    KeySelection,
    PreviewTable,
)

log = get_logger(__name__)


@dataclass(frozen=True)
class AppPaths:
    base_dir: Path
    outputs_dir: Path
    logs_dir: Path


class MainWindow(QMainWindow):
    def __init__(self, base_dir: Path) -> None:
        super().__init__()
        self._paths = AppPaths(
            base_dir=base_dir,
            outputs_dir=base_dir / "outputs",
            logs_dir=base_dir / "logs",
        )

        self.setWindowTitle("PyForecast")
        self.setMinimumSize(980, 720)

        scroll = QScrollArea(self)
        scroll.setWidgetResizable(True)
        self.setCentralWidget(scroll)

        root = QWidget(scroll)
        scroll.setWidget(root)

        layout = QVBoxLayout(root)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(12)

        title = QLabel("PyForecast")
        title.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        title.setStyleSheet("font-size: 22px; font-weight: 600;")

        subtitle = QLabel(
            "Import Excel/CSV → detect shape & frequency → build cd_key → normalize to long → forecast with Prophet."
        )
        subtitle.setWordWrap(True)
        subtitle.setStyleSheet("color: #666;")

        self._ingest_info = QLabel("No file loaded.")
        self._ingest_info.setWordWrap(True)
        self._ingest_info.setStyleSheet("padding: 10px; border: 1px solid #ddd; border-radius: 8px;")

        self._profile_info = QLabel("Profile: (not available)")
        self._profile_info.setWordWrap(True)
        self._profile_info.setStyleSheet("padding: 10px; border: 1px solid #eee; border-radius: 8px;")

        self._transform_info = QLabel("Transform: (not run)")
        self._transform_info.setWordWrap(True)
        self._transform_info.setStyleSheet("padding: 10px; border: 1px solid #eee; border-radius: 8px;")

        self._forecast_info = QLabel("Forecast: (not run)")
        self._forecast_info.setWordWrap(True)
        self._forecast_info.setStyleSheet("padding: 10px; border: 1px solid #eee; border-radius: 8px;")

        self._forecast_prompt = ForecastPrompt(self)
        self._forecast_prompt.config_changed.connect(self._on_forecast_cfg_changed)

        self._preview_title = QLabel("<b>Preview</b>")
        self._preview = PreviewTable(self)
        self._preview.setMinimumHeight(240)

        self._column_mapper = ColumnMapper(self)
        self._column_mapper.mapping_changed.connect(self._on_mapping_changed)

        self._key_builder = KeyBuilder(self)
        self._key_builder.selection_changed.connect(self._on_key_selection_changed)

        self._ingest_service = IngestService(preview_n=200)
        self._profiling_service = ProfilingService(sample_limit=200)

        self._file_picker = FilePickerButton(parent=self, ingest=self._ingest_service)
        self._file_picker.ingested.connect(self._on_ingested)

        self._btn_transform = QPushButton("Transform to canonical long")
        self._btn_transform.setEnabled(False)
        self._btn_transform.clicked.connect(self._run_transform)

        self._btn_forecast = QPushButton("Run forecast (Prophet)")
        self._btn_forecast.setEnabled(False)
        self._btn_forecast.clicked.connect(self._run_forecast)

        self._btn_quit = QPushButton("Quit")
        self._btn_quit.clicked.connect(self.close)

        layout.addWidget(title)
        layout.addWidget(subtitle)
        layout.addSpacing(8)
        layout.addWidget(self._file_picker)
        layout.addWidget(self._ingest_info)
        layout.addWidget(self._profile_info)
        layout.addWidget(self._column_mapper)
        layout.addWidget(self._key_builder)
        layout.addWidget(self._btn_transform)
        layout.addWidget(self._transform_info)
        layout.addWidget(self._forecast_prompt)
        layout.addWidget(self._btn_forecast)
        layout.addWidget(self._forecast_info)
        layout.addWidget(self._preview_title)
        layout.addWidget(self._preview)
        layout.addWidget(self._btn_quit)
        layout.addStretch(1)

        sb = QStatusBar(self)
        sb.showMessage(f"Outputs: {self._paths.outputs_dir}")
        self.setStatusBar(sb)

        self._last_ingested: IngestedData | None = None
        self._last_mapping: ColumnMapping | None = None
        self._last_key_sel: KeySelection | None = None
        self._last_transform_path: Path | None = None
        self._last_profile_freq: TimeFrequency | None = None
        self._last_history_points: int | None = None
        self._last_forecast_cfg: ForecastConfig = ForecastConfig(enabled=False, horizon=12)

    def _on_ingested(self, data: IngestedData) -> None:
        self._last_ingested = data
        self._last_mapping = None
        self._last_key_sel = None
        self._last_transform_path = None
        self._last_profile_freq = None
        self._last_history_points = None

        self._btn_transform.setEnabled(False)
        self._btn_forecast.setEnabled(False)

        self._transform_info.setText("Transform: (not run)")
        self._forecast_info.setText("Forecast: (not run)")
        self._forecast_prompt.set_context(frequency=None, n_points=None)

        cols_preview = ", ".join(data.columns[:12]) + (" ..." if len(data.columns) > 12 else "")
        self._ingest_info.setText(
            f"<b>Loaded:</b> {data.path.name}<br>"
            f"<b>Type:</b> {data.file_type.upper()}<br>"
            f"<b>Columns:</b> {len(data.columns)}<br>"
            f"<b>Preview:</b> {len(data.preview_rows)} rows<br>"
            f"<b>First columns:</b> {cols_preview}"
        )

        self._preview_title.setText("<b>Preview</b> (raw input)")
        self._preview.set_preview_rows(data.preview_rows)

        profile = self._profiling_service.profile(data.columns, data.preview_rows)
        self._last_profile_freq = profile.frequency.frequency if profile.frequency else None

        if profile.frequency:
            freq_txt = (
                f"{profile.frequency.frequency.name} "
                f"(conf={profile.frequency.confidence:.2f}, medΔ={profile.frequency.median_delta_days})"
            )
        else:
            freq_txt = "N/A"

        cand_txt = ", ".join(profile.date_candidates[:5]) if profile.date_candidates else "None"
        self._profile_info.setText(
            f"<b>Profile</b><br>"
            f"<b>Shape:</b> {profile.shape}<br>"
            f"<b>Date candidates:</b> {cand_txt}<br>"
            f"<b>Selected date:</b> {profile.inferred_date_column or 'None'}<br>"
            f"<b>Frequency:</b> {freq_txt}<br>"
            f"{('<i>Note:</i> ' + profile.notes) if profile.notes else ''}"
        )

        self._column_mapper.set_context(columns=data.columns, profile=profile)
        self._key_builder.set_context(columns=data.columns, preview_rows=data.preview_rows)

        # Enable forecast prompt immediately if we have frequency (even before transform).
        # Horizon recommendation will become smarter after transform (when we know n_points).
        self._forecast_prompt.set_context(
            frequency=self._last_profile_freq,
            n_points=None,
        )

        self._refresh_actions()

    def _on_mapping_changed(self, mapping: ColumnMapping) -> None:
        self._last_mapping = mapping
        self._refresh_actions()

    def _on_key_selection_changed(self, sel: KeySelection) -> None:
        self._last_key_sel = sel
        self._refresh_actions()

    def _on_forecast_cfg_changed(self, cfg: ForecastConfig) -> None:
        self._last_forecast_cfg = cfg
        self._refresh_actions()

    def _refresh_actions(self) -> None:
        ready_to_transform = (
            self._last_ingested is not None and self._last_mapping is not None and self._last_key_sel is not None
        )
        self._btn_transform.setEnabled(ready_to_transform)

        ready_to_forecast = (
            self._last_transform_path is not None
            and self._last_profile_freq is not None
            and self._last_forecast_cfg.enabled
            and self._last_forecast_cfg.horizon >= 1
        )
        self._btn_forecast.setEnabled(ready_to_forecast)

    def _load_canonical_preview(self, parquet_path: Path, n: int = 200) -> list[dict[str, object]]:
        try:
            import polars as pl
        except Exception as exc:
            raise PyForecastError(
                "Polars is required to preview canonical output. Install with: pip install -e '.[data]'"
            ) from exc

        df = pl.read_parquet(parquet_path).head(n)
        return df.to_dicts()

    def _count_unique_dates(self, parquet_path: Path) -> int:
        try:
            import polars as pl
        except Exception as exc:
            raise PyForecastError(
                "Polars is required to analyse canonical output. Install with: pip install -e '.[data]'"
            ) from exc

        df = (
            pl.scan_parquet(parquet_path)
            .select(pl.col(CANON.ds).n_unique().alias("n_dates"))
            .collect()
        )
        return int(df["n_dates"][0])

    def _run_transform(self) -> None:
        if self._last_ingested is None or self._last_mapping is None or self._last_key_sel is None:
            QMessageBox.information(self, "Missing step", "Load a file, map columns, and select key columns first.")
            return

        req = TransformRequest(
            path=self._last_ingested.path,
            file_type=self._last_ingested.file_type,
            shape=self._last_mapping.shape,
            date_col=self._last_mapping.date_col,
            value_col=self._last_mapping.value_col,
            key_parts=self._last_key_sel.key_parts,
            key_separator=self._last_key_sel.separator,
            out_dir=self._paths.outputs_dir,
            out_format="parquet",
        )

        try:
            self._transform_info.setText("Transform: running…")
            self.repaint()

            res = transform_to_canonical_long(req)
            self._last_transform_path = res.output_path

            canonical_rows = self._load_canonical_preview(res.output_path, n=200)
            self._preview_title.setText("<b>Preview</b> (canonical output: cd_key, ds, y)")
            self._preview.set_preview_rows(canonical_rows)

            # Now we can drive the smart horizon recommendation from real history depth
            self._last_history_points = self._count_unique_dates(res.output_path)
            self._forecast_prompt.set_context(
                frequency=self._last_profile_freq,
                n_points=self._last_history_points,
            )

            freq_txt = self._last_profile_freq.name if self._last_profile_freq else "N/A"
            self._transform_info.setText(
                f"<b>Transform OK</b><br>"
                f"Output: {res.output_path}<br>"
                f"Columns: {', '.join(res.canonical_columns)}<br>"
                f"History points (unique ds): {self._last_history_points}<br>"
                f"Frequency: {freq_txt}<br>"
                f"{('<i>Note:</i> ' + res.notes) if res.notes else ''}"
            )

            self._refresh_actions()
        except PyForecastError as exc:
            self._transform_info.setText("Transform: failed")
            QMessageBox.warning(self, "Transform failed", str(exc))
        except Exception as exc:
            self._transform_info.setText("Transform: failed")
            QMessageBox.critical(self, "Unexpected error", str(exc))

    def _run_forecast(self) -> None:
        if self._last_transform_path is None:
            QMessageBox.information(self, "Not ready", "Run transform first.")
            return
        if self._last_profile_freq is None:
            QMessageBox.information(self, "Not ready", "Frequency is unknown; cannot configure Prophet cadence.")
            return
        if not self._last_forecast_cfg.enabled:
            QMessageBox.information(self, "Not ready", "Enable forecasting first.")
            return

        req = ForecastRequest(
            canonical_path=self._last_transform_path,
            frequency=self._last_profile_freq,
            horizon=self._last_forecast_cfg.horizon,
            out_dir=self._paths.outputs_dir,
        )

        try:
            self._forecast_info.setText("Forecast: running…")
            self.repaint()

            res = forecast_prophet(req)
            self._forecast_info.setText(
                f"<b>Forecast OK</b><br>"
                f"Output dir: {res.output_dir}<br>"
                f"Series files: {len(res.series_forecast_files)}<br>"
                f"Skipped series: {res.skipped_series}<br>"
                f"{('<i>Note:</i> ' + res.notes) if res.notes else ''}"
            )
        except PyForecastError as exc:
            self._forecast_info.setText("Forecast: failed")
            QMessageBox.warning(self, "Forecast failed", str(exc))
        except Exception as exc:
            self._forecast_info.setText("Forecast: failed")
            QMessageBox.critical(self, "Unexpected error", str(exc))