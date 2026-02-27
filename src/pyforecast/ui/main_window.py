from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QFileDialog,
    QLabel,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QProgressBar,
    QScrollArea,
    QStatusBar,
    QVBoxLayout,
    QWidget,
)

from pyforecast.application.services import (
    ForecastRequest,
    IngestService,
    IngestedData,
    ProfilingService,
    TransformRequest,
)
from pyforecast.application.services.config_service import AppConfig, ConfigService
from pyforecast.domain.canonical_schema import CANON
from pyforecast.domain.timefreq import TimeFrequency
from pyforecast.infrastructure.logging import get_logger
from pyforecast.ui.workers import ThreadHandle, start_forecast_thread, start_transform_thread
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

        self._cfg_svc = ConfigService(base_dir=base_dir)
        self._cfg = self._cfg_svc.load()

        self._paths = AppPaths(
            base_dir=base_dir,
            outputs_dir=self._cfg_svc.resolve_output_dir(self._cfg),
            logs_dir=self._cfg_svc.logs_dir,
        )
        self._cfg_svc.ensure_dirs(self._paths.outputs_dir)

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

        # Output folder controls
        self._lbl_output = QLabel("")
        self._lbl_output.setWordWrap(True)
        self._lbl_output.setStyleSheet("padding: 8px; border: 1px solid #eee; border-radius: 8px;")

        self._btn_change_output = QPushButton("Change output folder")
        self._btn_change_output.clicked.connect(self._choose_output_dir)

        self._btn_open_output = QPushButton("Open output folder")
        self._btn_open_output.clicked.connect(self._open_output_dir)

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

        self._progress = QProgressBar(self)
        self._progress.setRange(0, 100)
        self._progress.setValue(0)
        self._progress.setVisible(False)

        self._progress_label = QLabel("")
        self._progress_label.setWordWrap(True)
        self._progress_label.setVisible(False)
        self._progress_label.setStyleSheet("color: #444;")

        self._btn_cancel = QPushButton("Cancel current task")
        self._btn_cancel.setEnabled(False)
        self._btn_cancel.setVisible(False)
        self._btn_cancel.clicked.connect(self._cancel_current_task)

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

        layout.addWidget(QLabel("<b>Output location</b>"))
        layout.addWidget(self._lbl_output)
        layout.addWidget(self._btn_change_output)
        layout.addWidget(self._btn_open_output)

        layout.addSpacing(8)
        layout.addWidget(self._file_picker)
        layout.addWidget(self._ingest_info)
        layout.addWidget(self._profile_info)
        layout.addWidget(self._column_mapper)
        layout.addWidget(self._key_builder)

        layout.addWidget(self._progress)
        layout.addWidget(self._progress_label)
        layout.addWidget(self._btn_cancel)

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
        self.setStatusBar(sb)
        self._refresh_output_label()

        self._last_ingested: IngestedData | None = None
        self._last_mapping: ColumnMapping | None = None
        self._last_key_sel: KeySelection | None = None
        self._last_transform_path: Path | None = None
        self._last_profile_freq: TimeFrequency | None = None
        self._last_history_points: int | None = None
        self._last_forecast_cfg: ForecastConfig = ForecastConfig(enabled=False, horizon=12)

        self._current_task: ThreadHandle | None = None
        self._current_task_kind: str | None = None

        self._last_forecast_out_dir: Path | None = None

    def set_output_dir(self, output_dir: Path) -> None:
        output_dir = Path(output_dir)
        self._paths = AppPaths(self._paths.base_dir, output_dir, self._paths.logs_dir)
        self._cfg = AppConfig(output_dir=output_dir)
        self._cfg_svc.save(self._cfg)
        self._cfg_svc.ensure_dirs(output_dir)
        self._refresh_output_label()

    def _refresh_output_label(self) -> None:
        self._lbl_output.setText(str(self._paths.outputs_dir))
        self.statusBar().showMessage(f"Outputs: {self._paths.outputs_dir}")

    def _choose_output_dir(self) -> None:
        if self._current_task is not None and self._current_task.is_running():
            QMessageBox.information(self, "Busy", "Wait for the current task to finish or cancel it first.")
            return

        start_dir = str(self._paths.outputs_dir)
        chosen = QFileDialog.getExistingDirectory(self, "Select Output Folder", start_dir)
        if not chosen:
            return

        out = Path(chosen)
        try:
            out.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            QMessageBox.warning(self, "Invalid folder", f"Could not use this folder:\n{exc}")
            return

        self.set_output_dir(out)

    def _open_output_dir(self) -> None:
        path = self._paths.outputs_dir
        try:
            if os.name == "nt":
                os.startfile(str(path))  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.run(["open", str(path)], check=False)
            else:
                subprocess.run(["xdg-open", str(path)], check=False)
        except Exception as exc:
            QMessageBox.warning(self, "Cannot open folder", str(exc))

    def _set_busy(self, kind: str, msg: str) -> None:
        self._current_task_kind = kind
        self._progress.setVisible(True)
        self._progress_label.setVisible(True)
        self._btn_cancel.setVisible(True)
        self._btn_cancel.setEnabled(True)

        self._progress.setValue(0)
        self._progress_label.setText(msg)

        self._file_picker.setEnabled(False)
        self._btn_transform.setEnabled(False)
        self._btn_forecast.setEnabled(False)
        self._btn_change_output.setEnabled(False)

    def _clear_busy(self) -> None:
        self._current_task_kind = None
        self._current_task = None

        self._progress.setVisible(False)
        self._progress_label.setVisible(False)
        self._btn_cancel.setVisible(False)
        self._btn_cancel.setEnabled(False)

        self._file_picker.setEnabled(True)
        self._btn_change_output.setEnabled(True)
        self._refresh_actions()

    def _cancel_current_task(self) -> None:
        if self._current_task is None or not self._current_task.is_running():
            return
        self._btn_cancel.setEnabled(False)
        self._progress_label.setText("Cancellation requested…")
        self._current_task.cancel()

    def _on_task_progress(self, pct: int, msg: str) -> None:
        self._progress.setValue(pct)
        self._progress_label.setText(msg)

    def _on_ingested(self, data: IngestedData) -> None:
        self._last_ingested = data
        self._last_mapping = None
        self._last_key_sel = None
        self._last_transform_path = None
        self._last_profile_freq = None
        self._last_history_points = None
        self._last_forecast_out_dir = None

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

        self._forecast_prompt.set_context(frequency=self._last_profile_freq, n_points=None)
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
        if self._current_task is not None and self._current_task.is_running():
            return

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

        self._set_busy("transform", "Starting transform…")

        handle = start_transform_thread(req)
        self._current_task = handle

        handle.runner.progress.connect(self._on_task_progress)
        handle.runner.failed.connect(self._on_transform_failed)
        handle.runner.cancelled.connect(self._on_transform_cancelled)
        handle.runner.finished.connect(self._on_transform_finished)

    def _on_transform_failed(self, message: str) -> None:
        self._transform_info.setText("Transform: failed")
        self._clear_busy()
        QMessageBox.warning(self, "Transform failed", message)

    def _on_transform_cancelled(self) -> None:
        self._transform_info.setText("Transform: cancelled")
        self._clear_busy()
        QMessageBox.information(self, "Cancelled", "Transform was cancelled.")

    def _on_transform_finished(self, res_obj: object) -> None:
        if not hasattr(res_obj, "output_path"):
            self._on_transform_failed("Invalid transform result.")
            return

        out_path = Path(getattr(res_obj, "output_path"))
        notes = getattr(res_obj, "notes", None)
        canonical_cols = getattr(res_obj, "canonical_columns", [CANON.cd_key, CANON.ds, CANON.y])

        self._last_transform_path = out_path
        self._last_forecast_out_dir = None

        try:
            import polars as pl
        except Exception as exc:
            self._clear_busy()
            QMessageBox.warning(self, "Preview unavailable", f"Polars not available for preview: {exc}")
            return

        df_prev = pl.read_parquet(self._last_transform_path).head(200)
        self._preview_title.setText("<b>Preview</b> (canonical output: cd_key, ds, y)")
        self._preview.set_preview_rows(df_prev.to_dicts())

        n_dates = (
            pl.scan_parquet(self._last_transform_path)
            .select(pl.col(CANON.ds).n_unique().alias("n"))
            .collect()["n"][0]
        )
        self._last_history_points = int(n_dates)
        self._forecast_prompt.set_context(frequency=self._last_profile_freq, n_points=self._last_history_points)

        freq_txt = self._last_profile_freq.name if self._last_profile_freq else "N/A"
        self._transform_info.setText(
            f"<b>Transform OK</b><br>"
            f"Output: {self._last_transform_path}<br>"
            f"Columns: {', '.join(canonical_cols)}<br>"
            f"History points (unique ds): {self._last_history_points}<br>"
            f"Frequency: {freq_txt}<br>"
            f"{('<i>Note:</i> ' + notes) if notes else ''}"
        )

        self._clear_busy()

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
            # exports both csv & parquet by default in forecast_service
            out_formats=None,
        )

        self._set_busy("forecast", "Starting forecast…")

        handle = start_forecast_thread(req)
        self._current_task = handle

        handle.runner.progress.connect(self._on_task_progress)
        handle.runner.failed.connect(self._on_forecast_failed)
        handle.runner.cancelled.connect(self._on_forecast_cancelled)
        handle.runner.finished.connect(self._on_forecast_finished)

    def _on_forecast_failed(self, message: str) -> None:
        self._forecast_info.setText("Forecast: failed")
        self._clear_busy()
        QMessageBox.warning(self, "Forecast failed", message)

    def _on_forecast_cancelled(self) -> None:
        self._forecast_info.setText("Forecast: cancelled")
        self._clear_busy()
        QMessageBox.information(self, "Cancelled", "Forecast was cancelled.")

    def _on_forecast_finished(self, res_obj: object) -> None:
        if not hasattr(res_obj, "output_dir"):
            self._on_forecast_failed("Invalid forecast result.")
            return

        out_dir = Path(getattr(res_obj, "output_dir"))
        series_files = list(getattr(res_obj, "series_forecast_files", []) or [])
        skipped = int(getattr(res_obj, "skipped_series", 0))
        notes = getattr(res_obj, "notes", None)

        self._last_forecast_out_dir = out_dir

        # Preview: prefer CSV (user-friendly), fallback to parquet.
        preview_loaded = False
        preview_err: str | None = None

        if series_files:
            csv_first = next((Path(p) for p in series_files if str(p).lower().endswith(".csv")), None)
            pq_first = next((Path(p) for p in series_files if str(p).lower().endswith(".parquet")), None)
            chosen = csv_first or pq_first

            if chosen is not None:
                try:
                    import polars as pl

                    if chosen.suffix.lower() == ".parquet":
                        df = pl.read_parquet(chosen).head(200)
                    else:
                        df = pl.read_csv(chosen).head(200)

                    self._preview_title.setText(f"<b>Preview</b> (forecast output: {chosen.name})")
                    self._preview.set_preview_rows(df.to_dicts())
                    preview_loaded = True
                except Exception as exc:
                    preview_err = str(exc)

        self._forecast_info.setText(
            f"<b>Forecast OK</b><br>"
            f"Output dir: {out_dir}<br>"
            f"Series files: {len(series_files)} (CSV + Parquet)<br>"
            f"Skipped series: {skipped}<br>"
            f"{('<i>Note:</i> ' + notes) if notes else ''}"
            f"{'<br><i>Preview:</i> loaded' if preview_loaded else ''}"
            f"{('<br><i>Preview error:</i> ' + preview_err) if (not preview_loaded and preview_err) else ''}"
        )

        log.info(
            "forecast_ui_ok",
            extra={"out_dir": str(out_dir), "written": len(series_files), "skipped": skipped},
        )
        self._clear_busy()