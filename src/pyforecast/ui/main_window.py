# main_window.py
from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QProgressBar,
    QScrollArea,
    QSplitter,
    QStatusBar,
    QTabWidget,
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
        self.setMinimumSize(1180, 760)

        # -----------------------------
        # Services / state
        # -----------------------------
        self._ingest_service = IngestService(preview_n=200)
        self._profiling_service = ProfilingService(sample_limit=200)

        self._last_ingested: IngestedData | None = None
        self._last_mapping: ColumnMapping | None = None
        self._last_key_sel: KeySelection | None = None
        self._last_transform_path: Path | None = None
        self._last_profile_freq: TimeFrequency | None = None
        self._last_history_points: int | None = None
        self._last_forecast_cfg: ForecastConfig = ForecastConfig(enabled=False, horizon=12)
        self._last_forecast_out_dir: Path | None = None

        self._current_task: ThreadHandle | None = None
        self._current_task_kind: str | None = None

        # -----------------------------
        # Root container
        # -----------------------------
        root = QWidget(self)
        root_layout = QVBoxLayout(root)
        root_layout.setContentsMargins(12, 12, 12, 12)
        root_layout.setSpacing(10)
        self.setCentralWidget(root)

        # -----------------------------
        # Header bar (top)
        # -----------------------------
        header = QWidget(root)
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(0, 0, 0, 0)
        header_layout.setSpacing(10)

        self._lbl_project = QLabel(f"Project: <span style='color:#aaa;'>{self._paths.base_dir.name}</span>")
        self._lbl_project.setTextFormat(Qt.RichText)
        self._lbl_project.setStyleSheet("color: #bbb;")
        header_layout.addWidget(self._lbl_project)

        header_layout.addStretch(1)

        self._lbl_output = QLabel("")
        self._lbl_output.setTextFormat(Qt.PlainText)
        self._lbl_output.setStyleSheet(
            "padding: 6px 10px; border: 1px solid #333; border-radius: 8px; color: #ddd;"
        )
        header_layout.addWidget(self._lbl_output)

        self._btn_change_output = QPushButton("Change")
        self._btn_change_output.clicked.connect(self._choose_output_dir)
        header_layout.addWidget(self._btn_change_output)

        self._btn_open_output = QPushButton("Open")
        self._btn_open_output.clicked.connect(self._open_output_dir)
        header_layout.addWidget(self._btn_open_output)

        self._btn_quit = QPushButton("Quit")
        self._btn_quit.clicked.connect(self.close)
        self._btn_quit.setStyleSheet("""
            QPushButton {
                background-color: #b33939;
                color: white;
                border-radius: 8px;
                padding: 6px 12px;
            }
            QPushButton:hover {
                background-color: #d64545;
            }
            QPushButton:pressed {
                background-color: #8f2d2d;
            }
        """)
        header_layout.addWidget(self._btn_quit)

        root_layout.addWidget(header)

        # -----------------------------
        # Split layout: Left config / Right data
        # -----------------------------
        splitter = QSplitter(Qt.Horizontal, root)
        splitter.setChildrenCollapsible(False)
        root_layout.addWidget(splitter, 1)

        # ===== Left: Configuration (scrollable) =====
        left_scroll = QScrollArea(splitter)
        left_scroll.setWidgetResizable(True)
        left_scroll.setFrameShape(QScrollArea.NoFrame)

        left_panel = QWidget(left_scroll)
        left_scroll.setWidget(left_panel)

        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(15, 15, 15, 15)
        left_layout.setSpacing(10)

        left_title_row = QWidget(left_panel)
        left_title_layout = QHBoxLayout(left_title_row)
        left_title_layout.setContentsMargins(0, 0, 0, 0)
        left_title_layout.setSpacing(8)

        left_title = QLabel("<b>Configuration</b>")
        left_title.setStyleSheet("font-size: 14px;")
        left_title_layout.addWidget(left_title)
        left_title_layout.addStretch(1)

        self._lbl_step = QLabel("Step 1 of 4")
        self._lbl_step.setStyleSheet("color: #9aa;")
        left_title_layout.addWidget(self._lbl_step)

        left_layout.addWidget(left_title_row)

        # Dataset Source
        box_source = QGroupBox("Dataset Source")
        box_source_layout = QVBoxLayout(box_source)

        self._file_picker = FilePickerButton(parent=self, ingest=self._ingest_service)
        self._file_picker.ingested.connect(self._on_ingested)

        self._ingest_info = QLabel("No file loaded.")
        self._ingest_info.setWordWrap(True)
        self._ingest_info.setStyleSheet(
            "padding: 8px; border: 1px solid #2b2b2b; border-radius: 8px; color: #ddd;"
        )

        box_source_layout.addWidget(self._file_picker)
        box_source_layout.addWidget(self._ingest_info)
        left_layout.addWidget(box_source)

        # Data Profiling
        box_profile = QGroupBox("Data Profiling")
        box_profile_layout = QVBoxLayout(box_profile)

        self._profile_info = QLabel("Profile: (not available)")
        self._profile_info.setWordWrap(True)
        self._profile_info.setStyleSheet(
            "padding: 8px; border: 1px solid #2b2b2b; border-radius: 8px; color: #ddd;"
        )
        box_profile_layout.addWidget(self._profile_info)
        left_layout.addWidget(box_profile)

        # Column mapping (Step 1)
        self._column_mapper = ColumnMapper(self)
        self._column_mapper.mapping_changed.connect(self._on_mapping_changed)
        left_layout.addWidget(self._column_mapper)

        # Key builder (Step 2)
        self._key_builder = KeyBuilder(self)
        self._key_builder.selection_changed.connect(self._on_key_selection_changed)
        left_layout.addWidget(self._key_builder)

        # Transform (action)
        box_actions = QGroupBox("Actions")
        box_actions_layout = QVBoxLayout(box_actions)

        self._btn_transform = QPushButton("Transform to canonical long")
        self._btn_transform.setEnabled(False)
        self._btn_transform.clicked.connect(self._run_transform)

        self._transform_info = QLabel("Transform: (not run)")
        self._transform_info.setWordWrap(True)
        self._transform_info.setTextFormat(Qt.RichText)
        self._transform_info.setTextInteractionFlags(Qt.TextSelectableByMouse)
        self._transform_info.setMaximumWidth(420)  # adjust if needed
        self._transform_info.setStyleSheet(
            "padding: 8px; border: 1px solid #2b2b2b; border-radius: 8px; color: #ddd;"
        )

        box_actions_layout.addWidget(self._btn_transform)
        box_actions_layout.addWidget(self._transform_info)
        left_layout.addWidget(box_actions)

        # Forecast Parameters (Step 3)
        self._forecast_prompt = ForecastPrompt(self)
        self._forecast_prompt.config_changed.connect(self._on_forecast_cfg_changed)
        left_layout.addWidget(self._forecast_prompt)
        left_layout.addStretch(1)

        # ===== Right: Tabs + Task Monitor =====
        right_panel = QWidget(splitter)
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.setSpacing(10)

        # Tabs (Input / Canonical / Forecast Results)
        self._tabs = QTabWidget(right_panel)

        tab_input = QWidget()
        tab_input_layout = QVBoxLayout(tab_input)
        tab_input_layout.setContentsMargins(0, 0, 0, 0)
        tab_input_layout.setSpacing(0)
        self._tbl_input = PreviewTable(tab_input)
        tab_input_layout.addWidget(self._tbl_input)

        tab_canon = QWidget()
        tab_canon_layout = QVBoxLayout(tab_canon)
        tab_canon_layout.setContentsMargins(0, 0, 0, 0)
        tab_canon_layout.setSpacing(0)
        self._tbl_canon = PreviewTable(tab_canon)
        tab_canon_layout.addWidget(self._tbl_canon)

        tab_forecast = QWidget()
        tab_forecast_layout = QVBoxLayout(tab_forecast)
        tab_forecast_layout.setContentsMargins(0, 0, 0, 0)
        tab_forecast_layout.setSpacing(0)
        self._tbl_forecast = PreviewTable(tab_forecast)
        tab_forecast_layout.addWidget(self._tbl_forecast)

        self._tabs.addTab(tab_input, "Input Data")
        self._tabs.addTab(tab_canon, "Canonical Data")
        self._tabs.addTab(tab_forecast, "Forecast Results")

        right_layout.addWidget(self._tabs, 1)

        # Task Monitor (bottom-right)
        box_task = QGroupBox("Task Monitor")
        box_task_layout = QVBoxLayout(box_task)
        box_task_layout.setContentsMargins(12, 10, 12, 12)
        box_task_layout.setSpacing(8)

        self._progress = QProgressBar(box_task)
        self._progress.setRange(0, 100)
        self._progress.setValue(0)
        self._progress.setVisible(False)

        self._progress_label = QLabel("")
        self._progress_label.setWordWrap(True)
        self._progress_label.setVisible(False)
        self._progress_label.setStyleSheet("color: #ddd;")

        btn_row = QWidget(box_task)
        btn_row_layout = QHBoxLayout(btn_row)
        btn_row_layout.setContentsMargins(0, 0, 0, 0)
        btn_row_layout.setSpacing(8)

        self._btn_cancel = QPushButton("Cancel")
        self._btn_cancel.setEnabled(False)
        self._btn_cancel.setVisible(False)
        self._btn_cancel.clicked.connect(self._cancel_current_task)

        self._btn_forecast = QPushButton("Run Forecast")
        self._btn_forecast.setEnabled(False)
        self._btn_forecast.clicked.connect(self._run_forecast)

        btn_row_layout.addStretch(1)
        btn_row_layout.addWidget(self._btn_cancel)
        btn_row_layout.addWidget(self._btn_forecast)

        self._forecast_info = QLabel("Forecast: (not run)")
        self._forecast_info.setWordWrap(True)
        self._forecast_info.setStyleSheet("color: #ddd;")

        box_task_layout.addWidget(self._progress)
        box_task_layout.addWidget(self._progress_label)
        box_task_layout.addWidget(btn_row)
        box_task_layout.addWidget(self._forecast_info)

        right_layout.addWidget(box_task)

        # Splitter sizing: left narrower, right wider
        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 1)
        splitter.setSizes([500, 680])

        # Status bar
        sb = QStatusBar(self)
        self.setStatusBar(sb)
        self._refresh_output_label()

        # Initialize prompt context
        self._forecast_prompt.set_context(frequency=None, n_points=None)
        self._apply_theme()
        

    
    def _apply_theme(self) -> None:
        self.setStyleSheet("""
            QWidget {
                background-color: #0c1014;
                color: #e6f2f2;
                font-size: 13px;
            }

            QGroupBox {
                border: 1px solid #436161;
                border-radius: 10px;
                margin-top: 10px;
                padding: 10px;
                background-color: #10161b;
            }

            QGroupBox:title {
                subcontrol-origin: margin;
                left: 8px;
                padding: 0 6px 0 6px;
                color: #2aa889;
            }

            QPushButton {
                background-color: #2aa889;
                color: #0c1014;
                border-radius: 8px;
                padding: 6px 12px;
            }

            QPushButton:hover {
                background-color: #4d8590;
            }

            QPushButton:disabled {
                background-color: #436161;
                color: #888;
            }

            QTabWidget::pane {
                border: 1px solid #436161;
                border-radius: 8px;
                margin-top: 4px;
            }

            QTabBar::tab {
                background: #10161b;
                padding: 8px 14px;
                border-top-left-radius: 6px;
                border-top-right-radius: 6px;
                color: #aaa;
            }

            QTabBar::tab:selected {
                background: #2aa889;
                color: #0c1014;
            }

            QProgressBar {
                border: 1px solid #436161;
                border-radius: 6px;
                text-align: center;
                background-color: #10161b;
            }

            QProgressBar::chunk {
                background-color: #2aa889;
                border-radius: 6px;
            }

            QTableWidget {
                background-color: #10161b;
                gridline-color: #1b242a;
            }

            QHeaderView::section {
                background-color: #436161;
                color: #e6f2f2;
                padding: 4px;
                border: none;
            }
        """)
    # -----------------------------
    # Output directory
    # -----------------------------
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

    # -----------------------------
    # Busy / task state
    # -----------------------------
    def _set_busy(self, kind: str, msg: str) -> None:
        self._current_task_kind = kind

        self._progress.setVisible(True)
        self._progress_label.setVisible(True)
        self._btn_cancel.setVisible(True)

        self._btn_cancel.setEnabled(True)
        self._progress.setValue(0)
        self._progress_label.setText(msg)

        # Lock UI
        self._file_picker.setEnabled(False)
        self._btn_transform.setEnabled(False)
        self._btn_forecast.setEnabled(False)
        self._btn_change_output.setEnabled(False)
        self._btn_open_output.setEnabled(False)

    def _clear_busy(self) -> None:
        self._current_task_kind = None
        self._current_task = None

        self._progress.setVisible(False)
        self._progress_label.setVisible(False)
        self._btn_cancel.setVisible(False)
        self._btn_cancel.setEnabled(False)

        # Unlock UI
        self._file_picker.setEnabled(True)
        self._btn_change_output.setEnabled(True)
        self._btn_open_output.setEnabled(True)
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

    # -----------------------------
    # Ingest / profile
    # -----------------------------
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
            f"<b>Rows (preview):</b> {len(data.preview_rows)}<br>"
            f"<b>Columns:</b> {len(data.columns)}<br>"
            f"<span style='color:#aaa;'><b>First columns:</b> {cols_preview}</span>"
        )

        # Right panel: Input tab
        self._tbl_input.set_preview_rows(data.preview_rows)
        self._tabs.setCurrentIndex(0)

        # Profile
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

        note_html = ""
        if profile.notes:
            note_html = f"<span style='color:#aaa;'><i>Note:</i> {profile.notes}</span>"

        self._profile_info.setText(
            "<b>Shape:</b> " + profile.shape + "<br>"
            f"<b>Date candidates:</b> {cand_txt}<br>"
            f"<b>Selected date:</b> {profile.inferred_date_column or 'None'}<br>"
            f"<b>Frequency:</b> {freq_txt}<br>"
            f"{note_html}"
        )

        # Left: Steps context
        self._lbl_step.setText("Step 1 of 4")

        self._column_mapper.set_context(columns=data.columns, profile=profile)
        self._key_builder.set_context(columns=data.columns, preview_rows=data.preview_rows)

        # Forecast prompt updated with freq (horizon recommendation may still need n_points)
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

        # Step label
        if self._last_ingested is None:
            self._lbl_step.setText("Step 1 of 4")
        elif self._last_transform_path is None:
            self._lbl_step.setText("Step 2 of 4")
        elif not self._last_forecast_cfg.enabled:
            self._lbl_step.setText("Step 3 of 4")
        else:
            self._lbl_step.setText("Step 4 of 4")

    # -----------------------------
    # Transform
    # -----------------------------
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
        self._tbl_canon.set_preview_rows(df_prev.to_dicts())
        self._tabs.setCurrentIndex(1)

        n_dates = (
            pl.scan_parquet(self._last_transform_path)
            .select(pl.col(CANON.ds).n_unique().alias("n"))
            .collect()["n"][0]
        )
        self._last_history_points = int(n_dates)
        self._forecast_prompt.set_context(frequency=self._last_profile_freq, n_points=self._last_history_points)

        freq_txt = self._last_profile_freq.name if self._last_profile_freq else "N/A"
        note_html = ""
        if notes:
            note_html = f"<span style='color:#aaa;'><i>Note:</i> {notes}</span>"

        self._transform_info.setText(
            "<b>Transform OK</b><br>"
            f"<span style='color:#aaa;'>Output:</span> {self._last_transform_path}<br>"
            f"<span style='color:#aaa;'>Columns:</span> {', '.join(canonical_cols)}<br>"
            f"<span style='color:#aaa;'>History points (unique ds):</span> {self._last_history_points}<br>"
            f"<span style='color:#aaa;'>Frequency:</span> {freq_txt}<br>"
            f"{note_html}"
        )

        self._clear_busy()

    # -----------------------------
    # Forecast
    # -----------------------------
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

                    self._tbl_forecast.set_preview_rows(df.to_dicts())
                    self._tabs.setCurrentIndex(2)
                    preview_loaded = True
                except Exception as exc:
                    preview_err = str(exc)

        note_html = ""
        if notes:
            note_html = f"<span style='color:#aaa;'><i>Note:</i> {notes}</span>"

        preview_html = ""
        if preview_loaded:
            preview_html = "<br><span style='color:#aaa;'><i>Preview:</i> loaded</span>"
        elif preview_err:
            preview_html = f"<br><span style='color:#f99;'><i>Preview error:</i> {preview_err}</span>"

        self._forecast_info.setText(
            "<b>Forecast OK</b><br>"
            f"<span style='color:#aaa;'>Output dir:</span> {out_dir}<br>"
            f"<span style='color:#aaa;'>Series files:</span> {len(series_files)} (CSV + Parquet)<br>"
            f"<span style='color:#aaa;'>Skipped series:</span> {skipped}<br>"
            f"{note_html}"
            f"{preview_html}"
        )

        log.info(
            "forecast_ui_ok",
            extra={"out_dir": str(out_dir), "written": len(series_files), "skipped": skipped},
        )
        self._clear_busy()