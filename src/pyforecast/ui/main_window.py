from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QLabel,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QStatusBar,
    QVBoxLayout,
    QWidget,
)

from pyforecast.application.services import IngestService, IngestedData, ProfilingService
from pyforecast.infrastructure.logging import get_logger
from pyforecast.ui.widgets import FilePickerButton, PreviewTable

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
        self.setMinimumSize(980, 740)

        root = QWidget(self)
        self.setCentralWidget(root)

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

        self._preview = PreviewTable(self)
        self._preview.setMinimumHeight(260)

        self._ingest_service = IngestService(preview_n=200)
        self._profiling_service = ProfilingService(sample_limit=200)

        self._file_picker = FilePickerButton(parent=self, ingest=self._ingest_service)
        self._file_picker.ingested.connect(self._on_ingested)

        self._btn_next = QPushButton("Next: map columns")
        self._btn_next.setEnabled(False)
        self._btn_next.clicked.connect(self._not_implemented_yet)

        self._btn_quit = QPushButton("Quit")
        self._btn_quit.clicked.connect(self.close)

        layout.addWidget(title)
        layout.addWidget(subtitle)
        layout.addSpacing(8)
        layout.addWidget(self._file_picker)
        layout.addWidget(self._ingest_info)
        layout.addWidget(self._profile_info)
        layout.addWidget(QLabel("<b>Preview</b>"))
        layout.addWidget(self._preview)
        layout.addWidget(self._btn_next)
        layout.addWidget(self._btn_quit)
        layout.addStretch(1)

        sb = QStatusBar(self)
        sb.showMessage(f"Outputs: {self._paths.outputs_dir}")
        self.setStatusBar(sb)

        log.info("main_window_ready", extra={"outputs_dir": str(self._paths.outputs_dir)})

        self._last_ingested: IngestedData | None = None

    def _on_ingested(self, data: IngestedData) -> None:
        self._last_ingested = data
        cols_preview = ", ".join(data.columns[:12]) + (" ..." if len(data.columns) > 12 else "")
        self._ingest_info.setText(
            f"<b>Loaded:</b> {data.path.name}<br>"
            f"<b>Type:</b> {data.file_type.upper()}<br>"
            f"<b>Columns:</b> {len(data.columns)}<br>"
            f"<b>Preview:</b> {len(data.preview_rows)} rows<br>"
            f"<b>First columns:</b> {cols_preview}"
        )
        self._preview.set_preview_rows(data.preview_rows)

        profile = self._profiling_service.profile(data.columns, data.preview_rows)
        if profile.frequency:
            freq_txt = f"{profile.frequency.frequency.name} (conf={profile.frequency.confidence:.2f}, medΔ={profile.frequency.median_delta_days})"
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

        self._btn_next.setEnabled(True)

        log.info(
            "file_ingested",
            extra={
                "path": str(data.path),
                "file_type": data.file_type,
                "n_cols": len(data.columns),
                "shape": profile.shape,
                "date_col": profile.inferred_date_column,
                "freq": (profile.frequency.frequency.value if profile.frequency else None),
                "freq_conf": (profile.frequency.confidence if profile.frequency else None),
            },
        )

    def _not_implemented_yet(self) -> None:
        if self._last_ingested is None:
            QMessageBox.information(self, "No file", "Load a file first.")
            return

        QMessageBox.information(
            self,
            "Next step",
            "Next we will implement:\n"
            "- wide vs long confirmation / override\n"
            "- date column selection (combobox)\n"
            "- value column selection\n"
            "- cd_key builder (multi-select)\n",
        )