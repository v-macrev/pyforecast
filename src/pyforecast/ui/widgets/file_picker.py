from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from PySide6.QtCore import QObject, Signal
from PySide6.QtWidgets import QFileDialog, QMessageBox, QPushButton

from pyforecast.application.services import IngestService, IngestedData
from pyforecast.domain.errors import PyForecastError
from pyforecast.infrastructure.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class FilePickerConfig:
    title: str = "Open data file"
    filter_spec: str = "Data files (*.csv *.xlsx);;CSV (*.csv);;Excel (*.xlsx);;All files (*.*)"


class FilePickerButton(QPushButton):
    ingested = Signal(object)  # IngestedData
    failed = Signal(str)  # error message

    def __init__(
        self,
        parent: QObject | None,
        ingest: IngestService,
        config: FilePickerConfig | None = None,
    ) -> None:
        super().__init__("Open file (CSV / Excel)", parent=parent)
        self._ingest = ingest
        self._cfg = config or FilePickerConfig()
        self.clicked.connect(self._on_click)

        self.setFixedHeight(40)

    def _on_click(self) -> None:
        file_path, _ = QFileDialog.getOpenFileName(
            self.window(),
            self._cfg.title,
            str(Path.home()),
            self._cfg.filter_spec,
        )
        if not file_path:
            return

        path = Path(file_path)
        log.info("file_selected", extra={"path": str(path)})

        try:
            data = self._ingest.ingest(path)
            self.ingested.emit(data)
        except PyForecastError as exc:
            msg = str(exc)
            log.warning("ingest_failed", extra={"path": str(path), "error": msg})
            self.failed.emit(msg)
            QMessageBox.warning(self.window(), "Could not open file", msg)
        except Exception as exc:
            # last-resort to avoid UI crash
            msg = f"Unexpected error while opening file: {exc}"
            log.exception("ingest_failed_unexpected", extra={"path": str(path), "error": str(exc)})
            self.failed.emit(msg)
            QMessageBox.critical(self.window(), "Unexpected error", msg)