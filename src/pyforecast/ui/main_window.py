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

from pyforecast.infrastructure.logging import get_logger

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
        self.setMinimumSize(980, 640)

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

        self._btn_open = QPushButton("Open file (CSV / Excel)")
        self._btn_open.setFixedHeight(40)
        self._btn_open.clicked.connect(self._not_implemented_yet)

        self._btn_quit = QPushButton("Quit")
        self._btn_quit.clicked.connect(self.close)

        layout.addWidget(title)
        layout.addWidget(subtitle)
        layout.addSpacing(8)
        layout.addWidget(self._btn_open)
        layout.addWidget(self._btn_quit)
        layout.addStretch(1)

        sb = QStatusBar(self)
        sb.showMessage(f"Outputs: {self._paths.outputs_dir}")
        self.setStatusBar(sb)

        log.info("main_window_ready", extra={"outputs_dir": str(self._paths.outputs_dir)})

    def _not_implemented_yet(self) -> None:
        QMessageBox.information(
            self,
            "Next step",
            "UI scaffold is ready.\n\nNext we will add:\n"
            "- File picker + preview\n"
            "- Column mapper (date/value)\n"
            "- Key builder (cd_key)\n"
            "- Transform + forecast pipeline",
        )