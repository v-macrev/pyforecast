from __future__ import annotations

import sys
from pathlib import Path

from PySide6.QtWidgets import QApplication, QMessageBox

from pyforecast.infrastructure.logging import configure_logging, get_logger
from pyforecast.ui.main_window import MainWindow


def _set_app_metadata(app: QApplication) -> None:
    app.setApplicationName("PyForecast")
    app.setOrganizationName("Macrev")
    app.setOrganizationDomain("local")
    app.setApplicationDisplayName("PyForecast")


def _ensure_app_dirs() -> Path:
    base = Path.home() / ".pyforecast"
    (base / "logs").mkdir(parents=True, exist_ok=True)
    (base / "outputs").mkdir(parents=True, exist_ok=True)
    return base


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv
    app = QApplication(argv)
    _set_app_metadata(app)

    base_dir = _ensure_app_dirs()
    configure_logging(log_dir=base_dir / "logs")
    log = get_logger(__name__)
    log.info("app_start", extra={"base_dir": str(base_dir)})

    try:
        window = MainWindow(base_dir=base_dir)
        window.show()
        return app.exec()
    except Exception as exc:
        log.exception("fatal_error", extra={"error": str(exc)})
        QMessageBox.critical(
            None,
            "Fatal error",
            f"An unexpected error occurred.\n\n{exc}\n\n"
            f"Check logs at: {base_dir / 'logs'}",
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())