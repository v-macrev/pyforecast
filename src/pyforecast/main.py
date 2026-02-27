from __future__ import annotations

import sys
from pathlib import Path

from PySide6.QtWidgets import QApplication, QMessageBox

from pyforecast.application.services.config_service import AppConfig, ConfigService
from pyforecast.infrastructure.logging import get_logger, init_logging
from pyforecast.ui.main_window import MainWindow


def main() -> int:
    cfg_svc = ConfigService()
    cfg: AppConfig = cfg_svc.load()

    base_dir: Path = cfg_svc.base_dir
    output_dir: Path = cfg_svc.resolve_output_dir(cfg)
    logs_dir: Path = cfg_svc.logs_dir
    cfg_svc.ensure_dirs(output_dir)

    init_logging(logs_dir=logs_dir)
    log = get_logger(__name__)
    log.info("app_start", extra={"base_dir": str(base_dir), "output_dir": str(output_dir), "logs_dir": str(logs_dir)})

    app = QApplication(sys.argv)

    try:
        # ✅ IMPORTANT: PySide/Shiboken does not accept unknown keyword args on QObject/QWidget subclasses
        window = MainWindow(base_dir)  # positional, not MainWindow(base_dir=...)
        window.set_output_dir(output_dir)
        window.show()
        return app.exec()
    except Exception as exc:
        log.exception("fatal_error", extra={"error": str(exc)})
        QMessageBox.critical(None, "PyForecast - Fatal Error", str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())