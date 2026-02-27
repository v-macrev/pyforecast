from __future__ import annotations
 
from dataclasses import dataclass
from pathlib import Path
from typing import Any
 
from PySide6.QtCore import QObject, QThread, Signal, Slot
 
from pyforecast.application.services import (
    ForecastRequest,
    ForecastResult,
    TransformRequest,
    TransformResult,
    forecast_prophet,
    transform_to_canonical_long,
)
from pyforecast.domain.errors import PyForecastError
from pyforecast.infrastructure.logging import get_logger
 
log = get_logger(__name__)

 
class _Runner(QObject):

    started = Signal()
    progress = Signal(int, str)  # percent, message
    finished = Signal(object)    # result object (TransformResult/ForecastResult)
    failed = Signal(str)         # user-friendly message
    cancelled = Signal()
 
    def __init__(self) -> None:
        super().__init__()
        self._cancel_requested = False
 
    def request_cancel(self) -> None:
        self._cancel_requested = True
 
    def _check_cancel(self) -> None:
        if self._cancel_requested:
            raise _Cancelled()
 
    def _emit_progress(self, pct: int, msg: str) -> None:
        pct = max(0, min(100, int(pct)))
        self.progress.emit(pct, msg)
 
 
class _Cancelled(Exception):
    pass
 
 
@dataclass(frozen=True)
class ThreadHandle:

    thread: QThread
    runner: _Runner
 
    def cancel(self) -> None:
        self.runner.request_cancel()
 
    def is_running(self) -> bool:
        return self.thread.isRunning()
 
class TransformWorker(_Runner):
 
    def __init__(self, req: TransformRequest) -> None:
        super().__init__()
        self._req = req
 
    @Slot()
    def run(self) -> None:
        self.started.emit()
        try:
            self._emit_progress(2, "Preparing transform…")
            self._check_cancel()
 
            self._emit_progress(10, "Reading input & building plan…")
            self._check_cancel()
 
            res = transform_to_canonical_long(self._req)
            self._check_cancel()
 
            self._emit_progress(100, "Transform complete.")
            self.finished.emit(res)
 
        except _Cancelled:
            log.info("transform_cancelled", extra={"in_path": str(self._req.path)})
            self.cancelled.emit()
        except PyForecastError as exc:
            log.warning("transform_failed", extra={"error": str(exc), "in_path": str(self._req.path)})
            self.failed.emit(str(exc))
        except Exception as exc:
            log.exception("transform_failed_unexpected", extra={"error": str(exc), "in_path": str(self._req.path)})
            self.failed.emit(f"Unexpected error during transform: {exc}")
 
class ForecastWorker(_Runner):
 
    def __init__(self, req: ForecastRequest) -> None:
        super().__init__()
        self._req = req
 
    @Slot()
    def run(self) -> None:
        self.started.emit()
        try:
            self._emit_progress(2, "Preparing forecast…")
            self._check_cancel()
 
            self._emit_progress(15, "Loading canonical dataset keys…")
            self._check_cancel()
 
            res = forecast_prophet(self._req)
            self._check_cancel()
 
            self._emit_progress(100, "Forecast complete.")
            self.finished.emit(res)
 
        except _Cancelled:
            log.info("forecast_cancelled", extra={"canonical_path": str(self._req.canonical_path)})
            self.cancelled.emit()
        except PyForecastError as exc:
            log.warning("forecast_failed", extra={"error": str(exc), "canonical_path": str(self._req.canonical_path)})
            self.failed.emit(str(exc))
        except Exception as exc:
            log.exception("forecast_failed_unexpected", extra={"error": str(exc)})
            self.failed.emit(f"Unexpected error during forecast: {exc}")
 
def start_transform_thread(req: TransformRequest) -> ThreadHandle:
    thread = QThread()
    worker = TransformWorker(req)
    worker.moveToThread(thread)
 
    thread.started.connect(worker.run)
    # Cleanup
    worker.finished.connect(lambda _: thread.quit())
    worker.failed.connect(lambda _: thread.quit())
    worker.cancelled.connect(thread.quit)
 
    thread.finished.connect(worker.deleteLater)
    thread.finished.connect(thread.deleteLater)
 
    thread.start()
    return ThreadHandle(thread=thread, runner=worker)
 
 
def start_forecast_thread(req: ForecastRequest) -> ThreadHandle:

    thread = QThread()
    worker = ForecastWorker(req)
    worker.moveToThread(thread)
 
    thread.started.connect(worker.run)
    worker.finished.connect(lambda _: thread.quit())
    worker.failed.connect(lambda _: thread.quit())
    worker.cancelled.connect(thread.quit)
 
    thread.finished.connect(worker.deleteLater)
    thread.finished.connect(thread.deleteLater)
 
    thread.start()
    return ThreadHandle(thread=thread, runner=worker)