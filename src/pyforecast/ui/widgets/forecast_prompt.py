from __future__ import annotations

from dataclasses import dataclass

from PySide6.QtCore import Signal
from PySide6.QtWidgets import (
    QCheckBox,
    QFormLayout,
    QGroupBox,
    QLabel,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from pyforecast.domain.timefreq import TimeFrequency


@dataclass(frozen=True)
class ForecastConfig:
    enabled: bool
    horizon: int


class ForecastPrompt(QWidget):
    """
    Forecast configuration widget.

    Key behaviour:
    - Recommendation is based on the amount of history (number of dates),
      with light frequency-aware guardrails (caps/mins).
    """

    config_changed = Signal(object)  # ForecastConfig

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)

        self._frequency: TimeFrequency | None = None
        self._n_points: int | None = None

        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(10)

        title = QLabel("<b>Step 3 — Forecast (Prophet)</b>")
        title.setStyleSheet("font-size: 14px;")

        self._box = QGroupBox("Forecast settings")
        form = QFormLayout(self._box)

        self._chk_enable = QCheckBox("Enable forecasting")
        self._chk_enable.stateChanged.connect(self._emit)

        self._sp_horizon = QSpinBox()
        self._sp_horizon.setMinimum(1)
        self._sp_horizon.setMaximum(10_000)
        self._sp_horizon.setValue(12)
        self._sp_horizon.valueChanged.connect(self._emit)

        self._lbl_recommendation = QLabel("Recommended horizon: —")
        self._lbl_recommendation.setStyleSheet("color: #444;")

        self._lbl_disclaimer = QLabel("")
        self._lbl_disclaimer.setWordWrap(True)
        self._lbl_disclaimer.setStyleSheet("color: #666;")

        form.addRow(self._chk_enable)
        form.addRow("Forecast horizon (periods):", self._sp_horizon)
        form.addRow("", self._lbl_recommendation)
        form.addRow("", self._lbl_disclaimer)

        root.addWidget(title)
        root.addWidget(self._box)
        root.addStretch(1)

        self.setEnabled(False)

    def set_context(self, *, frequency: TimeFrequency | None, n_points: int | None) -> None:
        """
        Provide series context:
          - frequency: inferred data cadence (may be None)
          - n_points: number of unique dates in the selected series (or global sample)
        """
        self._frequency = frequency
        self._n_points = n_points if (n_points is None or n_points >= 0) else None

        self._update_recommendation()

        # Only enable if we know at least something about history
        self.setEnabled(self._n_points is not None and self._n_points >= 3)

        # If we have a recommendation, set horizon default to it (non-destructive if user already changed)
        rec = self._recommended_horizon(self._frequency, self._n_points)
        if rec is not None:
            # Only auto-set if still at default-ish state (avoid fighting the user)
            if self._sp_horizon.value() == 12:
                self._sp_horizon.setValue(rec)

        self._emit()

    def config(self) -> ForecastConfig:
        return ForecastConfig(
            enabled=self._chk_enable.isChecked(),
            horizon=int(self._sp_horizon.value()),
        )

    def _update_recommendation(self) -> None:
        rec = self._recommended_horizon(self._frequency, self._n_points)
        if rec is None:
            self._lbl_recommendation.setText("Recommended horizon: —")
            self._lbl_disclaimer.setText(
                "Disclaimer: Set horizon based on how much history you have. "
                "Longer horizons increase uncertainty. Import data and select a date column first."
            )
            return

        n = int(self._n_points or 0)
        freq_name = self._frequency.name if self._frequency else "UNKNOWN"

        self._lbl_recommendation.setText(
            f"Recommended horizon: {rec} period(s) — based on {n} historical point(s) ({freq_name})"
        )
        self._lbl_disclaimer.setText(
            "Disclaimer: A safe horizon is usually a fraction of your history. "
            "As uncertainty grows, accuracy falls faster for longer horizons. "
            "Rule of thumb: keep horizon within ~15–30% of the available history, "
            "and prefer shorter horizons when history is sparse or irregular."
        )

    @staticmethod
    def _recommended_horizon(freq: TimeFrequency | None, n_points: int | None) -> int | None:
        """
        Smarter-than-naive heuristic:
        - Primary driver: history length (n_points)
        - For short histories, be conservative.
        - Apply frequency-aware caps/mins as guardrails (not the main driver).
        """
        if n_points is None or n_points < 3:
            return None

        n = n_points

        # Base ratio: how much of history to forecast
        # - Short histories: lower ratio (more conservative)
        # - Moderate: standard
        # - Long: slightly higher but still bounded by caps
        if n < 12:
            ratio = 0.15
        elif n < 30:
            ratio = 0.20
        elif n < 90:
            ratio = 0.25
        else:
            ratio = 0.30

        rec = max(1, int(round(n * ratio)))

        # Guardrails: frequency-based min/cap (light influence)
        # These exist to prevent silly numbers like 1 year forecast for 4 yearly points, etc.
        if freq == TimeFrequency.DAILY:
            rec = max(rec, 7)
            rec = min(rec, 90)
        elif freq == TimeFrequency.WEEKLY:
            rec = max(rec, 4)
            rec = min(rec, 26)
        elif freq == TimeFrequency.MONTHLY:
            rec = max(rec, 3)
            rec = min(rec, 24)
        elif freq == TimeFrequency.QUARTERLY:
            rec = max(rec, 2)
            rec = min(rec, 12)
        elif freq == TimeFrequency.YEARLY:
            rec = max(rec, 1)
            rec = min(rec, 5)
        else:
            # unknown/irregular: be conservative
            rec = max(rec, 3)
            rec = min(rec, max(3, int(round(n * 0.20))))

        # Final sanity: never recommend more than half the history
        rec = min(rec, max(1, n // 2))
        return rec

    def _emit(self) -> None:
        self.config_changed.emit(self.config())