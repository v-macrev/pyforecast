from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from PySide6.QtCore import Qt, Signal
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


class ForecastPrompt(QGroupBox):

    config_changed = Signal(ForecastConfig)

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__("Forecast (Prophet)", parent)

        self._frequency: TimeFrequency | None = None
        self._n_points: int | None = None

        self._chk_enable = QCheckBox("Enable forecast")
        self._chk_enable.stateChanged.connect(self._emit_change)

        self._spin_horizon = QSpinBox()
        self._spin_horizon.setRange(1, 10_000)
        self._spin_horizon.setValue(12)
        self._spin_horizon.valueChanged.connect(self._emit_change)

        self._lbl_recommendation = QLabel("")
        self._lbl_recommendation.setWordWrap(True)
        self._lbl_recommendation.setStyleSheet("color: #444;")

        self._lbl_disclaimer = QLabel(
            "⚠ Forecasting large datasets may take time. "
            "You can cancel the operation while running, "
            "but cancellation occurs at safe checkpoints."
        )
        self._lbl_disclaimer.setWordWrap(True)
        self._lbl_disclaimer.setStyleSheet("color: #777; font-size: 11px;")

        form = QFormLayout()
        form.setLabelAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        form.addRow(self._chk_enable)
        form.addRow("Horizon (periods):", self._spin_horizon)

        layout = QVBoxLayout(self)
        layout.addLayout(form)
        layout.addWidget(self._lbl_recommendation)
        layout.addWidget(self._lbl_disclaimer)

    def set_context(self, frequency: TimeFrequency | None, n_points: int | None) -> None:
        """
        Update internal context (history size + frequency).
        Used to compute smarter recommended horizon.
        """
        self._frequency = frequency
        self._n_points = n_points

        recommended = self._compute_recommended_horizon(n_points)
        if recommended is not None:
            self._spin_horizon.setValue(recommended)

        self._update_recommendation_label(recommended)
        self._emit_change()

    def _compute_recommended_horizon(self, n_points: int | None) -> int | None:
        if n_points is None or n_points <= 3:
            return None

        if n_points < 20:
            pct = 0.25
        elif n_points < 100:
            pct = 0.20
        else:
            pct = 0.15

        horizon = max(1, int(n_points * pct))
        horizon = min(horizon, max(1, n_points // 2))

        return horizon

    def _update_recommendation_label(self, recommended: int | None) -> None:
        if recommended is None:
            self._lbl_recommendation.setText(
                "Recommendation unavailable — insufficient historical data."
            )
            return

        self._lbl_recommendation.setText(
            f"Recommended horizon: <b>{recommended}</b> periods "
            f"(based on {self._n_points} historical points)."
        )

    def _emit_change(self) -> None:
        cfg = ForecastConfig(
            enabled=self._chk_enable.isChecked(),
            horizon=self._spin_horizon.value(),
        )
        self.config_changed.emit(cfg)