from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from PySide6.QtCore import Qt, Signal  # ← FIX: import Qt here
from PySide6.QtWidgets import (
    QComboBox,
    QFormLayout,
    QGroupBox,
    QLabel,
    QRadioButton,
    QVBoxLayout,
    QWidget,
)

from pyforecast.application.services.profiling_service import ProfileResult


@dataclass(frozen=True)
class ColumnMapping:
    shape: str  # "long" | "wide"
    date_col: str
    value_col: str | None


class ColumnMapper(QWidget):
    mapping_changed = Signal(object)  # ColumnMapping

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)

        self._columns: list[str] = []
        self._profile: ProfileResult | None = None

        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(10)

        title = QLabel("<b>Step 1 — Map columns</b>")
        title.setStyleSheet("font-size: 14px;")

        self._shape_box = QGroupBox("Data shape")
        shape_layout = QVBoxLayout(self._shape_box)
        self._rb_long = QRadioButton("Long (one date column + one value column)")
        self._rb_wide = QRadioButton("Wide (multiple period/value columns)")
        self._rb_long.setChecked(True)
        self._rb_long.toggled.connect(self._emit_mapping)
        self._rb_wide.toggled.connect(self._emit_mapping)
        shape_layout.addWidget(self._rb_long)
        shape_layout.addWidget(self._rb_wide)

        self._form_box = QGroupBox("Columns")
        form_layout = QFormLayout(self._form_box)
        form_layout.setLabelAlignment(Qt.AlignLeft | Qt.AlignVCenter)

        self._cb_date = QComboBox()
        self._cb_date.currentIndexChanged.connect(self._emit_mapping)

        self._cb_value = QComboBox()
        self._cb_value.currentIndexChanged.connect(self._emit_mapping)

        form_layout.addRow("Date column:", self._cb_date)
        form_layout.addRow("Value column:", self._cb_value)

        self._hint = QLabel(
            "Tip: If the inferred selection is wrong, override it here. "
            "Next step will build cd_key."
        )
        self._hint.setWordWrap(True)
        self._hint.setStyleSheet("color: #666;")

        root.addWidget(title)
        root.addWidget(self._shape_box)
        root.addWidget(self._form_box)
        root.addWidget(self._hint)
        root.addStretch(1)

        self._set_enabled(False)

    def set_context(self, columns: Iterable[str], profile: ProfileResult | None = None) -> None:
        self._columns = list(columns)
        self._profile = profile

        self._cb_date.blockSignals(True)
        self._cb_value.blockSignals(True)
        try:
            self._cb_date.clear()
            self._cb_value.clear()

            self._cb_date.addItems(self._columns)
            self._cb_value.addItems(self._columns)

            if profile and profile.inferred_date_column and profile.inferred_date_column in self._columns:
                self._cb_date.setCurrentText(profile.inferred_date_column)

            guess_value = self._guess_value_column()
            if guess_value:
                self._cb_value.setCurrentText(guess_value)
        finally:
            self._cb_date.blockSignals(False)
            self._cb_value.blockSignals(False)

        if profile and profile.shape in {"long", "wide"}:
            if profile.shape == "wide":
                self._rb_wide.setChecked(True)
            else:
                self._rb_long.setChecked(True)
        else:
            self._rb_long.setChecked(True)

        self._set_enabled(bool(self._columns))
        self._emit_mapping()

    def mapping(self) -> ColumnMapping | None:
        if not self.isEnabled() or not self._columns:
            return None

        date_col = self._cb_date.currentText().strip()
        value_col = self._cb_value.currentText().strip()
        shape = "wide" if self._rb_wide.isChecked() else "long"

        if not date_col:
            return None

        if shape == "wide":
            return ColumnMapping(shape=shape, date_col=date_col, value_col=None)

        return ColumnMapping(shape=shape, date_col=date_col, value_col=value_col or None)

    def _guess_value_column(self) -> str | None:
        if not self._columns:
            return None

        date_col = (
            self._profile.inferred_date_column
            if self._profile and self._profile.inferred_date_column
            else None
        )

        for c in self._columns:
            if date_col and c == date_col:
                continue
            cl = c.lower()
            if any(tok in cl for tok in ("value", "valor", "qty", "qtd", "volume", "sales", "y")):
                return c

        for c in self._columns:
            if date_col and c == date_col:
                continue
            return c

        return None

    def _set_enabled(self, enabled: bool) -> None:
        self._shape_box.setEnabled(enabled)
        self._form_box.setEnabled(enabled)
        self.setEnabled(enabled)

    def _emit_mapping(self) -> None:
        m = self.mapping()
        if m is not None:
            self.mapping_changed.emit(m)