from __future__ import annotations

from dataclasses import dataclass

from PySide6.QtCore import Signal
from PySide6.QtWidgets import (
    QAbstractItemView,
    QGroupBox,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QVBoxLayout,
    QWidget,
)


@dataclass(frozen=True)
class KeyBuildConfig:
    separator: str = "|"
    max_preview_rows: int = 8


@dataclass(frozen=True)
class KeySelection:
    key_parts: list[str]
    separator: str


class KeyBuilder(QWidget):
    selection_changed = Signal(object)  # KeySelection

    def __init__(self, parent: QWidget | None = None, config: KeyBuildConfig | None = None) -> None:
        super().__init__(parent)
        self._cfg = config or KeyBuildConfig()

        self._columns: list[str] = []
        self._preview_rows: list[dict[str, object]] = []

        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(10)

        title = QLabel("<b>Step 2 — Build cd_key</b>")
        title.setStyleSheet("font-size: 14px;")

        self._box = QGroupBox("Select key columns (multi-select)")
        box_layout = QVBoxLayout(self._box)

        self._list = QListWidget()
        self._list.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self._list.itemSelectionChanged.connect(self._emit_selection)

        self._preview = QLabel("cd_key preview: (select columns)")
        self._preview.setWordWrap(True)
        self._preview.setStyleSheet("color: #444; padding: 8px; border: 1px solid #eee; border-radius: 8px;")

        hint = QLabel(
            "Tip: choose stable identifier columns (e.g., store, sku, segment). "
            "Avoid free-text columns."
        )
        hint.setWordWrap(True)
        hint.setStyleSheet("color: #666;")

        box_layout.addWidget(self._list)
        root.addWidget(title)
        root.addWidget(self._box)
        root.addWidget(self._preview)
        root.addWidget(hint)
        root.addStretch(1)

        self.setEnabled(False)

    def set_context(self, columns: list[str], preview_rows: list[dict[str, object]]) -> None:
        self._columns = list(columns)
        self._preview_rows = list(preview_rows)

        self._list.blockSignals(True)
        try:
            self._list.clear()
            for c in self._columns:
                self._list.addItem(QListWidgetItem(c))
        finally:
            self._list.blockSignals(False)

        self.setEnabled(bool(self._columns))
        self._update_preview()
        self._emit_selection()

    def selection(self) -> KeySelection | None:
        if not self.isEnabled():
            return None
        key_parts = [i.text() for i in self._list.selectedItems()]
        if not key_parts:
            return None
        return KeySelection(key_parts=key_parts, separator=self._cfg.separator)

    def _emit_selection(self) -> None:
        sel = self.selection()
        self._update_preview()
        if sel is not None:
            self.selection_changed.emit(sel)

    def _update_preview(self) -> None:
        sel = self.selection()
        if sel is None:
            self._preview.setText("cd_key preview: (select one or more columns)")
            return

        lines: list[str] = []
        n = min(self._cfg.max_preview_rows, len(self._preview_rows))
        for r in self._preview_rows[:n]:
            parts = []
            for col in sel.key_parts:
                v = r.get(col, "")
                s = "" if v is None else str(v).strip()
                parts.append(s)
            lines.append(sel.separator.join(parts))

        preview_txt = "<br>".join(lines) if lines else "(no preview rows)"
        self._preview.setText(f"<b>cd_key preview</b> ({n} rows):<br>{preview_txt}")