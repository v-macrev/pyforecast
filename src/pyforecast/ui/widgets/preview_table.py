from __future__ import annotations

from typing import Any, Sequence

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QHeaderView,
    QTableWidget,
    QTableWidgetItem,
)


class PreviewTable(QTableWidget):
    """
    Lightweight preview table optimized for:

    - Small previews (<= 200 rows)
    - Fast re-render after transform
    - Safe handling of large datasets (we never render full dataset)
    """

    MAX_PREVIEW_ROWS = 200
    MAX_CELL_CHARS = 500

    def __init__(self, parent=None) -> None:
        super().__init__(parent)

        self.setAlternatingRowColors(True)
        self.setSortingEnabled(False)
        self.setEditTriggers(QTableWidget.NoEditTriggers)
        self.setSelectionBehavior(QTableWidget.SelectRows)
        self.setSelectionMode(QTableWidget.SingleSelection)

        self.verticalHeader().setVisible(False)
        self.horizontalHeader().setStretchLastSection(True)
        self.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)

    # ---------------------------------------------------
    # Public API
    # ---------------------------------------------------

    def set_preview_rows(self, rows: Sequence[dict[str, Any]]) -> None:
        """
        Accepts list[dict] (as produced by Polars .to_dicts()).
        Only renders first MAX_PREVIEW_ROWS.
        """
        self.setUpdatesEnabled(False)
        try:
            self.clear()

            if not rows:
                self.setRowCount(0)
                self.setColumnCount(0)
                return

            rows = rows[: self.MAX_PREVIEW_ROWS]
            columns = list(rows[0].keys())

            self.setColumnCount(len(columns))
            self.setHorizontalHeaderLabels(columns)
            self.setRowCount(len(rows))

            for r_idx, row in enumerate(rows):
                for c_idx, col in enumerate(columns):
                    value = row.get(col)
                    item = QTableWidgetItem(self._format_cell(value))
                    item.setTextAlignment(Qt.AlignLeft | Qt.AlignVCenter)
                    self.setItem(r_idx, c_idx, item)

            self._auto_resize_columns(columns)

        finally:
            self.setUpdatesEnabled(True)

    # ---------------------------------------------------
    # Internal helpers
    # ---------------------------------------------------

    def _format_cell(self, value: Any) -> str:
        if value is None:
            return ""

        text = str(value)
        if len(text) > self.MAX_CELL_CHARS:
            return text[: self.MAX_CELL_CHARS] + "…"
        return text

    def _auto_resize_columns(self, columns: list[str]) -> None:

        header = self.horizontalHeader()

        if len(columns) <= 6:
            header.setSectionResizeMode(QHeaderView.ResizeToContents)
        else:
            header.setSectionResizeMode(QHeaderView.Interactive)

        for i in range(len(columns)):
            header.resizeSection(i, min(240, header.sectionSize(i)))