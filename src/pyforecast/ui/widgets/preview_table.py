from __future__ import annotations

from typing import Any

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt
from PySide6.QtWidgets import QTableView, QWidget


class PreviewTableModel(QAbstractTableModel):
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        super().__init__()
        self._rows: list[dict[str, Any]] = rows or []
        self._cols: list[str] = self._infer_columns(self._rows)

    @staticmethod
    def _infer_columns(rows: list[dict[str, Any]]) -> list[str]:
        if not rows:
            return []
        # Stable column order: use keys from first row, then append unseen keys.
        cols = list(rows[0].keys())
        seen = set(cols)
        for r in rows[1:]:
            for k in r.keys():
                if k not in seen:
                    cols.append(k)
                    seen.add(k)
        return cols

    def set_rows(self, rows: list[dict[str, Any]]) -> None:
        self.beginResetModel()
        self._rows = rows
        self._cols = self._infer_columns(rows)
        self.endResetModel()

    def rowCount(self, parent: QModelIndex | None = None) -> int:  # noqa: N802
        return 0 if parent and parent.isValid() else len(self._rows)

    def columnCount(self, parent: QModelIndex | None = None) -> int:  # noqa: N802
        return 0 if parent and parent.isValid() else len(self._cols)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole) -> Any:  # noqa: N802
        if not index.isValid() or role != Qt.DisplayRole:
            return None
        r = index.row()
        c = index.column()
        key = self._cols[c]
        val = self._rows[r].get(key, None)
        if val is None:
            return ""
        return str(val)

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole) -> Any:  # noqa: N802
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal:
            return self._cols[section] if 0 <= section < len(self._cols) else ""
        return str(section + 1)

    def columns(self) -> list[str]:
        return list(self._cols)


class PreviewTable(QTableView):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._model = PreviewTableModel([])
        self.setModel(self._model)

        self.setAlternatingRowColors(True)
        self.setSortingEnabled(False)
        self.setWordWrap(False)

        hh = self.horizontalHeader()
        hh.setStretchLastSection(True)
        hh.setDefaultAlignment(Qt.AlignLeft | Qt.AlignVCenter)

        vh = self.verticalHeader()
        vh.setDefaultSectionSize(22)

    def set_preview_rows(self, rows: list[dict[str, Any]]) -> None:
        self._model.set_rows(rows)
        self.resizeColumnsToContents()