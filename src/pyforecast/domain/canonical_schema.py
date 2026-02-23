from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CanonicalColumns:

    cd_key: str = "cd_key"
    ds: str = "ds"
    y: str = "y"

    metric: str = "metric"


CANON = CanonicalColumns()