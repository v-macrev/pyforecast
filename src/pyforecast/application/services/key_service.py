from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from pyforecast.domain.errors import SchemaInferenceError


@dataclass(frozen=True)
class KeySpec:
    key_parts: list[str]
    separator: str = "|"
    null_token: str = ""

def _norm_part(v: object, null_token: str) -> str:
    if v is None:
        return null_token
    s = str(v).strip()
    return s if s else null_token


def build_cd_key_from_row(row: dict[str, object], spec: KeySpec) -> str:
    parts: list[str] = []
    for col in spec.key_parts:
        parts.append(_norm_part(row.get(col, None), spec.null_token))
    return spec.separator.join(parts)


def validate_key_parts(columns: Iterable[str], key_parts: Iterable[str]) -> list[str]:
    cols = set(columns)
    out: list[str] = []
    seen: set[str] = set()
    for c in key_parts:
        c = str(c).strip()
        if not c:
            continue
        if c not in cols:
            raise SchemaInferenceError(f"Key column not found: '{c}'")
        if c not in seen:
            out.append(c)
            seen.add(c)

    if not out:
        raise SchemaInferenceError("No key columns selected.")
    return out


def build_cd_key_for_preview(
    preview_rows: list[dict[str, object]],
    columns: list[str],
    key_parts: list[str],
    separator: str = "|",
) -> list[str]:

    kp = validate_key_parts(columns, key_parts)
    spec = KeySpec(key_parts=kp, separator=separator)
    return [build_cd_key_from_row(r, spec) for r in preview_rows]