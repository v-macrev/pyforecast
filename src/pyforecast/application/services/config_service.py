from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pyforecast.infrastructure.logging import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class AppConfig:
    output_dir: Path | None = None  # if None => default under Documents/PyForecast/Output


class ConfigService:

    def __init__(self, base_dir: Path | None = None) -> None:
        self._base_dir = base_dir or default_base_dir()
        self._config_path = self._base_dir / "config.json"

    @property
    def base_dir(self) -> Path:
        return self._base_dir

    @property
    def default_output_dir(self) -> Path:
        return self._base_dir / "Output"

    @property
    def logs_dir(self) -> Path:
        return self._base_dir / "Logs"

    def ensure_dirs(self, output_dir: Path | None = None) -> None:
        self._base_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        (output_dir or self.default_output_dir).mkdir(parents=True, exist_ok=True)

    def load(self) -> AppConfig:
        if not self._config_path.exists():
            cfg = AppConfig(output_dir=None)
            self.ensure_dirs(self.default_output_dir)
            return cfg

        try:
            raw = json.loads(self._config_path.read_text(encoding="utf-8"))
            out = _as_path(raw.get("output_dir"))
            cfg = AppConfig(output_dir=out)

            self.ensure_dirs(self.resolve_output_dir(cfg))
            return cfg

        except Exception as exc:
            log.warning("config_load_failed_using_defaults", extra={"error": str(exc), "path": str(self._config_path)})
            cfg = AppConfig(output_dir=None)
            self.ensure_dirs(self.default_output_dir)
            return cfg

    def save(self, cfg: AppConfig) -> None:
        self._base_dir.mkdir(parents=True, exist_ok=True)
        out_dir = self.resolve_output_dir(cfg)
        self.ensure_dirs(out_dir)

        payload = {"output_dir": str(cfg.output_dir) if cfg.output_dir else None}
        self._config_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

        log.info("config_saved", extra={"path": str(self._config_path), "output_dir": str(out_dir)})

    def resolve_output_dir(self, cfg: AppConfig) -> Path:
        if cfg.output_dir is None:
            return self.default_output_dir
        return cfg.output_dir


def default_documents_dir() -> Path:
    home = Path.home()
    docs = home / "Documents"
    return docs if docs.exists() else home  # fallback for unusual setups


def default_base_dir() -> Path:
    return default_documents_dir() / "PyForecast"


def _as_path(v: Any) -> Path | None:
    if v is None:
        return None
    try:
        p = Path(str(v)).expanduser()
        return p
    except Exception:
        return None