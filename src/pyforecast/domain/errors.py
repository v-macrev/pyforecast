from __future__ import annotations


class PyForecastError(Exception):
    """Base exception for all domain-level errors in PyForecast."""


class FileFormatError(PyForecastError):
    """Raised when the input file format is unsupported or malformed."""


class SchemaInferenceError(PyForecastError):
    """Raised when we cannot reliably infer schema (date/value columns, shape)."""


class DateInferenceError(PyForecastError):
    """Raised when no valid date column can be detected or parsed."""


class FrequencyInferenceError(PyForecastError):
    """Raised when time frequency cannot be determined."""


class TransformationError(PyForecastError):
    """Raised when wide/long transformation fails."""


class ForecastError(PyForecastError):
    """Raised when forecasting (Prophet) fails."""