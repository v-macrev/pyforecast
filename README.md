# PyForecast

Desktop forecasting system that ingests Excel/CSV data, detects dataset shape + time frequency, normalises to a canonical long format, and runs Prophet-based time series forecasts. :contentReference[oaicite:4]{index=4}

> **Status:** early stage (v0.0.1) — APIs and UI may change.

---

## What PyForecast does

PyForecast is built around a practical workflow:

1. **Ingest**: load a dataset (Excel/CSV) into the app.
2. **Profile**: detect the dataset “shape” (long vs wide) and infer the time frequency (daily/weekly/monthly, etc.).
3. **Map**: choose which columns represent date/value and what composes your entity key.
4. **Transform**: reshape the dataset into a canonical time-series format.
5. **Forecast**: produce forecasts using **Prophet** (optional extra). :contentReference[oaicite:5]{index=5}

---

## Canonical output format

After transformation, PyForecast aims to normalise data to a standard long-format time series:

- `cd_key` — entity identifier (built from one or more columns)
- `ds` — timestamp/date column
- `y` — numeric target value

This makes forecasting consistent, repeatable, and model-agnostic.

---

## Installation

### Requirements

- Python **3.10+** :contentReference[oaicite:6]{index=6}

### 1) Create and activate a virtual environment (recommended)

**Windows (PowerShell):**
```powershell
python -m venv .env
.env\Scripts\Activate.ps1
python -m pip install --upgrade pip
````

### 2) Install the project

Minimal install (UI base):

```bash
pip install -e .
```

Recommended install (data + excel + forecasting):

```bash
pip install -e ".[data,excel,forecast]"
```

Build tooling (PyInstaller):

```bash
pip install -e ".[build]"
```

**Extras available** ([GitHub][2]):

* `data`: polars, duckdb, pyarrow
* `excel`: python-calamine
* `forecast`: prophet
* `build`: pyinstaller
* `dev`: pytest, ruff, mypy
* `all`: `pyforecast[data,excel,forecast]`

---

## Running the app

PyInstaller builds from `src/pyforecast/main.py`. ([GitHub][3])

Run the desktop app:

```bash
python -m pyforecast.main
```

---

## Building a Windows executable

PyForecast includes a PyInstaller spec file at the repository root: `PyForecast.spec`. ([GitHub][5])

Local build:

```bash
pyinstaller --noconfirm --clean PyForecast.spec
```

Output will be under:

```text
dist/PyForecast/
```

---

## Releases (GitHub Actions)

This repository is set up to build and publish Windows releases via GitHub Actions when you push a version tag (e.g. `v0.1.1`).

Example:

```bash
git tag v0.1.1
git push origin v0.1.1
```

---

## Windows SmartScreen / “Unknown publisher”

If you distribute an unsigned `.exe`, Windows may warn users (“unknown publisher” / SmartScreen).

To remove “Unknown publisher” for general users, you need to **code sign** the executable with a certificate from a trusted CA.
(Workarounds like self-signed certs only help on machines where the cert is installed.)

---

## Development

Install dev tools:

```bash
pip install -e ".[dev]"
```

Run tests:

```bash
pytest
```

Lint / format (ruff is configured in `pyproject.toml`): ([GitHub][2])

```bash
ruff check .
ruff format .
```

Type check:

```bash
mypy src
```

---

## Project structure (high level)

PyForecast is organised as a desktop app plus services for profiling, transformation, and forecasting:

* `src/pyforecast/` — application entrypoint and packages
* `.github/workflows/` — CI/CD (release builds)
* `PyForecast.spec` — PyInstaller build configuration ([GitHub][3])
* `tests/` — automated tests (pytest)

---

## Licence

This project is licensed under the **GNU Affero General Public License v3.0 (AGPL-3.0)**. ([GitHub][1])

---

## Notes / known metadata mismatch

Your `pyproject.toml` currently declares `license = { text = "Proprietary" }`, but the repository includes an AGPL-3.0 `LICENSE` file. ([GitHub][2])

If you want metadata to be consistent, update `pyproject.toml` accordingly (I can propose the exact patch).

```

### One important fix you probably want
Right now your `pyproject.toml` says **Proprietary**, while your repo’s actual licence is **AGPL-3.0**. :contentReference[oaicite:14]{index=14}  
If you want, I’ll give you the exact `pyproject.toml` edit + commit title/description to make that consistent (no questions needed).
::contentReference[oaicite:15]{index=15}
```

[1]: https://raw.githubusercontent.com/v-macrev/pyforecast/main/LICENSE "raw.githubusercontent.com"
[2]: https://raw.githubusercontent.com/v-macrev/pyforecast/main/pyproject.toml "raw.githubusercontent.com"
[3]: https://raw.githubusercontent.com/v-macrev/pyforecast/main/PyForecast.spec "raw.githubusercontent.com"
[4]: https://raw.githubusercontent.com/v-macrev/pyforecast/main/README.md "raw.githubusercontent.com"
[5]: https://github.com/v-macrev/pyforecast "GitHub - v-macrev/pyforecast: Desktop forecasting system with schema detection, frequency inference, reshaping, and Prophet-based time series prediction."