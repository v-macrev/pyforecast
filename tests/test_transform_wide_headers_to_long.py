from __future__ import annotations
 
from pathlib import Path
 
import polars as pl
import pytest
 
from pyforecast.application.services.transform_service import TransformRequest, transform_to_canonical_long
from pyforecast.domain.canonical_schema import CANON
 
 
@pytest.mark.parametrize(
    "headers",
    [
        ["2024-01-01", "2024-01-02", "2024-01-03"],
        ["2024-01", "2024-02", "2024-03"],  # monthly headers
        ["20240101", "20240102", "20240103"],  # compact daily
    ],
)
def test_transform_wide_header_dates_to_canonical_long(tmp_path: Path, headers: list[str]) -> None:
    """
    Wide variant (header dates):
      - key columns identify the series (row)
      - date values live in column headers
      - output must be canonical: cd_key, ds, y
      - ds must be non-null and parsed from headers
    """
    # Create a wide dataframe: keys + date-in-header value columns
    data = {
        "sku": ["A", "B"],
        "store": ["1", "1"],
        headers[0]: [10, 5],
        headers[1]: [11, 6],
        headers[2]: [12, 7],
    }
    df = pl.DataFrame(data)
 
    in_path = tmp_path / "wide_headers.csv"
    df.write_csv(in_path)
 
    req = TransformRequest(
        path=in_path,
        file_type="csv",
        shape="wide",
        # In header-wide mode this might be meaningless; we still pass a value.
        # The service should still correctly parse ds from header names.
        date_col="date",
        value_col=None,
        key_parts=["store", "sku"],
        key_separator="|",
        out_dir=tmp_path,
        out_format="parquet",
    )
 
    res = transform_to_canonical_long(req)
    assert res.output_path.exists()
 
    out = pl.read_parquet(res.output_path)
 
    # Canonical columns exist
    assert set(out.columns) == {CANON.cd_key, CANON.ds, CANON.y}
 
    # ds should be parsed (no nulls for these 3 periods * 2 series = 6 rows)
    assert out.height == 6
    assert out[CANON.ds].null_count() == 0
 
    # cd_key should be store|sku (2 unique keys)
    assert set(out[CANON.cd_key].unique().to_list()) == {"1|A", "1|B"}
 
    # y should contain all input values (order not guaranteed)
    assert sorted(out[CANON.y].to_list()) == sorted([10, 11, 12, 5, 6, 7])
 
 
def test_transform_wide_header_dates_ignores_key_columns(tmp_path: Path) -> None:
    """
    Ensures we do NOT melt key columns as dates.
    """
    df = pl.DataFrame(
        {
            "sku": ["A"],
            "store": ["1"],
            "2024-01-01": [10],
            "2024-01-02": [11],
        }
    )
    in_path = tmp_path / "wide_headers.csv"
    df.write_csv(in_path)
 
    req = TransformRequest(
        path=in_path,
        file_type="csv",
        shape="wide",
        date_col="date",
        value_col=None,
        key_parts=["sku", "store"],
        out_dir=tmp_path,
        out_format="parquet",
    )
    res = transform_to_canonical_long(req)
 
    out = pl.read_parquet(res.output_path)
 
    # Only 2 melted periods => 2 rows
    assert out.height == 2
    assert out[CANON.ds].null_count() == 0
    assert set(out[CANON.cd_key].unique().to_list()) == {"A|1"}