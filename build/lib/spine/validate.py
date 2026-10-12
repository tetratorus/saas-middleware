"""Data-quality expectations (DLT-style) for silver and gold tables."""
from __future__ import annotations

import pandas as pd

from .spec import SILVER_CRS, silver_table

_KIND = {
    "string": lambda s: pd.api.types.is_string_dtype(s) or pd.api.types.is_object_dtype(s),
    "int": pd.api.types.is_integer_dtype,
    "float": lambda s: pd.api.types.is_float_dtype(s) or pd.api.types.is_integer_dtype(s),
    "bool": pd.api.types.is_bool_dtype,
    "timestamp": pd.api.types.is_datetime64_any_dtype,
    "geometry": lambda s: getattr(s, "geom_type", None) is not None or s.dtype.name == "geometry",
}


class ExpectationError(ValueError):
    pass


def check_columns(df: pd.DataFrame, columns: dict, key: list[str], where: str) -> None:
    errors = []
    for col, meta in columns.items():
        if col not in df.columns:
            if meta.get("required"):
                errors.append(f"missing required column '{col}'")
            continue
        s = df[col]
        if meta.get("required") and s.isna().any():
            errors.append(f"column '{col}' has {int(s.isna().sum())} nulls but is required")
        nonnull = s.dropna()
        if len(nonnull) and not _KIND[meta["type"]](s if meta["type"] == "geometry" else nonnull):
            if not (meta["type"] == "int" and pd.api.types.is_float_dtype(nonnull) and (nonnull % 1 == 0).all()):
                errors.append(f"column '{col}' should be {meta['type']}, got {s.dtype}")
    if key and all(k in df.columns for k in key) and df.duplicated(key).any():
        errors.append(f"duplicate key {key}: {int(df.duplicated(key).sum())} rows")
    if errors:
        raise ExpectationError(f"{where}: " + "; ".join(errors))


def check_silver(table: str, df: pd.DataFrame) -> None:
    spec = silver_table(table)
    check_columns(df, spec["columns"], spec.get("key", []), f"silver.{table}")
    if "geometry" in spec["columns"]:
        crs = getattr(df, "crs", None)
        if crs is None or crs.to_string() != SILVER_CRS:
            raise ExpectationError(f"silver.{table}: geometry CRS must be {SILVER_CRS}, got {crs}")


def check_gold(name: str, df: pd.DataFrame, out_spec: dict) -> None:
    cols = {c["name"]: {"type": c["type"], "required": True} for c in out_spec.get("columns", [])}
    check_columns(df, cols, out_spec.get("key", []), f"gold.{name}")
