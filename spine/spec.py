"""Load the layer specs: silver table schemas and the transform descriptor schema."""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

import yaml

_HERE = Path(__file__).resolve().parent
_CANDIDATES = [_HERE / "_spec", _HERE.parent / "spec"]


def spec_dir() -> Path:
    for c in _CANDIDATES:
        if (c / "transform.schema.json").exists():
            return c
    raise FileNotFoundError("spine spec directory not found")


@lru_cache()
def transform_schema() -> dict:
    return json.loads((spec_dir() / "transform.schema.json").read_text())


@lru_cache()
def silver_spec() -> dict:
    return yaml.safe_load((spec_dir() / "silver" / "tables.yaml").read_text())


def silver_table(name: str) -> dict:
    tables = silver_spec()["tables"]
    if name not in tables:
        raise KeyError(f"unknown silver table '{name}'. Known: {sorted(tables)}")
    return tables[name]


SILVER_CRS = "EPSG:3414"
