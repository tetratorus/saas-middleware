"""Golden tests: freeze the science before rewiring it.

Each golden case directory (relative to transform.yaml):
  inputs/<input>.parquet   or  inputs/<input>/   (bronze inputs for conformers)
  params.json              (optional)
  expected/<output>.parquet
Outputs are compared after sorting by the output key; numeric columns within rtol/atol.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

from .lake import read_table
from .transform import Descriptor, invoke, resolve_params


@dataclass
class GoldenResult:
    transform: str
    case: str
    passed: bool
    messages: list[str] = field(default_factory=list)


def load_case_inputs(desc: Descriptor, case_dir: Path) -> dict:
    inputs = {}
    for spec in desc.data["inputs"]:
        f = case_dir / "inputs" / f"{spec['name']}.parquet"
        d = case_dir / "inputs" / spec["name"]
        if f.exists():
            inputs[spec["name"]] = read_table(f)
        elif d.is_dir():
            inputs[spec["name"]] = d
        elif spec.get("optional"):
            inputs[spec["name"]] = None
        else:
            raise FileNotFoundError(f"golden case {case_dir} lacks input '{spec['name']}'")
    return inputs


def compare(name: str, got: pd.DataFrame, want: pd.DataFrame, key: list[str], tol: dict) -> list[str]:
    msgs = []
    missing = [c for c in want.columns if c not in got.columns]
    if missing:
        return [f"{name}: missing columns {missing}"]
    if len(got) != len(want):
        return [f"{name}: {len(got)} rows, expected {len(want)}"]
    sort = [k for k in key if k in want.columns] or [c for c in want.columns if c != "geometry"][:1]
    got = got.sort_values(sort).reset_index(drop=True)
    want = want.sort_values(sort).reset_index(drop=True)
    for col in want.columns:
        g, w = got[col], want[col]
        ctol = {**{k: v for k, v in tol.items() if k in ("rtol", "atol")}, **tol.get("columns", {}).get(col, {})}
        if col == "geometry" or getattr(w, "geom_type", None) is not None:
            bad = ~g.geom_equals_exact(w, tolerance=ctol.get("atol", 1e-6))
            if bad.any():
                msgs.append(f"{name}.{col}: {int(bad.sum())} geometries differ")
        elif pd.api.types.is_numeric_dtype(w) and not pd.api.types.is_bool_dtype(w):
            ok = np.isclose(g.astype(float), w.astype(float), rtol=ctol.get("rtol", 1e-9), atol=ctol.get("atol", 0.0),
                            equal_nan=True)
            if not ok.all():
                i = int(np.argmin(ok))
                msgs.append(f"{name}.{col}: {int((~ok).sum())} values outside tolerance (first: got {g.iloc[i]!r}, "
                            f"want {w.iloc[i]!r})")
        else:
            ne = ~((g.astype(str) == w.astype(str)) | (g.isna() & w.isna()))
            if ne.any():
                msgs.append(f"{name}.{col}: {int(ne.sum())} values differ")
    return msgs


def run_golden(desc: Descriptor) -> list[GoldenResult]:
    results = []
    out_specs = {o["name"]: o for o in desc.data["outputs"]}
    for case in desc.data.get("golden", []):
        cdir = (desc.dir / case["path"]).resolve()
        try:
            params_file = cdir / "params.json"
            params = resolve_params(desc, json.loads(params_file.read_text()) if params_file.exists() else {})
            got = invoke(desc, load_case_inputs(desc, cdir), params)
            msgs = []
            for out in out_specs:
                ef = cdir / "expected" / f"{out}.parquet"
                if not ef.exists():
                    msgs.append(f"no expected/{out}.parquet")
                    continue
                msgs += compare(out, got[out], read_table(ef), out_specs[out].get("key", []), case.get("tolerance", {}))
            results.append(GoldenResult(desc.id, case["name"], not msgs, msgs))
        except Exception as e:  # report, don't hide
            results.append(GoldenResult(desc.id, case["name"], False, [f"{type(e).__name__}: {e}"]))
    return results


def freeze(desc: Descriptor, case_name: str) -> Path:
    """Record current outputs as expected values for a golden case (use only against the unmodified upstream model)."""
    case = next(c for c in desc.data.get("golden", []) if c["name"] == case_name)
    cdir = (desc.dir / case["path"]).resolve()
    params_file = cdir / "params.json"
    params = resolve_params(desc, json.loads(params_file.read_text()) if params_file.exists() else {})
    got = invoke(desc, load_case_inputs(desc, cdir), params)
    (cdir / "expected").mkdir(parents=True, exist_ok=True)
    for name, df in got.items():
        df.to_parquet(cdir / "expected" / f"{name}.parquet", index=False)
    return cdir / "expected"
