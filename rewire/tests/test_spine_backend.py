import json
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

from saas.dor.schemas import ProcessorDescriptor

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import spine_backend

TRANSFORM = """id: demo.plot_capacity
version: 0.1.0
kind: transform
title: test transform
upstream: {repo: tetratorus/saas-middleware, license: MIT}
runtime: {type: python, entrypoint: "adapter:run"}
inputs:
  - {name: plots, source: "silver:plots", columns: [plot_id, area_m2]}
parameters:
  type: object
  properties:
    ratio: {type: number, default: 2.0}
outputs:
  - name: plot_capacity
    key: [plot_id]
    columns:
      - {name: plot_id, type: string}
      - {name: gfa_m2, type: float}
"""
ADAPTER = """def run(inputs, params):
    print("trigger:output:inner-model-object")
    df = inputs["plots"].copy()
    df["gfa_m2"] = df.area_m2 * params["ratio"]
    return {"plot_capacity": df[["plot_id", "gfa_m2"]]}
"""


@pytest.fixture
def transform(tmp_path):
    d = tmp_path / "demo"
    d.mkdir()
    (d / "transform.yaml").write_text(TRANSFORM)
    (d / "adapter.py").write_text(ADAPTER)
    return d / "transform.yaml"


def test_descriptor_is_valid_saas(transform):
    desc = ProcessorDescriptor.parse_obj(spine_backend.saas_descriptor(transform, ["default", "nscc"]))
    assert desc.name == "spine-demo-plot-capacity"
    assert [i.name for i in desc.input] == ["plots", "parameters"]
    assert desc.input[0].data_type == "spine.silver.plots" and desc.input[0].data_format == "parquet"
    assert desc.output[0].data_type == "spine.gold.demo.plot_capacity.plot_capacity"
    assert desc.configurations == ["default", "nscc"]


def test_external_is_refused(transform):
    transform.write_text(TRANSFORM.replace('{type: python, entrypoint: "adapter:run"}',
                                           '{type: external, notes: "HPC"}'))
    with pytest.raises(ValueError, match="external"):
        spine_backend.saas_descriptor(transform)


def test_exported_processor_runs_saas_wd_contract(transform, tmp_path):
    proc = spine_backend.export(transform, tmp_path / "proc")
    assert {p.name for p in proc.iterdir()} == {"descriptor.json", "processor.py", "execute.sh", "install.sh"}
    ProcessorDescriptor.parse_obj(json.loads((proc / "descriptor.json").read_text()))
    wd = tmp_path / "wd"
    wd.mkdir()
    pd.DataFrame({"plot_id": ["p1", "p2"], "area_m2": [100.0, 50.0]}).to_parquet(wd / "plots")
    (wd / "parameters").write_text(json.dumps({"ratio": 3.0}))
    # what execute.sh does after activating the venv that install.sh builds
    res = subprocess.run([sys.executable, "processor.py", str(wd)], cwd=proc, capture_output=True, text=True, check=False)
    assert res.returncode == 0, res.stderr
    assert "trigger:output:plot_capacity" in res.stdout and "trigger:progress:100" in res.stdout
    assert "inner-model-object" not in res.stdout and "inner-model-object" in res.stderr
    out = pd.read_parquet(wd / "plot_capacity").sort_values("plot_id")
    assert out.gfa_m2.tolist() == [300.0, 150.0]


def test_missing_input_fails(transform, tmp_path):
    assert spine_backend.run(transform, tmp_path) == 1
