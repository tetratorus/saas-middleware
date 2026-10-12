import json
import os
import re
import shutil
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


# adapter that, like DUCT's, finds upstream code relative to itself (repo/rewire/<id>/adapter.py -> repo/lib)
REPO_ADAPTER = """import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "lib"))
from science import gfa


def run(inputs, params):
    df = inputs["plots"].copy()
    df["gfa_m2"] = gfa(df.area_m2, params["ratio"])
    return {"plot_capacity": df[["plot_id", "gfa_m2"]]}
"""


@pytest.fixture
def transform(tmp_path):
    d = tmp_path / "demo"
    d.mkdir()
    (d / "transform.yaml").write_text(TRANSFORM)
    (d / "adapter.py").write_text(ADAPTER)
    return d / "transform.yaml"


@pytest.fixture
def repo_transform(tmp_path):
    repo = tmp_path / "model-repo"
    d = repo / "rewire" / "demo.plot_capacity"
    (d / "golden").mkdir(parents=True)
    (repo / "lib").mkdir()
    (d / "transform.yaml").write_text(TRANSFORM)
    (d / "adapter.py").write_text(REPO_ADAPTER)
    (d / "golden" / "big.parquet").write_bytes(b"x")
    (repo / "lib" / "science.py").write_text("def gfa(area, ratio):\n    return area * ratio\n")
    (repo / "lib" / "requirements.txt").write_text("igraph~=0.11.6\n")
    return repo, d / "transform.yaml"


def _wd(tmp_path, name="wd"):
    wd = tmp_path / name
    wd.mkdir()
    pd.DataFrame({"plot_id": ["p1", "p2"], "area_m2": [100.0, 50.0]}).to_parquet(wd / "plots")
    (wd / "parameters").write_text(json.dumps({"ratio": 3.0}))
    return wd


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
    assert {p.name for p in proc.iterdir()} == {"descriptor.json", "processor.py", "execute.sh", "install.sh",
                                                "spine_backend.py", "src"}
    ProcessorDescriptor.parse_obj(json.loads((proc / "descriptor.json").read_text()))
    wd = _wd(tmp_path)
    # what execute.sh does after activating the venv that install.sh builds
    res = subprocess.run([sys.executable, "processor.py", str(wd)], cwd=proc, capture_output=True, text=True, check=False)
    assert res.returncode == 0, res.stderr
    assert "trigger:output:plot_capacity" in res.stdout and "trigger:progress:100" in res.stdout
    assert "inner-model-object" not in res.stdout and "inner-model-object" in res.stderr
    out = pd.read_parquet(wd / "plot_capacity").sort_values("plot_id")
    assert out.gfa_m2.tolist() == [300.0, 150.0]


def test_missing_input_fails(transform, tmp_path):
    assert spine_backend.run(transform, tmp_path) == 1


def test_export_is_relocatable(repo_transform, tmp_path):
    repo, yaml_path = repo_transform
    proc = spine_backend.export(yaml_path, tmp_path / "proc", root=repo, include=["lib"],
                                requirements=["lib/requirements.txt"], extra_pip="'numpy==1.23.5'")
    files = sorted(p.relative_to(proc).as_posix() for p in proc.rglob("*") if p.is_file())
    assert files == ["descriptor.json", "execute.sh", "install.sh", "processor.py", "spine_backend.py",
                     "src/lib/requirements.txt", "src/lib/science.py",
                     "src/rewire/demo.plot_capacity/adapter.py", "src/rewire/demo.plot_capacity/transform.yaml"]
    install = (proc / "install.sh").read_text()
    assert spine_backend.SPINE_PIP in install and "-r src/lib/requirements.txt" in install and "numpy==1.23.5" in install
    # no machine-local path anywhere in the bundle
    local = [str(tmp_path), str(Path.home()), str(Path(spine_backend.__file__).resolve().parent)]
    abs_literal = re.compile(r"""["'](/|[A-Za-z]:\\)[^"']*["']""")
    for f in files:
        text = (proc / f).read_text()
        assert not any(p in text for p in local), f
        if f.endswith(".py") and f != "spine_backend.py":
            assert not abs_literal.search(text), f
    # move it to another host-like location, remove the source tree, run it the way execute.sh does
    deployed = tmp_path / "elsewhere" / "rti" / "procs" / "spine-demo"
    deployed.parent.mkdir(parents=True)
    shutil.move(str(proc), deployed)
    shutil.rmtree(repo)
    wd = _wd(tmp_path)
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    res = subprocess.run([sys.executable, str(deployed / "processor.py"), str(wd)], cwd=tmp_path / "elsewhere",
                       capture_output=True, text=True, check=False, env=env)
    assert res.returncode == 0, res.stderr
    assert "trigger:output:plot_capacity" in res.stdout
    assert pd.read_parquet(wd / "plot_capacity").sort_values("plot_id").gfa_m2.tolist() == [300.0, 150.0]


def test_export_refuses_paths_outside_root(repo_transform, tmp_path):
    repo, yaml_path = repo_transform
    with pytest.raises(ValueError, match="outside"):
        spine_backend.export(yaml_path, tmp_path / "proc", root=repo, include=["../escape"])


def test_export_does_not_clobber_unrelated_dir(transform, tmp_path):
    (tmp_path / "busy").mkdir()
    (tmp_path / "busy" / "keep.txt").write_text("x")
    with pytest.raises(FileExistsError):
        spine_backend.export(transform, tmp_path / "busy")
