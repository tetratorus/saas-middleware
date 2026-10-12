"""Transform descriptors (transform.yaml) and the runner.

Every model, conformer and workflow step is a process with typed inputs/outputs (OGC API Processes style).
The canonical execution contract is a directory contract, so any language/runtime can implement it:
  {inputs}/<input_name>.parquet   (bronze inputs: {inputs}/<input_name>/ directory)
  {params}                        (params.json)
  {outputs}/<output_name>.parquet (written by the model)
The python runtime is an in-process shortcut with the same signature: fn(inputs, params) -> {name: DataFrame}.
"""
from __future__ import annotations

import datetime as dt
import importlib
import json
import os
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import jsonschema
import pandas as pd
import yaml

from .lake import BASELINE, Lake, read_table, sha256_file, write_table
from .lineage import run_event
from .spec import transform_schema
from .validate import check_gold, check_silver


class NotRunnable(RuntimeError):
    pass


@dataclass
class Descriptor:
    data: dict
    path: Path

    @property
    def id(self) -> str:
        return self.data["id"]

    @property
    def dir(self) -> Path:
        return self.path.parent

    @property
    def kind(self) -> str:
        return self.data["kind"]

    def gold_deps(self) -> list[str]:
        return [i["source"].split(":", 1)[1].split("/")[0] for i in self.data["inputs"] if i["source"].startswith("gold:")]


def load_descriptor(path: os.PathLike | str) -> Descriptor:
    p = Path(path).resolve()
    if p.is_dir():
        p = p / "transform.yaml"
    data = yaml.safe_load(p.read_text())
    jsonschema.validate(data, transform_schema())
    if data["kind"] == "conformer":
        bad = [i["source"] for i in data["inputs"] if not i["source"].startswith("bronze:")]
        if bad:
            raise jsonschema.ValidationError(f"{data['id']}: conformer inputs must be bronze:, got {bad}")
    else:
        bad = [i["source"] for i in data["inputs"] if i["source"].startswith("bronze:")]
        if bad:
            raise jsonschema.ValidationError(f"{data['id']}: transforms read silver/gold, never bronze: {bad}")
    rt = data["runtime"]
    need = {"python": "entrypoint", "subprocess": "command", "docker": "image"}.get(rt["type"])
    if need and need not in rt:
        raise jsonschema.ValidationError(f"{data['id']}: runtime '{rt['type']}' requires '{need}'")
    return Descriptor(data, p)


def discover(paths: Optional[list[str]] = None) -> dict[str, Descriptor]:
    roots = paths or [p for p in os.environ.get("SPINE_PATH", ".").split(os.pathsep) if p]
    found: dict[str, Descriptor] = {}
    for root in roots:
        for p in sorted(Path(root).rglob("transform.yaml")):
            if any(part in {".git", "node_modules", ".venv", "golden"} for part in p.parts):
                continue
            d = load_descriptor(p)
            if d.id in found and found[d.id].path != d.path:
                raise ValueError(f"duplicate transform id {d.id}: {found[d.id].path} and {d.path}")
            found[d.id] = d
    return found


def resolve_params(desc: Descriptor, params: Optional[dict]) -> dict:
    schema = desc.data.get("parameters") or {"type": "object"}
    out = {}
    for k, v in (schema.get("properties") or {}).items():
        if "default" in v:
            out[k] = v["default"]
    out.update(params or {})
    jsonschema.validate(out, schema)
    return out


def invoke(desc: Descriptor, inputs: dict[str, Any], params: dict) -> dict[str, pd.DataFrame]:
    """Run the model on in-memory inputs (DataFrames, or Paths for bronze inputs)."""
    rt = desc.data["runtime"]
    if rt["type"] == "external":
        raise NotRunnable(f"{desc.id} runs outside the spine: {rt.get('notes', 'see descriptor')}")
    if rt["type"] == "python":
        mod_name, fn_name = rt["entrypoint"].split(":")
        for extra in reversed([desc.dir, *[(desc.dir / p).resolve() for p in rt.get("pythonpath", [])]]):
            while str(extra) in sys.path:
                sys.path.remove(str(extra))
            sys.path.insert(0, str(extra))
        importlib.invalidate_caches()
        top = mod_name.split(".")[0]
        cached = sys.modules.get(top)
        if cached is not None and not str(getattr(cached, "__file__", "") or "").startswith(str(desc.dir)):
            for m in [m for m in sys.modules if m == top or m.startswith(top + ".")]:
                del sys.modules[m]
        fn = getattr(importlib.import_module(mod_name), fn_name)
        result = fn(inputs, params)
    else:
        result = _invoke_dir_contract(desc, inputs, params)
    expected = {o["name"] for o in desc.data["outputs"]}
    missing = expected - set(result)
    if missing:
        raise RuntimeError(f"{desc.id} did not produce outputs {sorted(missing)}")
    return {k: result[k] for k in expected}


def _invoke_dir_contract(desc: Descriptor, inputs: dict[str, Any], params: dict) -> dict[str, pd.DataFrame]:
    rt = desc.data["runtime"]
    with tempfile.TemporaryDirectory(prefix=f"spine-{desc.id}-") as tmp:
        tmp = Path(tmp)
        idir, odir, pfile = tmp / "inputs", tmp / "outputs", tmp / "params.json"
        idir.mkdir(); odir.mkdir()
        for name, val in inputs.items():
            if isinstance(val, Path):
                (idir / name).symlink_to(val.resolve(), target_is_directory=True)
            elif val is not None:
                write_table(val, idir / f"{name}.parquet")
        pfile.write_text(json.dumps(params, default=str))
        if rt["type"] == "subprocess":
            subst = {"{inputs}": str(idir), "{outputs}": str(odir), "{params}": str(pfile)}
            argv = [sys.executable if a == "python" else _subst(a, subst) for a in rt["command"]]
            cwd = (desc.dir / rt.get("workdir", ".")).resolve()
        else:  # docker
            subst = {"{inputs}": "/spine/inputs", "{outputs}": "/spine/outputs", "{params}": "/spine/params.json"}
            argv = ["docker", "run", "--rm", "-v", f"{tmp}:/spine", "-v", f"{idir}:/spine/inputs:ro"]
            for name, val in inputs.items():
                if isinstance(val, Path):
                    argv += ["-v", f"{val.resolve()}:/spine/inputs/{name}:ro"]
            argv += [rt["image"]] + [_subst(a, subst) for a in rt.get("command", [])]
            cwd = None
        proc = subprocess.run(argv, cwd=cwd, capture_output=True, text=True)
        if proc.returncode != 0:
            raise RuntimeError(f"{desc.id} failed ({proc.returncode}):\n{proc.stdout[-4000:]}\n{proc.stderr[-4000:]}")
        return {p.stem: read_table(p) for p in odir.glob("*.parquet")}


def _subst(arg: str, subst: dict) -> str:
    for k, v in subst.items():
        arg = arg.replace(k, v)
    return arg


def execute(lake: Lake, desc: Descriptor, *, scenario: str = BASELINE, params: Optional[dict] = None) -> dict:
    """Resolve inputs from the lake, run, check expectations, write Silver/Gold + lineage. Returns the run event."""
    started = dt.datetime.now(dt.timezone.utc)
    params = resolve_params(desc, params)
    if desc.kind == "conformer" and scenario != BASELINE:
        raise ValueError("conformers build the baseline silver layer; use scenarios to change it")
    inputs, lineage_in = {}, []
    for spec in desc.data["inputs"]:
        layer, ref = spec["source"].split(":", 1)
        try:
            if layer == "bronze":
                source, _, version = ref.partition("/")
                path = lake.bronze_dir(source, version or None)
                inputs[spec["name"]] = path
                m = json.loads((path / "manifest.json").read_text())
                lineage_in.append({"namespace": "lake.bronze", "name": f"{source}/{path.name}",
                                   "facets": {"manifest": {"source_url": m["source_url"], "license": m["license"],
                                                           "files": len(m["files"])}}})
                continue
            if layer == "silver":
                df = lake.silver_read(ref, scenario)
                lineage_in.append({"namespace": "lake.silver", "name": ref,
                                   "facets": {"scenario": scenario, "rows": len(df),
                                              "source_versions": sorted(map(str, df["source_version"].dropna().unique()))
                                              if "source_version" in df.columns else []}})
            else:
                tid, table = ref.split("/")
                df = lake.gold_read(tid, table, scenario)
                lin = lake.gold_dir(scenario, tid) / "_lineage.json"
                lineage_in.append({"namespace": "lake.gold", "name": f"{tid}/{table}",
                                   "facets": {"scenario": scenario, "rows": len(df),
                                              "parentRun": json.loads(lin.read_text())["run"]["runId"] if lin.exists() else None}})
            if spec.get("columns"):
                missing = [c for c in spec["columns"] if c not in df.columns]
                if missing:
                    raise KeyError(f"{desc.id}: input '{spec['name']}' ({spec['source']}) lacks columns {missing}")
            inputs[spec["name"]] = df
        except FileNotFoundError:
            if spec.get("optional"):
                inputs[spec["name"]] = None
                continue
            raise
    outputs = invoke(desc, inputs, params)
    lineage_out = []
    out_specs = {o["name"]: o for o in desc.data["outputs"]}
    if desc.kind == "conformer":
        for name, df in outputs.items():
            p = lake.silver_write(name, df)
            lineage_out.append({"namespace": "lake.silver", "name": name, "facets": {"rows": len(df), "sha256": sha256_file(p)}})
        event = run_event(descriptor=desc.data, descriptor_dir=desc.dir, scenario=scenario, params=params,
                          inputs=lineage_in, outputs=lineage_out, started=started)
        ldir = lake.root / "silver" / "_lineage"
        ldir.mkdir(parents=True, exist_ok=True)
        (ldir / f"{desc.id}.json").write_text(json.dumps(event, indent=2, default=str))
        return event
    gdir = lake.gold_dir(scenario, desc.id)
    for name, df in outputs.items():
        check_gold(name, df, out_specs[name])
        p = gdir / f"{name}.parquet"
        write_table(df, p)
        lineage_out.append({"namespace": "lake.gold", "name": f"{desc.id}/{name}",
                            "facets": {"scenario": scenario, "rows": len(df), "sha256": sha256_file(p)}})
    event = run_event(descriptor=desc.data, descriptor_dir=desc.dir, scenario=scenario, params=params,
                      inputs=lineage_in, outputs=lineage_out, started=started)
    (gdir / "_lineage.json").write_text(json.dumps(event, indent=2, default=str))
    return event


def plan(descs: dict[str, Descriptor], targets: list[str]) -> list[str]:
    """Topological order of transforms needed to build `targets` (gold dependencies first)."""
    order, seen = [], set()

    def visit(tid: str, stack: tuple = ()):
        if tid in stack:
            raise ValueError(f"cycle: {' -> '.join(stack + (tid,))}")
        if tid in seen:
            return
        if tid not in descs:
            raise KeyError(f"unknown transform '{tid}'")
        for dep in descs[tid].gold_deps():
            visit(dep, stack + (tid,))
        seen.add(tid)
        order.append(tid)

    for t in targets:
        visit(t)
    return order


def run_workflow(lake: Lake, descs: dict[str, Descriptor], workflow: dict, scenario: Optional[str] = None) -> list[dict]:
    """workflow.yaml: {name, scenario?, steps: [{transform: id, params: {...}}]}; dependencies are added automatically."""
    scenario = scenario or workflow.get("scenario", BASELINE)
    step_params = {s["transform"]: s.get("params", {}) for s in workflow["steps"]}
    events = []
    for tid in plan(descs, [s["transform"] for s in workflow["steps"]]):
        events.append(execute(lake, descs[tid], scenario=scenario, params=step_params.get(tid)))
    return events
