"""Dagster software-defined assets generated from transform.yaml descriptors (3DBAG / Mapbox pattern).
Run: SPINE_LAKE=... SPINE_PATH=... dagster dev -m spine.dagster_defs"""
from __future__ import annotations

import os

from dagster import AssetKey, AssetOut, AssetSpec, Definitions, MaterializeResult, multi_asset

from .lake import BASELINE, Lake
from .transform import Descriptor, discover, execute


def _key(layer: str, tid_or_table: str, table: str | None = None) -> AssetKey:
    return AssetKey([layer, *tid_or_table.split("."), *([table] if table else [])])


def _out_key(desc: Descriptor, out: str) -> AssetKey:
    return _key("silver", out) if desc.kind == "conformer" else _key("gold", desc.id, out)


def _in_key(src: str) -> AssetKey:
    layer, ref = src.split(":", 1)
    if layer == "gold":
        tid, table = ref.split("/")
        return _key("gold", tid, table)
    return _key(layer, ref.split("/")[0])


def build_definitions(lake_root=None, paths=None, scenario: str = BASELINE) -> Definitions:
    descs = discover(paths)
    produced = {_out_key(d, o["name"]) for d in descs.values() for o in d.data["outputs"]}
    assets, externals = [], {}
    for desc in descs.values():
        deps = [_in_key(i["source"]) for i in desc.data["inputs"]]
        for k in deps:
            if k not in produced:
                externals[k] = AssetSpec(k, group_name=k.path[0], description="source / produced outside this code location")
        outs = {o["name"]: AssetOut(key=_out_key(desc, o["name"]), group_name="silver" if desc.kind == "conformer" else "gold",
                                    description=desc.data["title"], is_required=False,
                                    metadata={"transform": desc.id, "version": desc.data["version"],
                                              "license": desc.data["upstream"]["license"],
                                              "readiness_level": desc.data.get("readiness", {}).get("level")})
                for o in desc.data["outputs"]}

        def make(desc=desc, outs=outs, deps=deps):
            @multi_asset(name=desc.id.replace(".", "__"), outs=outs, deps=deps, can_subset=False,
                         compute_kind=desc.data["runtime"]["type"])
            def _asset(context):
                ev = execute(Lake(lake_root), desc, scenario=scenario)
                for o in ev["outputs"]:
                    name = o["name"].split("/")[-1]
                    yield MaterializeResult(asset_key=_out_key(desc, name),
                                            metadata={"rows": o["facets"]["rows"], "sha256": o["facets"]["sha256"],
                                                      "run_id": ev["run"]["runId"], "scenario": scenario})
            return _asset
        assets.append(make())
    return Definitions(assets=[*externals.values(), *assets])


if os.environ.get("SPINE_PATH"):
    defs = build_definitions(os.environ.get("SPINE_LAKE"), os.environ["SPINE_PATH"].split(os.pathsep),
                             os.environ.get("SPINE_SCENARIO", BASELINE))
