import json
import threading
import urllib.request
from pathlib import Path

import jsonschema
import pandas as pd
import pytest

from spine import Lake, discover, execute, load_descriptor, plan, run_workflow
from spine.golden import run_golden
from spine.lake import AppendOnlyError
from spine.validate import ExpectationError

ROOT = Path(__file__).resolve().parents[1]
TOY = ROOT / "examples" / "toy"


@pytest.fixture
def lake(tmp_path):
    lk = Lake(tmp_path / "lake")
    lk.bronze_add("toy_plots", "v1", [TOY / "bronze" / "plots.csv"], source_url="file://toy", license="MIT")
    execute(lk, load_descriptor(TOY / "conform_plots"))
    return lk


def test_bronze_is_append_only(lake, tmp_path):
    again = lake.bronze_add("toy_plots", "v1", [TOY / "bronze" / "plots.csv"], source_url="file://toy", license="MIT")
    assert again["version"] == "v1"
    changed = tmp_path / "plots.csv"
    changed.write_text((TOY / "bronze" / "plots.csv").read_text().replace("2.8", "3.0"))
    with pytest.raises(AppendOnlyError):
        lake.bronze_add("toy_plots", "v1", [changed], source_url="file://toy", license="MIT")
    lake.bronze_add("toy_plots", "v2", [changed], source_url="file://toy", license="MIT")
    assert lake.bronze_versions("toy_plots") == ["v1", "v2"]


def test_conformer_writes_valid_silver_with_lineage(lake):
    plots = lake.silver_read("plots")
    assert plots.crs.to_epsg() == 3414 and set(plots.plot_id) == {"P1", "P2", "P3"}
    ev = json.loads((lake.root / "silver" / "_lineage" / "example.conform_plots.json").read_text())
    assert ev["inputs"][0]["name"] == "toy_plots/v1"


def test_silver_expectations_reject_bad_data(lake):
    bad = pd.DataFrame({"plot_id": ["P1", "P1"], "zone_type": ["A", "B"], "source_version": ["x", "x"]})
    with pytest.raises(ExpectationError, match="duplicate key"):
        lake.silver_write("zoning", bad)


def test_transform_gold_and_scenario_branch(lake):
    d = load_descriptor(TOY / "plot_capacity")
    execute(lake, d)
    base = lake.gold_read("example.plot_capacity", "plot_capacity").set_index("plot_id").gfa_m2
    assert base["P1"] == pytest.approx(28000)
    lake.scenario_create("rezone_p1", description="P1 to GPR 3.5")
    lake.scenario_set("rezone_p1", "zoning", pd.DataFrame([{"plot_id": "P1", "gross_plot_ratio": 3.5}]))
    assert lake.silver_read("zoning").set_index("plot_id").gross_plot_ratio["P1"] == 2.8  # baseline untouched
    ev = execute(lake, d, scenario="rezone_p1")
    alt = lake.gold_read("example.plot_capacity", "plot_capacity", "rezone_p1").set_index("plot_id").gfa_m2
    assert alt["P1"] == pytest.approx(35000) and alt["P2"] == base["P2"]
    assert ev["run"]["facets"]["scenario"]["name"] == "rezone_p1"
    assert ev["inputs"][1]["facets"]["source_versions"] == ["v1"]


def test_nested_scenario_and_delete(lake):
    lake.scenario_create("a")
    lake.scenario_set("a", "zoning", pd.DataFrame([{"plot_id": "P1", "gross_plot_ratio": 3.0}]))
    lake.scenario_create("b", parent="a")
    lake.scenario_set("b", "zoning", pd.DataFrame([{"plot_id": "P2", "_deleted": True}]))
    z = lake.silver_read("zoning", "b").set_index("plot_id")
    assert z.gross_plot_ratio["P1"] == 3.0 and "P2" not in z.index


def test_golden_pass_and_fail(tmp_path):
    d = load_descriptor(TOY / "plot_capacity")
    assert all(r.passed for r in run_golden(d))
    d.data["golden"][0]["path"] = str(TOY / "plot_capacity" / "golden" / "basic")
    d.data["parameters"]["properties"]["efficiency"]["default"] = 0.9
    res = run_golden(d)
    assert not res[0].passed and "outside tolerance" in res[0].messages[0]


def test_descriptor_rules():
    d = load_descriptor(TOY / "plot_capacity").data
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate({**d, "id": "NoNamespace"}, __import__("spine.spec").spec.transform_schema())
    import yaml, tempfile
    with tempfile.TemporaryDirectory() as t:
        p = Path(t) / "transform.yaml"
        p.write_text(yaml.safe_dump({**d, "inputs": [{"name": "x", "source": "bronze:raw"}]}))
        with pytest.raises(jsonschema.ValidationError, match="never bronze"):
            load_descriptor(p)


def test_subprocess_runtime(lake, tmp_path):
    tdir = tmp_path / "sub"
    tdir.mkdir()
    (tdir / "model.py").write_text(
        "import sys, json, pandas as pd\n"
        "i, o, p = sys.argv[1:]\n"
        "z = pd.read_parquet(i + '/zoning.parquet')\n"
        "z['double'] = z.gross_plot_ratio.fillna(0) * 2 * json.load(open(p))['k']\n"
        "z[['plot_id', 'double']].to_parquet(o + '/doubled.parquet')\n")
    (tdir / "transform.yaml").write_text(
        "id: example.sub\nversion: 0.1.0\nkind: transform\ntitle: sub\n"
        "upstream: {repo: x, license: MIT}\n"
        "runtime: {type: subprocess, command: [python, model.py, '{inputs}', '{outputs}', '{params}']}\n"
        "inputs: [{name: zoning, source: 'silver:zoning'}]\n"
        "parameters: {type: object, properties: {k: {type: number, default: 1}}}\n"
        "outputs: [{name: doubled, key: [plot_id], columns: [{name: double, type: float}]}]\n")
    execute(lake, load_descriptor(tdir), params={"k": 2})
    out = lake.gold_read("example.sub", "doubled").set_index("plot_id").double
    assert out["P2"] == pytest.approx(16.8)


def test_workflow_plans_gold_dependencies(lake, tmp_path):
    tdir = tmp_path / "chain"
    tdir.mkdir()
    (tdir / "adapter.py").write_text(
        "def run(inputs, params):\n    df = inputs['cap'].copy(); df['gfa_k'] = df.gfa_m2 / 1000\n"
        "    return {'cap_k': df[['plot_id', 'gfa_k']]}\n")
    (tdir / "transform.yaml").write_text(
        "id: example.chain\nversion: 0.1.0\nkind: transform\ntitle: chain\nupstream: {repo: x, license: MIT}\n"
        "runtime: {type: python, entrypoint: 'adapter:run'}\n"
        "inputs: [{name: cap, source: 'gold:example.plot_capacity/plot_capacity'}]\n"
        "outputs: [{name: cap_k, key: [plot_id]}]\n")
    descs = discover([str(TOY), str(tdir)])
    assert plan(descs, ["example.chain"]) == ["example.plot_capacity", "example.chain"]
    evs = run_workflow(lake, descs, {"steps": [{"transform": "example.chain"}]})
    assert evs[1]["inputs"][0]["facets"]["parentRun"] == evs[0]["run"]["runId"]


def test_serve_ogc_features(lake):
    from http.server import ThreadingHTTPServer
    from spine.serve import make_handler
    execute(lake, load_descriptor(TOY / "plot_capacity"))
    srv = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(lake))
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{srv.server_address[1]}"
    cols = json.load(urllib.request.urlopen(base + "/collections"))["collections"]
    assert "gold.example.plot_capacity.plot_capacity" in [c["id"] for c in cols]
    fc = json.load(urllib.request.urlopen(base + "/collections/gold.example.plot_capacity.plot_capacity/items"))
    assert fc["numberReturned"] == 3 and fc["features"][0]["geometry"]["type"] == "Polygon"
    srv.shutdown()
