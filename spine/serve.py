"""View layer: serve Silver and Gold as GeoJSON over a minimal OGC API - Features interface.

  GET /                         landing
  GET /conformance
  GET /collections              silver.<table> and gold.<transform_id>.<table>
  GET /collections/{id}/items?scenario=<name>&limit=<n>   GeoJSON FeatureCollection (EPSG:4326)
  GET /scenarios
  GET /lineage/{scenario}/{transform_id}
Gold tables without geometry are joined to Silver geometry through their hub key (plot_id/building_id/district_id).
"""
from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import geopandas as gpd
import pandas as pd

from .lake import BASELINE, Lake

HUB_GEOMETRY = [("plot_id", "plots"), ("building_id", "buildings"), ("district_id", "districts"), ("cell_id", "land_cover"),
                ("road_id", "roads")]


def collections(lake: Lake) -> list[dict]:
    out = []
    sdir = lake.root / "silver"
    if sdir.exists():
        for p in sorted(sdir.glob("*.parquet")):
            out.append({"id": f"silver.{p.stem}", "title": f"Silver: {p.stem}", "layer": "silver"})
    seen = set()
    for scenario, tid, table in lake.gold_tables():
        cid = f"gold.{tid}.{table}"
        if cid not in seen:
            seen.add(cid)
            out.append({"id": cid, "title": f"Gold: {tid} / {table}", "layer": "gold"})
    return out


def features(lake: Lake, cid: str, scenario: str = BASELINE, limit: int = 10000) -> dict:
    layer, rest = cid.split(".", 1)
    if layer == "silver":
        df = lake.silver_read(rest, scenario)
    elif layer == "gold":
        tid, table = rest.rsplit(".", 1)
        df = lake.gold_read(tid, table, scenario)
    else:
        raise KeyError(cid)
    if not isinstance(df, gpd.GeoDataFrame):
        for key, table in HUB_GEOMETRY:
            if key in df.columns:
                try:
                    geo = lake.silver_read(table, scenario)[[key, "geometry"]]
                except FileNotFoundError:
                    continue
                df = gpd.GeoDataFrame(df.merge(geo, on=key, how="left"), geometry="geometry", crs=geo.crs)
                break
    df = df.head(limit)
    if isinstance(df, gpd.GeoDataFrame):
        if df.crs is not None:
            df = df.to_crs(4326)
        fc = json.loads(df.to_json(default=str))
    else:
        fc = {"type": "FeatureCollection", "features": [
            {"type": "Feature", "geometry": None, "properties": json.loads(r.to_json(default_handler=str))}
            for _, r in df.iterrows()]}
    fc["numberReturned"] = len(fc["features"])
    fc["scenario"] = scenario
    return fc


def make_handler(lake: Lake):
    class Handler(BaseHTTPRequestHandler):
        def _send(self, code: int, body: dict):
            data = json.dumps(body, default=str).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/geo+json" if "features" in body else "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *a):
            pass

        def do_GET(self):
            u = urlparse(self.path)
            q = {k: v[0] for k, v in parse_qs(u.query).items()}
            parts = [p for p in u.path.split("/") if p]
            try:
                if not parts:
                    return self._send(200, {"title": "sg-urban-spine", "links": [
                        {"rel": "data", "href": "/collections"}, {"rel": "conformance", "href": "/conformance"}]})
                if parts == ["conformance"]:
                    return self._send(200, {"conformsTo": [
                        "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core",
                        "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson"]})
                if parts == ["collections"]:
                    cols = collections(lake)
                    for c in cols:
                        c["links"] = [{"rel": "items", "href": f"/collections/{c['id']}/items"}]
                    return self._send(200, {"collections": cols})
                if len(parts) == 3 and parts[0] == "collections" and parts[2] == "items":
                    return self._send(200, features(lake, parts[1], q.get("scenario", BASELINE),
                                                    int(q.get("limit", 10000))))
                if parts == ["scenarios"]:
                    return self._send(200, {"scenarios": lake.scenarios()})
                if len(parts) == 3 and parts[0] == "lineage":
                    p = lake.gold_dir(parts[1], parts[2]) / "_lineage.json"
                    return self._send(200, json.loads(p.read_text()))
                return self._send(404, {"error": "not found"})
            except (FileNotFoundError, KeyError) as e:
                return self._send(404, {"error": str(e)})
            except Exception as e:
                return self._send(500, {"error": f"{type(e).__name__}: {e}"})

    return Handler


def serve(lake: Lake, host: str = "127.0.0.1", port: int = 8090) -> None:
    httpd = ThreadingHTTPServer((host, port), make_handler(lake))
    print(f"serving {lake.root} on http://{host}:{port}")
    httpd.serve_forever()
