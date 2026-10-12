"""The lake: Bronze (raw, append-only) -> Silver (canonical) -> Gold (results), plus scenario branches.

Layout under the lake root:
  bronze/<source>/<version>/...files    + manifest.json   (append-only)
  silver/<table>.parquet                                   (baseline canonical city model)
  scenarios/<name>/scenario.yaml + silver/<table>.parquet  (overlay rows keyed by the table's hub key)
  gold/<scenario>/<transform_id>/<table>.parquet + _lineage.json
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import shutil
from pathlib import Path
from typing import Iterable, Optional

import geopandas as gpd
import pandas as pd
import yaml

from .spec import silver_table
from .validate import check_silver

BASELINE = "baseline"
DELETED = "_deleted"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def read_table(path: Path) -> pd.DataFrame:
    try:
        return gpd.read_parquet(path)
    except (ValueError, KeyError):
        return pd.read_parquet(path)


def write_table(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp.parquet")
    df.to_parquet(tmp, index=False)
    os.replace(tmp, path)


class AppendOnlyError(RuntimeError):
    pass


class Lake:
    def __init__(self, root: os.PathLike | str | None = None):
        self.root = Path(root or os.environ.get("SPINE_LAKE", "lake")).resolve()

    # ---------- bronze ----------
    def bronze_dir(self, source: str, version: Optional[str] = None) -> Path:
        base = self.root / "bronze" / source
        if version is None:
            versions = self.bronze_versions(source)
            if not versions:
                raise FileNotFoundError(f"no bronze versions for source '{source}' in {base}")
            version = versions[-1]
        return base / version

    def bronze_versions(self, source: str) -> list[str]:
        base = self.root / "bronze" / source
        if not base.exists():
            return []
        vs = [p for p in base.iterdir() if (p / "manifest.json").exists()]
        return [p.name for p in sorted(vs, key=lambda p: json.loads((p / "manifest.json").read_text())["added_at"])]

    def bronze_add(self, source: str, version: str, files: Iterable[os.PathLike], *, source_url: str,
                   license: str, notes: str = "", link: bool = False) -> dict:
        dest = self.root / "bronze" / source / version
        files = [Path(f).resolve() for f in files]
        if (dest / "manifest.json").exists():
            existing = json.loads((dest / "manifest.json").read_text())
            want = {f.name: sha256_file(f) for f in _walk(files)}
            have = {e["path"]: e["sha256"] for e in existing["files"]}
            if all(have.get(k) == v for k, v in want.items()) and len(want) == len(have):
                return existing
            raise AppendOnlyError(f"bronze/{source}/{version} already exists with different content; add a new version instead")
        dest.mkdir(parents=True, exist_ok=True)
        entries = []
        for f in files:
            target = dest / f.name
            if f.is_dir():
                if link:
                    target.symlink_to(f, target_is_directory=True)
                else:
                    shutil.copytree(f, target)
            elif link:
                target.symlink_to(f)
            else:
                shutil.copy2(f, target)
        for p in sorted(_walk([dest])):
            if p.name == "manifest.json":
                continue
            entries.append({"path": str(p.relative_to(dest)), "bytes": p.stat().st_size, "sha256": sha256_file(p)})
        manifest = {"source": source, "version": version, "source_url": source_url, "license": license,
                    "notes": notes, "added_at": _now(), "files": entries}
        (dest / "manifest.json").write_text(json.dumps(manifest, indent=2))
        return manifest

    def bronze_manifest(self, source: str, version: Optional[str] = None) -> dict:
        return json.loads((self.bronze_dir(source, version) / "manifest.json").read_text())

    # ---------- silver ----------
    def silver_path(self, table: str) -> Path:
        return self.root / "silver" / f"{table}.parquet"

    def silver_write(self, table: str, df: pd.DataFrame) -> Path:
        check_silver(table, df)
        p = self.silver_path(table)
        write_table(df, p)
        return p

    def silver_read(self, table: str, scenario: str = BASELINE) -> pd.DataFrame:
        if scenario == BASELINE:
            p = self.silver_path(table)
            if not p.exists():
                raise FileNotFoundError(f"silver table '{table}' not materialised at {p}")
            return read_table(p)
        meta = self.scenario_meta(scenario)
        base = self.silver_read(table, meta.get("parent", BASELINE))
        overlay_p = self.scenario_dir(scenario) / "silver" / f"{table}.parquet"
        if not overlay_p.exists():
            return base
        return apply_overlay(base, read_table(overlay_p), silver_table(table)["key"])

    # ---------- scenarios (branches) ----------
    def scenario_dir(self, name: str) -> Path:
        return self.root / "scenarios" / name

    def scenario_meta(self, name: str) -> dict:
        p = self.scenario_dir(name) / "scenario.yaml"
        if not p.exists():
            raise FileNotFoundError(f"scenario '{name}' does not exist")
        return yaml.safe_load(p.read_text())

    def scenarios(self) -> list[str]:
        d = self.root / "scenarios"
        return [BASELINE] + (sorted(p.name for p in d.iterdir() if (p / "scenario.yaml").exists()) if d.exists() else [])

    def scenario_create(self, name: str, parent: str = BASELINE, description: str = "") -> Path:
        if name == BASELINE:
            raise ValueError("'baseline' is reserved")
        d = self.scenario_dir(name)
        if (d / "scenario.yaml").exists():
            raise FileExistsError(f"scenario '{name}' exists")
        if parent != BASELINE:
            self.scenario_meta(parent)
        d.mkdir(parents=True)
        (d / "scenario.yaml").write_text(yaml.safe_dump({"name": name, "parent": parent, "description": description,
                                                         "created_at": _now()}))
        return d

    def scenario_set(self, name: str, table: str, rows: pd.DataFrame) -> Path:
        """Upsert overlay rows (full rows or partial columns) for a silver table in a scenario."""
        key = silver_table(table)["key"]
        self.scenario_meta(name)
        p = self.scenario_dir(name) / "silver" / f"{table}.parquet"
        parent_rows = self.silver_read(table, self.scenario_meta(name).get("parent", BASELINE))
        rows = rows.copy()
        missing = [c for c in parent_rows.columns if c not in rows.columns]
        if missing:
            filled = rows.merge(parent_rows[key + missing], on=key, how="left")
            rows = filled
        if DELETED not in rows.columns:
            rows[DELETED] = False
        if p.exists():
            existing = read_table(p)
            rows = pd.concat([existing[~existing.set_index(key).index.isin(rows.set_index(key).index)], rows],
                             ignore_index=True)
        if "geometry" in rows.columns and not isinstance(rows, gpd.GeoDataFrame):
            rows = gpd.GeoDataFrame(rows, geometry="geometry", crs=getattr(parent_rows, "crs", None))
        check_silver(table, rows.drop(columns=[DELETED]))
        write_table(rows, p)
        return p

    # ---------- gold ----------
    def gold_dir(self, scenario: str, transform_id: str) -> Path:
        return self.root / "gold" / scenario / transform_id

    def gold_read(self, transform_id: str, table: str, scenario: str = BASELINE) -> pd.DataFrame:
        p = self.gold_dir(scenario, transform_id) / f"{table}.parquet"
        if not p.exists():
            raise FileNotFoundError(f"gold {transform_id}/{table} not computed for scenario '{scenario}' ({p})")
        return read_table(p)

    def gold_tables(self) -> list[tuple[str, str, str]]:
        out = []
        g = self.root / "gold"
        if g.exists():
            for p in sorted(g.glob("*/*/*.parquet")):
                out.append((p.parent.parent.name, p.parent.name, p.stem))
        return out


def apply_overlay(base: pd.DataFrame, overlay: pd.DataFrame, key: list[str]) -> pd.DataFrame:
    deleted = overlay[DELETED].fillna(False).astype(bool) if DELETED in overlay.columns else pd.Series(False, index=overlay.index)
    keep = ~base.set_index(key).index.isin(overlay.set_index(key).index)
    add = overlay[~deleted.values].drop(columns=[DELETED], errors="ignore")
    add = add[[c for c in base.columns if c in add.columns]]
    out = pd.concat([base[keep], add], ignore_index=True)
    if isinstance(base, gpd.GeoDataFrame):
        out = gpd.GeoDataFrame(out, geometry=base.geometry.name, crs=base.crs)
    return out


def _walk(paths: Iterable[Path]):
    for p in paths:
        if p.is_dir():
            yield from (q for q in p.rglob("*") if q.is_file())
        else:
            yield p


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="microseconds")
