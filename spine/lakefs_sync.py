"""Optional: mirror a scenario's lake into lakeFS (git-for-data branches; lakeFS / Iceberg branch pattern).
One lakeFS branch per scenario, one commit per publish, lineage run ids as commit metadata.
Env: LAKEFS_HOST, LAKEFS_ACCESS_KEY_ID, LAKEFS_SECRET_ACCESS_KEY, LAKEFS_REPO (default sg-urban-spine)."""
from __future__ import annotations

import json
import os

import lakefs
from lakefs.client import Client

from .lake import BASELINE, Lake


def _client() -> Client:
    return Client(host=os.environ["LAKEFS_HOST"], username=os.environ["LAKEFS_ACCESS_KEY_ID"],
                  password=os.environ["LAKEFS_SECRET_ACCESS_KEY"])


def publish(lake: Lake, scenario: str = BASELINE, message: str = "") -> str:
    c = _client()
    repo = lakefs.Repository(os.environ.get("LAKEFS_REPO", "sg-urban-spine"), client=c)
    main = repo.branch("main")
    branch = main if scenario == BASELINE else repo.branch(f"scenario-{scenario}").create(source_reference="main", exist_ok=True)
    roots = [lake.root / "silver"] if scenario == BASELINE else [lake.scenario_dir(scenario)]
    roots.append(lake.root / "gold" / scenario)
    run_ids = []
    for root in roots:
        for f in sorted(p for p in root.rglob("*") if p.is_file() and "_staging" not in p.parts):
            rel = f.relative_to(lake.root).as_posix()
            with open(f, "rb") as fh:
                branch.object(rel).upload(data=fh.read(), mode="wb")
            if f.name == "_lineage.json":
                run_ids.append(json.loads(f.read_text())["run"]["runId"])
    ref = branch.commit(message=message or f"publish {scenario}", metadata={"openlineage_runs": ",".join(run_ids)[:4000]})
    return ref.get_commit().id
