"""OpenLineage-style run events attached to every Gold (and Silver) write."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import subprocess
import uuid
from pathlib import Path

PRODUCER = "https://github.com/tetratorus/sg-urban-spine"
SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/definitions/RunEvent"


def params_hash(params: dict) -> str:
    return hashlib.sha256(json.dumps(params, sort_keys=True, default=str).encode()).hexdigest()[:16]


def git_info(path: Path) -> dict:
    try:
        sha = subprocess.run(["git", "-C", str(path), "rev-parse", "HEAD"], capture_output=True, text=True,
                             timeout=5).stdout.strip()
        remote = subprocess.run(["git", "-C", str(path), "config", "--get", "remote.origin.url"], capture_output=True,
                                text=True, timeout=5).stdout.strip()
        dirty = bool(subprocess.run(["git", "-C", str(path), "status", "--porcelain", "--", "."], capture_output=True,
                                    text=True, timeout=5).stdout.strip())
        return {"commit": sha or None, "remote": _strip_creds(remote) or None, "dirty": dirty}
    except Exception:
        return {"commit": None, "remote": None, "dirty": None}


def _strip_creds(url: str) -> str:
    if "@" in url and "://" in url:
        scheme, rest = url.split("://", 1)
        return f"{scheme}://{rest.split('@', 1)[1]}"
    return url


def run_event(*, descriptor: dict, descriptor_dir: Path, scenario: str, params: dict, inputs: list[dict],
              outputs: list[dict], started: dt.datetime) -> dict:
    return {
        "eventType": "COMPLETE",
        "eventTime": dt.datetime.now(dt.timezone.utc).isoformat(),
        "producer": PRODUCER,
        "schemaURL": SCHEMA_URL,
        "run": {
            "runId": str(uuid.uuid4()),
            "facets": {
                "nominalTime": {"nominalStartTime": started.isoformat()},
                "scenario": {"name": scenario},
                "parameters": {"values": params, "hash": params_hash(params)},
            },
        },
        "job": {
            "namespace": "sg-urban-spine",
            "name": descriptor["id"],
            "facets": {
                "version": descriptor["version"],
                "kind": descriptor["kind"],
                "upstream": descriptor["upstream"],
                "sourceCode": git_info(descriptor_dir),
                "runtime": descriptor["runtime"]["type"],
            },
        },
        "inputs": inputs,
        "outputs": outputs,
    }
