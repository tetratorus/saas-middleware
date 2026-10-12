"""spine CLI. Lake root: --lake or $SPINE_LAKE. Transform search path: --path or $SPINE_PATH (os.pathsep-separated)."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd
import yaml

from .golden import freeze, run_golden
from .lake import BASELINE, Lake
from .transform import discover, execute, load_descriptor, run_workflow


def _desc(args, ref):
    if Path(ref).exists():
        return load_descriptor(ref)
    descs = discover(args.path)
    if ref not in descs:
        sys.exit(f"unknown transform '{ref}'. Known: {', '.join(sorted(descs)) or '(none on SPINE_PATH)'}")
    return descs[ref]


def main(argv=None):
    ap = argparse.ArgumentParser(prog="spine")
    ap.add_argument("--lake", default=None)
    ap.add_argument("--path", action="append", default=None, help="dirs to search for transform.yaml")
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("ls", help="list transforms on the search path"); s.set_defaults(fn="ls")
    s = sub.add_parser("validate", help="validate transform.yaml descriptors"); s.add_argument("refs", nargs="*")
    s = sub.add_parser("bronze-add", help="append a raw source version to bronze")
    s.add_argument("source"); s.add_argument("version"); s.add_argument("files", nargs="+")
    s.add_argument("--url", required=True); s.add_argument("--license", required=True); s.add_argument("--notes", default="")
    s.add_argument("--link", action="store_true", help="symlink instead of copy (large data)")
    s = sub.add_parser("run", help="run a conformer or transform"); s.add_argument("ref")
    s.add_argument("--scenario", default=BASELINE); s.add_argument("--params", default="{}", help="JSON")
    s = sub.add_parser("test", help="run golden tests"); s.add_argument("refs", nargs="*")
    s = sub.add_parser("freeze", help="record golden expected outputs"); s.add_argument("ref"); s.add_argument("case")
    s = sub.add_parser("scenario", help="create/set/list scenario branches")
    s.add_argument("action", choices=["create", "set", "ls"]); s.add_argument("name", nargs="?")
    s.add_argument("--parent", default=BASELINE); s.add_argument("--description", default="")
    s.add_argument("--table"); s.add_argument("--rows", help="JSON list of row objects (partial columns ok)")
    s = sub.add_parser("workflow", help="run a workflow.yaml"); s.add_argument("file"); s.add_argument("--scenario")
    s = sub.add_parser("serve", help="serve silver/gold as OGC API Features"); s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8090)
    args = ap.parse_args(argv)
    lake = Lake(args.lake)

    if args.cmd == "ls":
        for tid, d in sorted(discover(args.path).items()):
            print(f"{tid:32} {d.data['version']:8} {d.kind:10} {d.data['runtime']['type']:10} {d.path}")
    elif args.cmd == "validate":
        refs = args.refs or [str(d.path) for d in discover(args.path).values()]
        for r in refs:
            d = _desc(args, r); print(f"ok  {d.id}  ({d.path})")
    elif args.cmd == "bronze-add":
        m = lake.bronze_add(args.source, args.version, args.files, source_url=args.url, license=args.license,
                            notes=args.notes, link=args.link)
        print(json.dumps({k: m[k] for k in ("source", "version", "added_at")} | {"files": len(m["files"])}))
    elif args.cmd == "run":
        ev = execute(lake, _desc(args, args.ref), scenario=args.scenario, params=json.loads(args.params))
        print(json.dumps({"run": ev["run"]["runId"], "outputs": [o["name"] for o in ev["outputs"]]}, indent=2))
    elif args.cmd == "test":
        descs = [_desc(args, r) for r in args.refs] if args.refs else list(discover(args.path).values())
        failed = 0
        for d in descs:
            if not d.data.get("golden"):
                print(f"--   {d.id}: no golden cases"); continue
            for r in run_golden(d):
                print(f"{'PASS' if r.passed else 'FAIL'} {r.transform} [{r.case}]")
                for m in r.messages:
                    print(f"     {m}")
                failed += not r.passed
        sys.exit(1 if failed else 0)
    elif args.cmd == "freeze":
        print(freeze(_desc(args, args.ref), args.case))
    elif args.cmd == "scenario":
        if args.action == "ls":
            print("\n".join(lake.scenarios()))
        elif args.action == "create":
            print(lake.scenario_create(args.name, args.parent, args.description))
        else:
            print(lake.scenario_set(args.name, args.table, pd.DataFrame(json.loads(args.rows))))
    elif args.cmd == "workflow":
        wf = yaml.safe_load(Path(args.file).read_text())
        for ev in run_workflow(lake, discover(args.path), wf, args.scenario):
            print(f"ran {ev['job']['name']} -> {[o['name'] for o in ev['outputs']]}")
    elif args.cmd == "serve":
        from .serve import serve
        serve(lake, args.host, args.port)


if __name__ == "__main__":
    main()
