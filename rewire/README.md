# rewire/ — SaaS Middleware as an execution backend for sg-urban-spine

[tetratorus/sg-urban-spine](https://github.com/tetratorus/sg-urban-spine) defines models as `transform.yaml` +
thin adapter over a shared Silver/Gold GeoParquet lake (EPSG:3414). SaaS Middleware already solves the part the
spine leaves open: deploying a processor somewhere (local, SSH host, NSCC via PBS, Docker), running it in a working
directory, streaming `trigger:progress:<n>` / `trigger:output:<name>` and storing outputs with provenance in a DOR.
Nothing in `saas/` is changed; this directory only adds an **optional** bridge, `spine_backend.py`.

## Two directions

**1. Spine transform -> SaaS processor (runnable runtimes: python, subprocess, docker, cwl).**

```bash
pip install 'git+https://github.com/tetratorus/sg-urban-spine@main'
python -m rewire.spine_backend export path/to/<transform_id>/transform.yaml procs/<transform_id> [--config nscc]
```

writes a normal SaaS processor directory:

| file | content |
|---|---|
| `descriptor.json` | `ProcessorDescriptor`: one input per spine input (`data_type` `spine.silver.<table>` / `spine.gold.<id>.<table>`, `data_format` `parquet`), a `parameters` JSONObject whose `data_schema` is the transform's `parameters` JSON Schema, one output per Gold table |
| `processor.py` | calls `spine_backend.run(transform.yaml, wd)`: reads `<wd>/<input>` parquet, resolves `parameters`, calls `spine.transform.invoke`, writes `<wd>/<output>` parquet and prints the SaaS triggers |
| `install.sh` / `execute.sh` | the RTI native adapter contract (`install.sh <config>`, `execute.sh <config> <wd>`); Python 3.10 venv with the spine installed |

Commit that directory to a repo and deploy it with the usual GPP (`source`, `commit_id`, `proc_path`,
`proc_config`); jobs are submitted with `saas-cli` or `SDKProcessor.submit_and_wait`. The spine still owns
Write-Audit-Publish and lineage: publish the parquet outputs from the DOR into Gold with the spine.

**2. SaaS processor -> spine transform (`runtime: external`).** Heavy models that already are SaaS processors
(DUCT `ucm-wrf-*`, `ucm-palm-*`, `ucm-scout` on NSCC) stay that way. Their spine descriptors in
tetratorus/duct-fom `rewire/` say `runtime: external` and ship `adapter.prepare(inputs, params, wd)` (Silver ->
the processor's input files) and `adapter.collect(wd)` (its HDF5 outputs -> Gold frames). `export` refuses external
transforms on purpose: there is nothing to wrap, SaaS runs the original processor.

## Tests

```bash
python3.10 -m venv venv && . venv/bin/activate
pip install -e . pytest 'git+https://github.com/tetratorus/sg-urban-spine@main' \
  'numpy==1.23.5' 'pandas<2.3' 'pyarrow<18' 'geopandas==0.14.4' 'shapely<2.1'
pytest rewire/tests
```

`rewire/tests/test_spine_backend.py`: generated descriptors parse as `saas.dor.schemas.ProcessorDescriptor`;
the exported `processor.py` runs the SaaS working-directory contract and emits the triggers; external transforms are
refused; missing inputs exit non-zero.

## Gaps

- Not exercised end-to-end through a running RTI node + DOR (the RTI clones processors from a git remote; the test
  calls `processor.py` the way `execute.sh` does, without `install.sh`'s venv).
- Spine `subprocess`/`docker`/`cwl` runtimes are passed through `spine.transform.invoke`, so their own tooling
  (docker, cwltool) must exist on the RTI host.
- No automatic DOR -> spine Gold publish step; done by the caller.

## Upstream changes

None (no file under `saas/` or `examples/` modified).
