# Layer contract: how a repo plugs into the spine

Rewire, don't rewrite. The upstream model code stays as it is. Each fork adds one top-level `rewire/` directory.

```
rewire/
  README.md                 what was wired, how to run, what is NOT runnable and why
  <transform_id>/           one dir per conformer/transform, e.g. rewire/uwg.urban_heat/
    transform.yaml          descriptor, validated by spec/transform.schema.json (`spine validate`)
    adapter.py              thin glue: silver/gold DataFrames -> upstream model API -> gold DataFrames
    golden/<case>/          inputs/*.parquet, params.json, expected/*.parquet (frozen from the UNMODIFIED model)
  bronze/                   (optional) scripts that fetch raw data and register it: `spine bronze-add`
  tests/                    pytest: golden tests + adapter unit tests (runnable with `pytest rewire/tests`)
```

## Rules
1. **Bronze is append-only.** Raw files are registered with `spine bronze-add <source> <version> --url --license`. Never edit them; a new release is a new version.
2. **Conformers (kind: conformer)** read only `bronze:` inputs and write Silver tables defined in `spec/silver/tables.yaml`. CRS EPSG:3414. Every row has `source_version`. Keys (hubs) are stable IDs.
3. **Transforms (kind: transform)** read only `silver:` / `gold:` inputs and write Gold tables. No hidden files, no absolute or Windows paths, no network calls inside the model step. Paths come from the inputs dict / directory contract.
4. **One copy of each science.** If two repos wrap the same model (CEA in duct-fom and TWA CEAAgent; GFA in 3d-landuse-planning and TWA GFAAgent), there is one transform id; the other side calls or points to it.
5. **Golden tests freeze the science** before any rewiring. Expected outputs come from the unmodified upstream model (or the upstream's own reference results). State tolerances and where references came from.
6. **Runtimes.** `python` (in-process, `module:function`), `subprocess` (any language, directory contract `{inputs} {outputs} {params}`), `docker` (same contract in a container), `external` (HPC/licensed; descriptor documents the contract, `notes` says how to run).
7. **Licences travel with descriptors.** `upstream.license` and `upstream.commercial_use` (allowed | restricted | review) are mandatory truths, not decoration.
8. **Views read Gold** through `spine serve` (OGC API Features / GeoJSON) — never by calling models directly.

## Transform ids in use (keep these exact)
| id | kind | owner repo |
|---|---|---|
| ura.mp2019_conform | conformer | 3d-landuse-planning (plots, zoning, regulations, roads from MP2019 + dev control) |
| gfa.allowable | transform | 3d-landuse-planning (+ TWA GFAAgent points here) |
| uwg.urban_heat | transform | uwg |
| morphology.district | transform | uwg (buildings + land_cover -> district_morphology gold: bldheight, blddensity, vertohor, grass/tree cover) |
| epw.weather_conform | conformer | uwg (EPW -> silver weather_hourly) |
| cea.building_demand | transform | CityEnergyAnalyst (+ duct-fom bem-cea and TWA CEAAgent point here) |
| cea.solar_potential | transform | CityEnergyAnalyst (optional) |
| duct.bdp_conform | conformer | duct-explorer (building footprints, LCZ -> silver buildings, land_cover) |
| duct.mva_wind_corridor | transform | duct-fom |
| duct.wrf_mesoscale, duct.palm_microscale, duct.scout_microscale | transform (external) | duct-fom |
| simmobility.mid_term | transform (external) | simmobility-prod |
| twa.sg_stack_conform | conformer | TheWorldAvatar (TWA Singapore stack datasets -> silver buildings) |
