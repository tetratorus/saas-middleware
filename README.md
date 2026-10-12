# sg-urban-spine

**Rewire, don't rewrite.** A pace-layered spine that lets Singapore's research models (UWG, City Energy Analyst, 3D Land Use Planning, DUCT, The World Avatar, SimMobility…) run together over one versioned model of the city — without touching their science.

```
5 Views        DUCT Explorer · TWA viz · ePlanner      <- spine serve (OGC API Features / GeoJSON)
4 Gold         results per scenario + _lineage.json    <- OpenLineage-style run events
3 Transforms   the models, each a transform.yaml       <- OGC API Processes-style contract + golden tests
2 Platform     spine runner (python | subprocess | docker | external), SaaS middleware, TWA stack
1 Silver       canonical city model, GeoParquet, EPSG:3414; hubs plot_id / building_id / district_id
   Scenarios   branches = overlay rows keyed by hub id (lakeFS/Nessie idea)
0 Bronze       raw, append-only, manifest per version (Databricks medallion)
```
Slow layers at the bottom outlive fast layers at the top. See [spec/LAYERS.md](spec/LAYERS.md) for the contract each repo implements.

## Quick start
```bash
pip install -e '.[dev]'
pytest                                               # spine's own tests
export SPINE_LAKE=./lake SPINE_PATH=examples/toy
spine bronze-add toy_plots v1 examples/toy/bronze/plots.csv --url file://toy --license MIT
spine run example.conform_plots                       # bronze -> silver
spine run example.plot_capacity                       # silver -> gold (+ lineage)
spine scenario create rezone_p1 --description "P1 to GPR 3.5"
spine scenario set rezone_p1 --table zoning --rows '[{"plot_id":"P1","gross_plot_ratio":3.5}]'
spine run example.plot_capacity --scenario rezone_p1  # same model, branched data
spine test                                            # golden tests
spine serve                                           # http://127.0.0.1:8090/collections
dagster dev -m spine.dagster_defs                     # asset graph UI (needs SPINE_PATH)
```

## Production precedents
See [spec/LAYERS.md](spec/LAYERS.md) for the full table. Pattern = operational model shell (Delft-FEWS/NWS CHPS, NOAA NextGen, ESA EOEPCA).

## Concepts borrowed (production-tested)
Databricks medallion (Bronze/Silver/Gold) · Data Vault 2.0 hubs/satellites · GeoParquet + SVY21 · OGC API Processes / Application Packages · lakeFS / Project Nessie branches · OpenLineage · Delta Live Tables expectations · OGC API Features.
