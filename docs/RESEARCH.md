# Known production patterns for managing multi-model urban/scientific data (research log; sources verified by fetch)
Raw search dumps: ~/ura-work/research_raw.md ; overflow fetch: ~/.devin-files/devin-remote-overflows-1000/b5998295/content.txt, .../6e1788aa/content.txt

## Verified so far
- OGC = Open Geospatial Consortium (standards body: WMS/WFS, GeoPackage, CityGML, 3D Tiles community std, GeoParquet).
- OGC API Processes in PRODUCTION: ESA EOEPCA (EO Exploitation Platform Common Architecture) "Processing" building block = ZOO-Project zoo-project-dru, supports OGC WPS 1/2 and OGC API Processes Parts 1&2; apps packaged as CWL "OGC EO Application Packages"; Calrissian runs CWL as Kubernetes jobs; Toil/HTCondor for HPC. https://deployment-guide.docs.eoepca.org/current/building-blocks/oapip-engine/ ; OGC best practice 20-089 https://eoepca.org/docs/18/02b_-_OGC_EO_Apps_Best_Practice.pdf
- openEO: process graphs, OGC community standard (docs.ogc.org/cp/24-060) — used by Copernicus Data Space.
- Databricks medallion: bronze raw / silver validated / gold enriched; "incrementally improve structure and quality" https://docs.databricks.com/aws/en/lakehouse/medallion
- Write-Audit-Publish (WAP) with Apache Iceberg branches (AWS Glue Data Quality): write to branch, audit/quality checks, publish (fast-forward main). https://aws.amazon.com/blogs/big-data/build-write-audit-publish-pattern-with-apache-iceberg-branching-and-aws-glue-data-quality/ ; https://iceberg.apache.org/docs/latest/branching/
- lakeFS: branches for isolated envs / ETL testing https://docs.lakefs.io/use-cases/isolated-environments/
- Data mesh (Dehghani, martinfowler.com): 4 principles: domain ownership, data as a product, self-serve platform, federated computational governance. https://martinfowler.com/articles/data-mesh-principles.html  -> maps: each research group = domain owning its model/data product; spine = self-serve platform; LAYERS.md rules = federated computational governance.
- Dagster software-defined assets: code declares what data assets exist + how computed; materialize = run + persist. https://docs.dagster.io/guides/build/assets
- OpenLineage: Airflow provider + Marquez; AWS MWAA blog. https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/guides/structure.html
- UK National Digital Twin Programme: Information Management Framework (IMF): foundation data model/ontology + reference data library + integration architecture (federated, connected twins). https://ndtp.co.uk/integration-architecture/ ; https://digitaltwinhub.co.uk/information-management-framework-imf/
- Digital Twin Victoria (gov platform): "Managing Datasources" docs https://landusevictoria.atlassian.net/wiki/spaces/DTVP/pages/1946222641/Managing+Datasources (page 403 to fetch)
- Virtual Singapore: CityGML-based, NRF/SLA/GovTech, BIM integration https://www.gim-international.com/article/enhancing-virtual-singapore-with-bim-data ; https://isprs-archives.copernicus.org/articles/XLII-4-W7/37/2017/isprs-archives-XLII-4-W7-37-2017.pdf
- Helsinki 3D: CityGML + mesh, open WFS https://www.hel.fi/en/decision-making/information-on-helsinki/maps-and-geospatial-data/helsinki-3d
- Pangeo/STAC/Zarr cloud-native geospatial (catalog = STAC) https://guide.cloudnativegeo.org/cookbooks/zarr-stac-report/
- Copernicus Climate Data Store: catalogue + toolbox of workflows over datasets https://cds.climate.copernicus.eu/background
## TODO: verify NDTP IMF details, DT Victoria arch, STAC catalog, Iceberg vs GeoParquet; then implications -> spine changes:
  candidate changes: (1) catalog = STAC items per bronze/silver/gold asset; (2) scenarios = Iceberg/lakeFS branches + WAP (audit=golden+expectations before publish); (3) transform contract = OGC App Package (CWL) + OGC API Processes endpoint, our transform.yaml as thin profile or replace; (4) data-mesh framing for ownership.

## Round 2 (verified via search snippets, raw: research_raw2.md)
- Digital Twin Victoria (state gov, prod): "configured to FEDERATE data from data repositories, not store data within its own database" -> twin = catalog + views over owners' data, not a central lake. https://landusevictoria.atlassian.net/wiki/spaces/DTVP/pages/1946222641/Managing+Datasources
- UK NDTP / CDBB: Gemini Principles (2018) + Information Management Framework: Foundation Data Model (top ontology) + Reference Data Library + Integration Architecture -> "ecosystem of connected digital twins". https://www.cdbb.cam.ac.uk/system/files/documents/TheGeminiPrinciples.pdf
- STAC: OGC adopting as Community Standard (2025-03). https://www.ogc.org/requests/ogc-looks-to-adopt-stac-as-community-standard-seeking-public-comment-before-moving-to-vote/
- OGC Best Practice for EO Application Package 20-089r1 (CWL + container). https://docs.ogc.org/bp/20-089r1.html
- Apache Iceberg spec v3 adds geometry/geography types. https://iceberg.apache.org/spec/
- WAP: Netflix origin (Dataflow sample pipelines). https://www.engineering.fyi/article/ready-to-go-sample-data-pipelines-with-dataflow
- DIN SPEC 91607:2024 urban digital twin concepts (German standard).
- dbt layering staging -> intermediate -> marts (= bronze/silver/gold equiv). https://docs.getdbt.com/best-practices/
- 3DBAG (Netherlands national 3D building model, TU Delft) production pipeline repo https://github.com/3DBAG/3dbag-pipeline (check: Dagster-orchestrated)
- CONFIRMED 3DBAG pipeline: "Dagster is a Python framework, however, almost all data processing is done by external tools" = prod precedent for rewire-not-rewrite (national 3D building model NL).

## Proposed revision (sent to Leonard 14:15 UTC, awaiting choice)
orchestration=Dagster assets (3DBAG); model contract=OGC App Package (CWL+container) exposed via OGC API Processes (ZOO-Project, EOEPCA); catalog=STAC; storage=Iceberg (geo types v3) or GeoParquet+lakeFS; scenarios=branches + Write-Audit-Publish (audit=golden+expectations); lineage=OpenLineage (Dagster integration); federation-not-centralisation (DT Victoria, NDTP IMF); ownership=data mesh (domains=research groups).

## Round 3: operational model integration (raw: research_raw3.md, research_fetch3.md)
### Closest analogue: putting many research models into agency operations
- **Delft-FEWS (Deltares)**: operational forecasting shell used by ~dozens of agencies. **General Adapter** runs external models; "central philosophy ... knows as little as possible of module specific details"; data exchange via FEWS Published Interface (PI-XML / NetCDF), model adapters per model. https://publicwiki.deltares.nl/spaces/FEWSDOC/pages/8683932/05+General+Adapter+Module
  - **US NWS CHPS** = Delft-FEWS for all 13 River Forecast Centers (US national river forecasts). https://oss.deltares.nl/web/delft-fews/-/chps-river-forecasts-usa ; https://www.weather.gov/owp/oh_hrl_chps_implementation_strategy
- **NOAA OWP NextGen (ngen)** National Water Model framework: heterogeneous models (C/C++/Fortran/Python) via **CSDMS BMI**; "data centric approach, organizing the data first and mapping appropriate solutions to the existing data". https://github.com/NOAA-OWP/ngen ; https://www.weather.gov/media/owp/oh/docs/2021-OWP-NWM-NextGen-Framework.pdf
- **CSDMS Basic Model Interface (BMI 2.0)**: initialize/update/finalize + get/set_value, language-agnostic. https://joss.theoj.org/papers/10.21105/joss.02317 ; https://github.com/csdms/bmi
- **ESMF/NUOPC**: coupling layer of NOAA Unified Forecast System (operational weather). https://ufs.epic.noaa.gov/wp-content/uploads/2020/08/UFS_Webinar_ESMF_in_UFS_Architecture.pdf
- **OGC OpenMI 2.0** standard: runtime data exchange between models "transform integrated modelling into an operational tool". https://www.ogc.org/standards/openmi/
- **NOAA R2O governance**: NAO 216-105B research transitions policy + Readiness Levels RL1-9 (gates for research->operations). https://www.noaa.gov/organization/administration/nao-216-105b-policy-on-research-and-development-transitions ; https://orta.research.noaa.gov/support/readiness-levels/ ; UFS R2O + EPIC program https://www.epic.noaa.gov/about-epic/epic-program-details/
### Urban / planning platforms in production
- **UrbanSim Cloud**: land-use model integrates with travel models via exchanged skims/networks (static-skim mode = one-way coupling). https://cloud.urbansim.com/docs/general/documentation/travel%20model%20integration.html
- **ArcGIS Urban**: plans, scenarios, zoning-capacity. https://doc.esri.com/en/arcgis-urban/latest/help/help-scenarios.html
- **Digital Twin Victoria / NSW / Digital Earth Australia** on **TerriaJS** (CSIRO Data61) + Cesium; federates 60+ formats direct from portals; NationalMap 13,000 datasets. https://terria.com/resources/case-studies/australian-digital-twins ; https://landusevictoria.atlassian.net/wiki/spaces/DTVP/pages/2032369806/What+are+the+tech-specs+for+DTV
- **Netherlands**: Geonovum reference architecture "Stelsel Digitale Tweeling Fysieke Leefomgeving" (national DT system). https://www.geonovum.nl/uploads/documents/20220211%20Referentiearchitectuur%20Stelsel%20DTFL%20versie%200.9.pdf ; **TNO Urban Strategy** couples traffic/noise/air models. https://www.tno.nl/en/newsroom/insights/2023/04/urban-strategy-digital-twins-mobility/
- **UK NDTP IRIS** open-source integration demonstrator https://github.com/National-Digital-Twin/IRIS
- **Destination Earth (ECMWF/EU)**: Digital Twin Engine + Data Lake + Polytope (semantic data access). https://destine.ecmwf.int/digital-twin-engine/
- **Virtual Singapore**: Dassault 3DEXPERIENCE + NRF. https://www.3ds.com/newsroom/press-releases/dassault-systemes-and-national-research-foundation-collaborate-develop-virtual-singapore-platform ; OneMap3D (SLA 2021).
### Data layer in production
- **Overture Maps** (Amazon/Meta/Microsoft/TomTom): GeoParquet releases + **GERS** stable global entity IDs + bridge files + ID-stability rules (= hub keys across sources). https://docs.overturemaps.org/gers/ ; https://docs.overturemaps.org/gers/stability/ ; https://overturemaps.org/blog/2025/why-we-chose-geoparquet...
- OGC API EDR v1.1 (environmental data retrieval, adopted). Dagster at Mapbox (incremental adoption) https://dagster.io/customers/incremental-adoption-mapbox ; Airflow runs weather (Airflow Summit 2025). DVC for data/model versioning. IDM modelops-bundle (simulation model registry).
### Research-software handover literature
- "Sustainable Research Software Hand-Over" JORS https://openresearchsoftware.metajnl.com/articles/10.5334/jors.307 ; "How to build models for government: criteria driving model acceptance" Policy Sciences https://link.springer.com/article/10.1007/s11077-016-9250-4

## Round 4 (raw: research_fetch4.md)
- Delft-FEWS model adapters: an overview of models already linked lives on the Deltares wiki ("Models linked to Delft-FEWS"); each model has a generalAdapter config + moduleInstanceDescriptors. Pre-/post-adapters convert FEWS PI data <-> the model's native files.
- NWS chose Delft-FEWS for CHPS after evaluating service-oriented frameworks (incl. USDA OMS), because migrating the legacy NWSRFS would have disrupted operations. https://www.weather.gov/owp/oh_hrl_chps_implementation_strategy
- Overture GERS: "moves matching work upstream"; stable IDs for building/segment/place/division...; publishes ID churn; bridge files map source IDs -> GERS. https://docs.overturemaps.org/gers/stability/
- UK NDTP IRIS (production demonstrator, retrofit targeting): ETL = Airbyte + dbt. https://github.com/National-Digital-Twin/IRIS
- Mapbox adopted Dagster incrementally, alongside Airflow (~100 pipelines). https://dagster.io/customers/incremental-adoption-mapbox
- DestinE Digital Twin Engine = common orchestration + data handling + HPC across twins.
- Model acceptance in government (Policy Sciences 2016): criteria = credibility, transparency, fit with the policy process; there is a gap between how much models could be used in government and how much they actually are.

## SYNTHESIS: the industry-proven design (what to do)
Pattern = "OPERATIONAL MODEL SHELL" (Delft-FEWS/CHPS, NOAA NextGen, ESA EOEPCA, DestinE DTE): one platform that knows nothing about any model's internals; each model gets a thin adapter (pre-adapter -> run -> post-adapter) to ONE shared data exchange format; the model's science stays untouched. Production at US NWS (all 13 RFCs) and Deltares clients worldwide.
Layer by layer, each choice with a production precedent:
1. Raw/staging: append-only, versioned source drops (Databricks bronze; dbt staging; Overture monthly releases).
2. Canonical city model: GeoParquet (Overture), stable entity IDs + bridge files from each source's IDs (Overture GERS) -- solves "plot/building ID differs per repo". Shared vocabulary = TWA ontology (cf. UK NDTP IMF foundation data model).
3. Model contract: batch adapter = FEWS General Adapter style; containers + CWL as OGC Application Package (ESA EOEPCA) for heavy/HPC models; BMI (NOAA NextGen) only if models must exchange data step-by-step (not needed for GFA/UWG/CEA batch chain).
4. Orchestration: Dagster assets (3DBAG national pipeline, Mapbox) -- replace my homegrown runner.
5. Scenarios: branches + Write-Audit-Publish (Iceberg/lakeFS; Netflix) -- audit = golden tests + data checks before a result is published.
6. Lineage: OpenLineage (Airflow/Dagster integrations, Marquez).
7. Views: federate, don't centralise -- TerriaJS (Digital Twin Victoria, NSW, NationalMap 13k datasets) reading OGC/GeoJSON/3D Tiles; DUCT Explorer + TWA viz + ePlanner as consumers.
8. Governance for research->operations: NOAA NAO 216-105B + Readiness Levels RL1-9 as acceptance gates per model; data-mesh ownership (research group owns its model product).
What changes vs my v0.1 spine: keep the layer rules + adapters (= FEWS pattern); REPLACE homegrown runner -> Dagster, homegrown scenario overlays -> lakeFS/Iceberg branches + WAP, homegrown IDs -> GERS-style bridge tables, homegrown descriptor -> CWL App Package + thin metadata; ADD readiness-level gates + TerriaJS view.
