import geopandas as gpd
import pandas as pd
from shapely.geometry import box


def run(inputs, params):
    raw = pd.read_csv(inputs["raw"] / "plots.csv")
    version = inputs["raw"].name
    geom = [box(r.x0, r.y0, r.x1, r.y1) for r in raw.itertuples()]
    plots = gpd.GeoDataFrame({"plot_id": raw.plot_id, "source_version": version}, geometry=geom, crs="EPSG:3414")
    plots["area_m2"] = plots.area
    zoning = pd.DataFrame({"plot_id": raw.plot_id, "zone_type": raw.zone, "gross_plot_ratio": raw.gpr,
                           "source_version": version})
    return {"plots": plots, "zoning": zoning}
