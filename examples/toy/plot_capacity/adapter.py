import pandas as pd


def run(inputs, params):
    df = pd.DataFrame(inputs["plots"][["plot_id", "area_m2"]]).merge(inputs["zoning"][["plot_id", "gross_plot_ratio"]], on="plot_id")
    df["gfa_m2"] = df.area_m2 * df.gross_plot_ratio.fillna(0) * params["efficiency"]
    return {"plot_capacity": df[["plot_id", "gfa_m2"]]}
