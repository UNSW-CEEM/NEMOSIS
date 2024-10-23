import pandas as pd
import numpy as np
from datetime import timedelta
from src.nemosis import static_table, dynamic_data_compiler
import plotly.express as px

# Specify where we will be caching the raw AEMO data.
raw_data_cache = "C:/Users/nick/Desktop/cache"

# Time window to pull data from.
start_time = "2021/04/27 00:00:00"
end_time = "2021/04/28 00:00:00"

# Download the latest FCAS causer pays elements file. The update_static_file=True argument forces nemosis to
# download the a new copy of file from AEMO even if a copy already exists in the cache.
fcas_causer_pays_elements = static_table(
    table_name="ELEMENTS_FCAS_4_SECOND",
    raw_data_location=raw_data_cache,
    update_static_file=True,
)

# Using filtering and manual inspection find which fcas element numbers belong to Hornsdale Power Reserve.
elements_for_honsdale_power_reserve = fcas_causer_pays_elements[
    fcas_causer_pays_elements["EMSNAME"].str.contains("HPR")
]

# Check which variable numbers we will need.
fcas_causer_pays_elements = static_table(
    table_name="ELEMENTS_FCAS_4_SECOND",
    raw_data_location=raw_data_cache,
    update_static_file=True,
)

scada_4s_resolution = dynamic_data_compiler(
    start_time,
    end_time,
    table_name="FCAS_4_SECOND",
    raw_data_location=raw_data_cache,
    filter_cols=["ELEMENTNUMBER", "VARIABLENUMBER"],
    filter_values=([330, 331], [2, 5]),
    fformat="parquet",
)

scada_5min_resolution = dynamic_data_compiler(
    start_time,
    end_time,
    "DISPATCHLOAD",
    raw_data_cache,
    select_columns=["SETTLEMENTDATE", "DUID", "INITIALMW", "TOTALCLEARED"],
    filter_cols=["DUID"],
    filter_values=(["HPRG1", "HPRL1"],),
)

elements = {330: "HPRG1", 331: "HPRL1"}

variables = {2: "scada_value", 5: "regulation_target"}

scada_4s_resolution["DUID"] = scada_4s_resolution["ELEMENTNUMBER"].apply(
    lambda x: elements[x]
)
scada_4s_resolution["variable"] = scada_4s_resolution["VARIABLENUMBER"].apply(
    lambda x: variables[x]
)

scada_4s_resolution = scada_4s_resolution.pivot(
    index=["TIMESTAMP", "DUID"], columns="variable", values="VALUE"
)

scada_4s_resolution.reset_index(inplace=True)

scada = pd.merge_asof(
    scada_4s_resolution,
    scada_5min_resolution,
    left_on="TIMESTAMP",
    right_on="SETTLEMENTDATE",
    by="DUID",
    direction="forward",
)

scada["fraction_ramp_complete"] = 1 - (
    (scada["SETTLEMENTDATE"] - scada["TIMESTAMP"]) / timedelta(minutes=5)
)

scada["linear_ramp_target"] = (
    scada["INITIALMW"]
    + (scada["TOTALCLEARED"] - scada["INITIALMW"]) * scada["fraction_ramp_complete"]
)

scada["linear_ramp_target"] = np.where(
    scada["DUID"] == "HPRL1",
    -1 * scada["linear_ramp_target"],
    scada["linear_ramp_target"],
)
scada["scada_value"] = np.where(
    scada["DUID"] == "HPRL1", -1 * scada["scada_value"], scada["scada_value"]
)
scada["regulation_target"] = np.where(
    scada["DUID"] == "HPRL1",
    -1 * scada["regulation_target"],
    scada["regulation_target"],
)

scada = scada.groupby("TIMESTAMP", as_index=False).agg(
    {"linear_ramp_target": "sum", "scada_value": "sum", "regulation_target": "sum"}
)

scada["target"] = scada["linear_ramp_target"] + scada["regulation_target"]

fig = px.line(scada, x="TIMESTAMP", y=["target", "scada_value"])
fig.show()
