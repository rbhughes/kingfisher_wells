import os
from pathlib import Path
import streamlit as st
import pandas as pd
from shapely import wkt
from streamlit_folium import st_folium
import folium

st.set_page_config(layout="wide")
st.title("Kingfisher County Well Location Variance")

st.markdown(
    """
    ### Do the Latitude/Longitude locations for the same wells match among vendors?
    **_Mostly!_** The surface location for wells present from all three sources (
    [S&amp;P Global](https://www.spglobal.com/commodity-insights/en/products-solutions/upstream-midstream-oil-gas),
    [Enverus](https://www.enverus.com/products/enverus-core/),
    and [Oklahoma Corporation Commission](https://gisdata-occokc.opendata.arcgis.com/)
    ) as of July 2025 are mapped below.
    Datums are normalized to WGS84, and we skipped wells where any Lat/Lon was null.
    Setting a lower Distance threshold will reveal less extreme variation but may degrade performance.
    Use the API filter to limit by API number (`35073...`).
    """
)

st.divider()
st.sidebar.header("Data Source")
USE_DATABRICKS = st.sidebar.checkbox("Run in Databricks Mode", value=False)

if USE_DATABRICKS:
    from databricks.connect import DatabricksSession

    TABLE_NAME = "geodata.gold.well_surface_locations"
    cluster_id = os.getenv("DATABRICKS_CLUSTER_ID")
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")

    session = DatabricksSession.builder.remote(
        host=host, token=token, cluster_id=cluster_id
    ).getOrCreate()

    """
    # ! NOPE: query = f"SELECT * FROM {TABLE_NAME}"
    We have to cast geom types in Databricks here. Why?


    Databricks Connect (with Spark Connect, as required by Databricks for 
    Python code in 2025) does not support serializing spatial/geography 
    columns (like geometry, geography, or UDTs from the spark-geospatial 
    library) to Pandas via .toPandas().

    In Databricks Notebook UX, these types are rendered using internal JVM 
    bridges, but Spark Connect uses gRPC and cannot serialize these JVM-side 
    objects for local Python clients.

    When reading from duckdb/parquet locally in Streamlit, all columns are 
    deserialized as strings or binary, which pandas/shapely can handle—thus, 
    no problem.
    """

    query = f"""
    SELECT
    uwi_10,
    well_name_ENV,
    distance_env_occ,
    distance_env_sp,
    distance_occ_sp,
    ST_AsText(geom_ENV) AS geom_ENV,
    ST_AsText(geom_OCC) AS geom_OCC,
    ST_AsText(geom_SP)  AS geom_SP
    FROM {TABLE_NAME}
    """
    df = session.sql(query).toPandas()

else:
    import duckdb

    app_dir = Path(__file__).parent

    sample_data_path = (
        app_dir.parent.parent
        / "sample_data"
        / "part-00000-tid-2056548461317784044-e39174f2-0e34-468b-aec7-a12cdc7a95f0-16-1.c000.snappy.parquet"
    )

    df = duckdb.sql(f"select * from '{sample_data_path}'").df()


# Filter for only those wells with all three vendor geoms
df = df.groupby("uwi_10").filter(
    lambda g: g["geom_ENV"].notnull().all()
    and g["geom_OCC"].notnull().all()
    and g["geom_SP"].notnull().all()
)

TOTAL_WELLS = len(df)

st.markdown(
    f"""
    #### {len(df)} well surface "triplets" in the dataset.
    _At least 3% of wells in Kingfisher county disagree on their surface plot by over 500 meters._
    """
)

# --- UI controls: side by side ---
col1, col2 = st.columns([1, 2])
with col1:
    threshold_str = st.text_input(
        "Distance threshold (meters):", value="500", key="threshold"
    )
with col2:
    filter_text = st.text_input(
        "Filter by 10-digit API# (35073...):",
        value="",
        key="apifilter",
        placeholder="Enter API# prefix",
    )

# --- Validate threshold input ---
error_msg = ""
try:
    DISTANCE_THRESHOLD = float(threshold_str)
    if DISTANCE_THRESHOLD < 0:
        error_msg = "Please enter a non-negative threshold."
        DISTANCE_THRESHOLD = 500
except ValueError:
    error_msg = "Threshold must be a number."
    DISTANCE_THRESHOLD = 500

if error_msg:
    st.error(error_msg)

# --- Apply distance threshold filter using OR ---
df_extremes = df[
    (df["distance_env_occ"] > DISTANCE_THRESHOLD)
    | (df["distance_env_sp"] > DISTANCE_THRESHOLD)
    | (df["distance_occ_sp"] > DISTANCE_THRESHOLD)
]
df_top = df_extremes.copy()

# --- Filter by uwi_10 ---
df_top["uwi_10_str"] = df_top["uwi_10"].astype(str)
if filter_text:
    filtered_df = df_top[df_top["uwi_10_str"].str.startswith(filter_text)]
else:
    filtered_df = df_top

label_map = {"geom_OCC": "OCC", "geom_ENV": "Enverus", "geom_SP": "S&P Global"}
color_map = {"OCC": "blue", "Enverus": "red", "S&P Global": "orange"}
rows = []
for _, row in filtered_df.iterrows():
    for col in ["geom_ENV", "geom_OCC", "geom_SP"]:
        geom = row[col]
        if pd.notnull(geom):
            try:
                point = wkt.loads(geom)
                label = label_map[col]
                rows.append(
                    {
                        "uwi_10": row["uwi_10"],
                        "well_name": row["well_name_ENV"],
                        "label": label,
                        "lat": point.y,
                        "lon": point.x,
                    }
                )
            except Exception:
                pass

if rows:
    CENTER_LAT = sum(r["lat"] for r in rows) / len(rows)
    CENTER_LON = sum(r["lon"] for r in rows) / len(rows)
else:
    CENTER_LAT, CENTER_LON = 36.0, -97.7

map_col, legend_col = st.columns([4, 1])
with map_col:
    m = folium.Map(location=[CENTER_LAT, CENTER_LON], zoom_start=10)
    groups = {}
    for label in color_map:
        groups[label] = folium.FeatureGroup(name=label, show=True)
        m.add_child(groups[label])
    for r in rows:
        folium.CircleMarker(
            location=[r["lat"], r["lon"]],
            radius=7,
            color=color_map.get(r["label"], "gray"),
            opacity=0.5,
            fill=True,
            fill_color=color_map.get(r["label"], "gray"),
            fill_opacity=0.5,
            popup=f"{r['uwi_10']} ({r['label']})",
        ).add_to(groups[r["label"]])
    folium.LayerControl(collapsed=False).add_to(m)
    st_folium(m, width=1100, height=520)

with legend_col:
    st.markdown(
        """
        <div style='padding:10px;margin-top:15px;border:2px solid #eee;background:white;width:160px;'>
          <b>Legend</b><br>
          <span style='display:inline-block;width:12px;height:12px;background:blue;border-radius:50%;margin-right:6px'></span> OCC<br>
          <span style='display:inline-block;width:12px;height:12px;background:red;border-radius:50%;margin-right:6px'></span> Enverus<br>
          <span style='display:inline-block;width:12px;height:12px;background:orange;border-radius:50%;margin-right:6px'></span> S&amp;P Global
        </div>
        """,
        unsafe_allow_html=True,
    )

st.dataframe(filtered_df.drop(columns=["uwi_10_str"]))
