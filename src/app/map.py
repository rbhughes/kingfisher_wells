import streamlit as st
from streamlit_folium import st_folium
import folium
import pandas as pd
import duckdb
from shapely import wkt

parq_path = "/Users/bryan/dev/kingfisher_wells/src/well_surface_locations/"
parq_path += "part-00000-tid-2056548461317784044-e39174f2-0e34-468b-aec7-a12cdc7a95f0-16-1.c000.snappy.parquet"
df = duckdb.sql(f"select * from '{parq_path}'").df()

DISTANCE_THRESHOLD = 500

# Filter for rows where at least one pairwise distance exceeds the threshold
df_extremes = df[
    (df["distance_env_occ"] > DISTANCE_THRESHOLD)
    | (df["distance_env_sp"] > DISTANCE_THRESHOLD)
    | (df["distance_occ_sp"] > DISTANCE_THRESHOLD)
]


# df_top = df.head(200)
df_top = df_extremes
label_map = {"geom_OCC": "OCC", "geom_ENV": "Enverus", "geom_SP": "S&P Global"}
color_map = {"OCC": "blue", "Enverus": "red", "S&P Global": "orange"}

###
filter_text = st.text_input("Filter by 10-digit API# (35073...):")

df_top["uwi_10_str"] = df_top["uwi_10"].astype(str)

if filter_text:
    # Filter rows where uwi_10_str starts with the filter_text (prefix search)
    filtered_df = df_top[df_top["uwi_10_str"].str.startswith(filter_text)]
else:
    filtered_df = df_top
###

# Prepare data for plotting
rows = []
# for _, row in df_top.iterrows():
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

st.title("Kingfisher County Wells")
st.header("A three-vendor geospatial comparison")

st.set_page_config(layout="wide")


# --- Streamlit HTML legend (works anywhere!) ---
st.markdown(
    """
    <div style='padding:10px;margin-bottom:15px;border:2px solid #eee;background:white;width:200px;'>
      <b>Legend</b><br>
      <span style='display:inline-block;width:12px;height:12px;background:blue;border-radius:50%'></span> OCC<br>
      <span style='display:inline-block;width:12px;height:12px;background:red;border-radius:50%'></span> Enverus<br>
      <span style='display:inline-block;width:12px;height:12px;background:orange;border-radius:50%'></span> S&amp;P Global
    </div>
    """,
    unsafe_allow_html=True,
)

# --- Create the map with FeatureGroups for layer toggling ---
m = folium.Map(location=[CENTER_LAT, CENTER_LON], zoom_start=10)

groups = {}
for label in color_map:
    groups[label] = folium.FeatureGroup(name=label, show=True)
    m.add_child(groups[label])

# Plot points in their respective FeatureGroups
for r in rows:
    folium.CircleMarker(
        location=[r["lat"], r["lon"]],
        radius=7,
        color=color_map.get(r["label"], "gray"),
        opacity=0.5,
        fill=True,
        fill_color=color_map.get(r["label"], "gray"),
        fill_opacity=0.4,
        popup=f"{r['uwi_10']} ({r['label']})",
    ).add_to(groups[r["label"]])

# Add layer control for user toggling
folium.LayerControl(collapsed=False).add_to(m)

st_folium(m, width=None, height=700)

# Optionally drop the uwi_10_str helper column before display:
st.dataframe(filtered_df.drop(columns=["uwi_10_str"]))

# st.dataframe(df_top)
