"""One-time: transform the committed July 2025 gold snapshot into the
compact JSON the static site consumes (site/public/wells.json).

Triplets only (all three vendors present), matching the original
Streamlit app. The output is committed — the dataset is frozen, so
the site build needs no Python and no pipeline.

Run from the repo root:
    uv run --no-project --with duckdb python src/scripts/build_site_data.py
"""
import json
from pathlib import Path

import duckdb

ROOT = Path(__file__).parent.parent.parent
PARQUET = next((ROOT / "sample_data").glob("*.snappy.parquet"))
OUT = ROOT / "site" / "public" / "wells.json"

con = duckdb.connect()
con.execute("install spatial; load spatial")
rows = con.execute(f"""
  with g as (
    select uwi_10, well_name_ENV,
      st_geomfromtext(geom_ENV) ge, st_geomfromtext(geom_OCC) go,
      st_geomfromtext(geom_SP) gs,
      distance_env_occ deo, distance_env_sp des, distance_occ_sp dos
    from '{PARQUET}'
    where geom_ENV is not null and geom_OCC is not null
      and geom_SP is not null)
  select uwi_10, well_name_ENV,
    round(st_x(ge), 7), round(st_y(ge), 7),
    round(st_x(go), 7), round(st_y(go), 7),
    round(st_x(gs), 7), round(st_y(gs), 7),
    round(deo, 1), round(des, 1), round(dos, 1)
  from g order by uwi_10
""").fetchall()

wells = [
    {"u": str(r[0]), "n": r[1],
     "e": [r[2], r[3]], "o": [r[4], r[5]], "s": [r[6], r[7]],
     "d": [r[8], r[9], r[10]]}
    for r in rows
]
OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text(json.dumps(wells, separators=(",", ":")))
print(f"{len(wells):,} triplets -> {OUT} "
      f"({OUT.stat().st_size/1e6:.2f} MB)")
