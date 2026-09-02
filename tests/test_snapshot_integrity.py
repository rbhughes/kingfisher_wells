"""Sanity-check the committed gold snapshot: the stored pairwise
distances must match distances recomputed from the snapshot's own WKT
points (WGS84 spheroid). Guards against a corrupted or mislabeled
export ever replacing the July 2025 artifact."""
from pathlib import Path

import duckdb

PARQUET = (
    Path(__file__).parent.parent
    / "sample_data"
    / "part-00000-tid-2056548461317784044-e39174f2-0e34-468b-aec7-"
    "a12cdc7a95f0-16-1.c000.snappy.parquet"
)

PAIRS = [
    ("distance_env_occ", "geom_ENV", "geom_OCC"),
    ("distance_env_sp", "geom_ENV", "geom_SP"),
    ("distance_occ_sp", "geom_OCC", "geom_SP"),
]


def test_stored_distances_match_recomputed():
    con = duckdb.connect()
    con.execute("install spatial; load spatial")
    for stored, a, b in PAIRS:
        # DuckDB's st_distance_spheroid expects (lat, lon) point order,
        # the reverse of the WKT's (lon, lat).
        n, worst = con.execute(f"""
            with g as (
              select {stored} as stored,
                st_geomfromtext({a}) ga, st_geomfromtext({b}) gb
              from '{PARQUET}'
              where {a} is not null and {b} is not null
                and {stored} is not null)
            select count(*), max(abs(stored - st_distance_spheroid(
                st_point(st_y(ga), st_x(ga)),
                st_point(st_y(gb), st_x(gb)))))
            from g
        """).fetchone()
        assert n > 8000, f"{stored}: unexpectedly few rows ({n})"
        # Databricks ST_DISTANCESPHEROID and DuckDB's agree to well
        # under a meter; anything larger means a broken artifact.
        assert worst < 1.0, f"{stored}: worst disagreement {worst} m"


def test_triplet_count_matches_readme_claim():
    con = duckdb.connect()
    n, = con.execute(f"""
        select count(*) from '{PARQUET}'
        where geom_ENV is not null and geom_OCC is not null
          and geom_SP is not null
    """).fetchone()
    assert 8000 < n < 9000, n
