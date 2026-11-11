"""Rebuild src/lakehouse/seeds/airports.csv from OurAirports (public domain).

    curl -o /tmp/airports.csv https://davidmegginson.github.io/ourairports-data/airports.csv
    uv run python scripts/build_airport_seed.py /tmp/airports.csv

Keeps US and US-territory airports. Codes match on IATA first, then on the K-prefixed ICAO
ident, because BTS keeps an airport's old code after a rename (Palm Beach: DJT here, PBI in BTS).
"""

import sys
from pathlib import Path

import duckdb

OUT = Path(__file__).resolve().parents[1] / "src" / "lakehouse" / "seeds" / "airports.csv"
COUNTRIES = "('US', 'PR', 'VI', 'GU', 'AS', 'MP')"

src = sys.argv[1]
con = duckdb.connect()
con.execute(f"CREATE VIEW oa AS SELECT * FROM read_csv('{src}', all_varchar = true)")
con.execute(f"""
COPY (
  WITH base AS (
    SELECT * FROM oa
    WHERE iso_country IN {COUNTRIES} AND type IN ('large_airport', 'medium_airport', 'small_airport')
  ), coded AS (
    -- match on the IATA code first, then on the K-prefixed ICAO ident (BTS keeps old codes
    -- after a rename: OurAirports lists Palm Beach as DJT, BTS still reports PBI)
    SELECT nullif(iata_code, '') AS airport_code, 1 AS match_rank, * FROM base
    UNION ALL
    SELECT substr(ident, 2), 2, * FROM base
    WHERE ident LIKE 'K___' AND type IN ('large_airport', 'medium_airport')
  )
  SELECT airport_code, ident AS icao_code, name AS airport_name, type AS airport_type,
         round(latitude_deg::DOUBLE, 5) AS latitude, round(longitude_deg::DOUBLE, 5) AS longitude,
         elevation_ft::INTEGER AS elevation_ft, iso_region
  FROM coded
  WHERE airport_code IS NOT NULL
  QUALIFY row_number() OVER (PARTITION BY airport_code ORDER BY match_rank,
                             CASE type WHEN 'large_airport' THEN 1
                                       WHEN 'medium_airport' THEN 2 ELSE 3 END) = 1
  ORDER BY airport_code
) TO '{OUT}' (HEADER)
""")
print(con.execute(f"SELECT count(*) FROM read_csv('{OUT}')").fetchone()[0], "airports written to", OUT)
