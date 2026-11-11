-- gold.dim_airport: Slowly Changing Dimension, Type 2.
--
-- BTS identifies an airport by a stable numeric ID, but its code, city name and state can change
-- (renamed cities, re-coded airports). Each distinct run of attributes becomes a version with
-- valid_from / valid_to, so a flight joins to the airport *as it was named on that day*.
--
-- The dimension is rebuilt from all of silver every run instead of MERGEd incrementally. That keeps
-- it correct when an older month is loaded late (a backfill can insert a version in the middle of
-- history), and at a few thousand airports it costs milliseconds.
--
-- Names and coordinates come from OurAirports. They're reference data, not tracked over time here.

WITH sightings AS (
  SELECT origin_airport_id AS airport_id, origin AS airport_code, origin_city AS city_name,
         origin_state AS state_code, origin_state_name AS state_name, flight_date
  FROM silver_flights
  UNION ALL
  SELECT dest_airport_id, dest, dest_city, dest_state, dest_state_name, flight_date
  FROM silver_flights
),
spans AS (  -- one row per airport per distinct attribute set per month
  SELECT airport_id, airport_code, city_name, state_code, state_name,
         date_trunc('month', flight_date) AS month, min(flight_date) AS first_seen
  FROM sightings
  GROUP BY ALL
),
flagged AS (
  SELECT *,
    md5(concat_ws('|', airport_code, city_name, state_code, state_name)) AS attr_hash,
    CASE WHEN md5(concat_ws('|', airport_code, city_name, state_code, state_name))
              = lag(md5(concat_ws('|', airport_code, city_name, state_code, state_name)))
                  OVER (PARTITION BY airport_id ORDER BY first_seen, airport_code, city_name)
         THEN 0 ELSE 1 END AS starts_version
  FROM spans
),
numbered AS (  -- gaps and islands: a new version starts whenever the attributes change
  SELECT *, sum(starts_version) OVER (
           PARTITION BY airport_id ORDER BY first_seen, airport_code, city_name
           ROWS UNBOUNDED PRECEDING) AS version
  FROM flagged
),
versions AS (
  SELECT airport_id, version, any_value(airport_code) AS airport_code, any_value(city_name) AS city_name,
         any_value(state_code) AS state_code, any_value(state_name) AS state_name,
         min(first_seen) AS valid_from
  FROM numbered
  GROUP BY airport_id, version
)
SELECT
  substr(md5(v.airport_id || '|' || v.valid_from), 1, 16) AS airport_sk,
  v.airport_id, v.airport_code, v.city_name, v.state_code, v.state_name,
  -- reference attributes from OurAirports (seeds/airports.csv); NULL when the code isn't found
  a.airport_name, a.icao_code, a.latitude, a.longitude, a.elevation_ft,
  v.version::INTEGER AS version,
  v.valid_from,
  coalesce(lead(v.valid_from) OVER w - INTERVAL 1 DAY, DATE '9999-12-31')::DATE AS valid_to,
  lead(v.valid_from) OVER w IS NULL AS is_current
FROM versions v
LEFT JOIN airports_seed a USING (airport_code)
WINDOW w AS (PARTITION BY v.airport_id ORDER BY v.valid_from)
ORDER BY v.airport_id, v.version
