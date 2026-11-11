-- gold.fact_flight: one row per scheduled flight leg, keyed to the dimensions.
-- Airports join on the version that was valid on the flight date (SCD Type 2).

SELECT
  f.flight_key,
  CAST(strftime(f.flight_date, '%Y%m%d') AS INTEGER) AS date_key,
  f.flight_date,
  f.carrier_code,
  o.airport_sk AS origin_airport_sk,
  d.airport_sk AS dest_airport_sk,
  f.flight_number,
  f.tail_number,
  f.route,
  f.crs_dep_local,
  f.crs_dep_hour,
  f.cancelled,
  f.cancellation_reason,
  f.diverted,
  f.is_on_time,
  f.dep_delay_min,
  f.arr_delay_min,
  f.arr_delay_late_min,
  f.taxi_out_min,
  f.taxi_in_min,
  f.air_time_min,
  f.distance_mi,
  f.carrier_delay_min,
  f.weather_delay_min,
  f.nas_delay_min,
  f.security_delay_min,
  f.late_aircraft_delay_min,
  f._source_month
FROM silver_flights f
JOIN gold_dim_airport o
  ON o.airport_id = f.origin_airport_id AND f.flight_date BETWEEN o.valid_from AND o.valid_to
JOIN gold_dim_airport d
  ON d.airport_id = f.dest_airport_id AND f.flight_date BETWEEN d.valid_from AND d.valid_to
