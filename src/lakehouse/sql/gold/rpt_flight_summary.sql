-- gold.rpt_flight_summary: the report's main cube, one row per month x carrier x origin airport.
-- Every measure is additive (counts and sums, never rates), so the report can filter and re-total
-- it in the browser; rates are computed after summing, never averaged.

SELECT
  strftime(f.flight_date, '%Y-%m') AS year_month,
  f.carrier_code,
  o.airport_code AS origin,
  o.state_code AS origin_state,
  count(*) AS scheduled,
  count(*) FILTER (WHERE NOT f.cancelled) AS departed,
  count(*) FILTER (WHERE NOT f.cancelled AND NOT f.diverted) AS completed,
  count(*) FILTER (WHERE f.is_on_time) AS on_time,
  count(*) FILTER (WHERE f.cancelled) AS cancelled,
  count(*) FILTER (WHERE f.cancellation_reason = 'Carrier') AS cancelled_carrier,
  count(*) FILTER (WHERE f.cancellation_reason = 'Weather') AS cancelled_weather,
  count(*) FILTER (WHERE f.cancellation_reason = 'National Air System') AS cancelled_nas,
  count(*) FILTER (WHERE f.cancellation_reason = 'Security') AS cancelled_security,
  count(*) FILTER (WHERE f.diverted) AS diverted,
  count(*) FILTER (WHERE f.dep_delay_min >= 15) AS dep_delayed,
  count(*) FILTER (WHERE f.arr_delay_min >= 15) AS arr_delayed,
  coalesce(sum(f.arr_delay_min) FILTER (WHERE NOT f.cancelled AND NOT f.diverted), 0)::BIGINT AS arr_delay_min_sum,
  coalesce(sum(f.taxi_out_min), 0)::BIGINT AS taxi_out_min_sum,
  count(f.taxi_out_min) AS taxi_out_n,
  coalesce(sum(f.carrier_delay_min), 0)::BIGINT AS carrier_delay_min,
  coalesce(sum(f.weather_delay_min), 0)::BIGINT AS weather_delay_min,
  coalesce(sum(f.nas_delay_min), 0)::BIGINT AS nas_delay_min,
  coalesce(sum(f.security_delay_min), 0)::BIGINT AS security_delay_min,
  coalesce(sum(f.late_aircraft_delay_min), 0)::BIGINT AS late_aircraft_delay_min,
  -- arrival delay distribution of completed flights, in minutes
  count(*) FILTER (WHERE f.arr_delay_min < -15) AS arr_bucket_early_15,
  count(*) FILTER (WHERE f.arr_delay_min >= -15 AND f.arr_delay_min < 0) AS arr_bucket_early_0,
  count(*) FILTER (WHERE f.arr_delay_min >= 0 AND f.arr_delay_min < 15) AS arr_bucket_0_15,
  count(*) FILTER (WHERE f.arr_delay_min >= 15 AND f.arr_delay_min < 30) AS arr_bucket_15_30,
  count(*) FILTER (WHERE f.arr_delay_min >= 30 AND f.arr_delay_min < 60) AS arr_bucket_30_60,
  count(*) FILTER (WHERE f.arr_delay_min >= 60 AND f.arr_delay_min < 120) AS arr_bucket_60_120,
  count(*) FILTER (WHERE f.arr_delay_min >= 120 AND f.arr_delay_min < 180) AS arr_bucket_120_180,
  count(*) FILTER (WHERE f.arr_delay_min >= 180) AS arr_bucket_180_plus
FROM gold_fact_flight f
JOIN gold_dim_airport o ON o.airport_sk = f.origin_airport_sk
GROUP BY ALL
ORDER BY year_month, carrier_code, origin
