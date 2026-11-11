-- gold.rpt_route_summary: month x carrier x airport pair (additive).

SELECT
  strftime(f.flight_date, '%Y-%m') AS year_month,
  f.carrier_code,
  o.airport_code AS origin,
  d.airport_code AS dest,
  any_value(f.distance_mi) AS distance_mi,
  count(*) AS scheduled,
  count(*) FILTER (WHERE NOT f.cancelled AND NOT f.diverted) AS completed,
  count(*) FILTER (WHERE f.is_on_time) AS on_time,
  count(*) FILTER (WHERE f.cancelled) AS cancelled,
  coalesce(sum(f.arr_delay_min) FILTER (WHERE NOT f.cancelled AND NOT f.diverted), 0)::BIGINT AS arr_delay_min_sum
FROM gold_fact_flight f
JOIN gold_dim_airport o ON o.airport_sk = f.origin_airport_sk
JOIN gold_dim_airport d ON d.airport_sk = f.dest_airport_sk
GROUP BY ALL
ORDER BY year_month, carrier_code, origin, dest
