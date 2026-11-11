-- gold.mart_route_month: reliability of each airport pair per month.
-- Routes with fewer than 30 scheduled flights in a month are left out: their rates are noise.

SELECT
  strftime(f.flight_date, '%Y-%m') AS year_month,
  o.airport_code AS origin,
  o.city_name AS origin_city,
  d.airport_code AS dest,
  d.city_name AS dest_city,
  any_value(f.distance_mi) AS distance_mi,
  count(*) AS scheduled,
  count(DISTINCT f.carrier_code) AS carriers,
  round(count(*) FILTER (WHERE f.is_on_time) / nullif(count(*) FILTER (WHERE NOT f.cancelled AND NOT f.diverted), 0), 4)
    AS on_time_rate,
  round(count(*) FILTER (WHERE f.cancelled) / count(*), 4) AS cancel_rate,
  round(avg(f.arr_delay_min), 2) AS avg_arr_delay_min,
  quantile_cont(f.arr_delay_min, 0.9) AS p90_arr_delay_min
FROM gold_fact_flight f
JOIN gold_dim_airport o ON o.airport_sk = f.origin_airport_sk
JOIN gold_dim_airport d ON d.airport_sk = f.dest_airport_sk
GROUP BY ALL
HAVING count(*) >= 30
ORDER BY year_month, scheduled DESC
