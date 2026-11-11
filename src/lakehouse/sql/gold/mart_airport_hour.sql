-- gold.mart_airport_hour: how departure delay builds through the day at each airport.

SELECT
  o.airport_code AS origin,
  o.city_name AS origin_city,
  f.crs_dep_hour AS hour,
  count(*) AS scheduled,
  round(avg(f.dep_delay_min), 2) AS avg_dep_delay_min,
  round(count(*) FILTER (WHERE f.dep_delay_min >= 15) / nullif(count(*) FILTER (WHERE NOT f.cancelled), 0), 4)
    AS dep_delayed_rate,
  round(count(*) FILTER (WHERE f.cancelled) / count(*), 4) AS cancel_rate
FROM gold_fact_flight f
JOIN gold_dim_airport o ON o.airport_sk = f.origin_airport_sk
GROUP BY ALL
ORDER BY origin, hour
