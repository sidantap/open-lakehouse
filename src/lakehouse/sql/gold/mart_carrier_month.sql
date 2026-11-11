-- gold.mart_carrier_month: on-time performance by carrier and month, with delay minutes by cause.

SELECT
  strftime(f.flight_date, '%Y-%m') AS year_month,
  f.carrier_code,
  c.carrier_name,
  count(*) AS scheduled,
  count(*) FILTER (WHERE f.cancelled) AS cancelled,
  count(*) FILTER (WHERE f.diverted) AS diverted,
  count(*) FILTER (WHERE f.is_on_time) AS on_time,
  round(count(*) FILTER (WHERE f.is_on_time) / nullif(count(*) FILTER (WHERE NOT f.cancelled AND NOT f.diverted), 0), 4)
    AS on_time_rate,
  round(count(*) FILTER (WHERE f.cancelled) / count(*), 4) AS cancel_rate,
  round(avg(f.arr_delay_min), 2) AS avg_arr_delay_min,
  sum(f.carrier_delay_min) AS carrier_delay_min,
  sum(f.weather_delay_min) AS weather_delay_min,
  sum(f.nas_delay_min) AS nas_delay_min,
  sum(f.security_delay_min) AS security_delay_min,
  sum(f.late_aircraft_delay_min) AS late_aircraft_delay_min
FROM gold_fact_flight f
JOIN gold_dim_carrier c USING (carrier_code)
GROUP BY ALL
ORDER BY year_month, carrier_code
