-- gold.rpt_departure_hour: departures by month x carrier x weekday x scheduled hour (additive).

SELECT
  strftime(f.flight_date, '%Y-%m') AS year_month,
  f.carrier_code,
  isodow(f.flight_date) AS iso_day_of_week,
  f.crs_dep_hour AS hour,
  count(*) AS scheduled,
  count(*) FILTER (WHERE NOT f.cancelled) AS departed,
  count(*) FILTER (WHERE f.dep_delay_min >= 15) AS dep_delayed
FROM gold_fact_flight f
GROUP BY ALL
ORDER BY year_month, carrier_code, iso_day_of_week, hour
