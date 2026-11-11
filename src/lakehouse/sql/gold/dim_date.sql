-- gold.dim_date: one row per day across the loaded range.

SELECT
  CAST(strftime(d, '%Y%m%d') AS INTEGER) AS date_key,
  d::DATE AS date,
  year(d) AS year,
  quarter(d) AS quarter,
  month(d) AS month,
  strftime(d, '%B') AS month_name,
  strftime(d, '%Y-%m') AS year_month,
  isodow(d) AS iso_day_of_week,
  strftime(d, '%A') AS day_name,
  isodow(d) IN (6, 7) AS is_weekend
FROM range(
  (SELECT min(flight_date) FROM silver_flights)::TIMESTAMP,
  (SELECT max(flight_date) FROM silver_flights)::TIMESTAMP + INTERVAL 1 DAY,
  INTERVAL 1 DAY
) t(d)
ORDER BY d
