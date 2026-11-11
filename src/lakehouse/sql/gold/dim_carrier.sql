-- gold.dim_carrier: carrier names from a small seed file. A code that shows up in the data but not
-- in the seed still gets a row, so the fact table never loses flights to a failed join.

WITH seen AS (
  SELECT carrier_code, any_value(carrier_dot_id) AS carrier_dot_id, min(flight_date) AS first_seen,
         max(flight_date) AS last_seen
  FROM silver_flights
  GROUP BY carrier_code
)
SELECT
  s.carrier_code,
  s.carrier_dot_id,
  coalesce(c.carrier_name, 'Unknown carrier (' || s.carrier_code || ')') AS carrier_name,
  coalesce(c.carrier_group, 'Unknown') AS carrier_group,
  s.first_seen,
  s.last_seen
FROM seen s
LEFT JOIN carriers_seed c USING (carrier_code)
ORDER BY carrier_code
