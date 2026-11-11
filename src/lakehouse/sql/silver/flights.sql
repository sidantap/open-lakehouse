-- silver.flights: contract-valid rows, plus a few columns every consumer would otherwise derive.
-- Input: `validated`, the output of the contract's validation query, filtered to valid rows.

SELECT
  -- a stable key for the flight leg, built from the contract's primary key
  substr(md5(concat_ws('|', flight_date, carrier_code, flight_number, origin, dest, crs_dep_hhmm)), 1, 16)
    AS flight_key,
  * EXCLUDE (_source_file, _row_number),

  -- scheduled departure as a local timestamp; BTS writes midnight as 2400
  flight_date::TIMESTAMP
    + to_minutes(CAST(substr(crs_dep_hhmm, 1, 2) AS INTEGER) * 60 + CAST(substr(crs_dep_hhmm, 3, 2) AS INTEGER))
    AS crs_dep_local,
  CAST(substr(crs_dep_hhmm, 1, 2) AS INTEGER) % 24 AS crs_dep_hour,

  origin || '-' || dest AS route,
  CASE cancellation_code
    WHEN 'A' THEN 'Carrier' WHEN 'B' THEN 'Weather'
    WHEN 'C' THEN 'National Air System' WHEN 'D' THEN 'Security'
  END AS cancellation_reason,
  -- the DOT definition: arrived less than 15 minutes after schedule
  NOT cancelled AND NOT diverted AND NOT arr_del15 AS is_on_time
FROM validated
