-- Databricks AI/BI dashboard datasets
-- Change the catalog and schema if you used non-default notebook parameters.

-- KPI cards
SELECT
  SUM(trip_count) AS total_trips,
  ROUND(SUM(gross_revenue), 2) AS gross_revenue,
  ROUND(SUM(gross_revenue) / NULLIF(SUM(trip_count), 0), 2) AS revenue_per_trip
FROM workspace.ride_hailing.gold_daily_zone_metrics;

-- Demand by hour of day: a single series, ordered numerically.
SELECT
  pickup_hour,
  SUM(trip_count) AS total_trips
FROM workspace.ride_hailing.gold_hourly_zone_metrics
GROUP BY pickup_hour
ORDER BY pickup_hour;

-- Top 10 pickup zones by trip volume.
SELECT
  zone,
  SUM(trip_count) AS total_trips
FROM workspace.ride_hailing.gold_daily_zone_metrics
WHERE zone IS NOT NULL
GROUP BY zone
ORDER BY total_trips DESC
LIMIT 10;

-- Revenue and driver pay by borough.
SELECT
  borough,
  ROUND(SUM(gross_revenue), 2) AS gross_revenue,
  ROUND(SUM(driver_pay), 2) AS driver_pay
FROM workspace.ride_hailing.gold_daily_zone_metrics
WHERE borough IS NOT NULL
GROUP BY borough
ORDER BY gross_revenue DESC;

-- Airport revenue per trip.
SELECT
  zone,
  SUM(trip_count) AS total_trips,
  ROUND(SUM(gross_revenue) / NULLIF(SUM(trip_count), 0), 2) AS revenue_per_trip
FROM workspace.ride_hailing.gold_airport_hotspots
GROUP BY zone
ORDER BY total_trips DESC;

-- Latest pipeline quality run.
WITH latest AS (
  SELECT MAX(checked_at) AS checked_at
  FROM workspace.ride_hailing.pipeline_quality_audit
)
SELECT q.*
FROM workspace.ride_hailing.pipeline_quality_audit AS q
INNER JOIN latest USING (checked_at)
ORDER BY q.passed, q.check_name;

