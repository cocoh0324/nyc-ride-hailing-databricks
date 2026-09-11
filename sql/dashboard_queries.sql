-- Databricks AI/BI dashboard datasets

-- TRIP METRICS DATASET
-- Use this single dataset for every trip, revenue, zone, borough and airport
-- visual. The shared pickup_date field allows one global date filter to affect
-- all of those widgets consistently.
SELECT *
FROM workspace.ride_hailing.dashboard_trip_metrics;

-- Configure the AI/BI visuals from the Trip Metrics dataset as follows:
--   Total Trips:              SUM(trip_count)
--   Gross Revenue:            SUM(gross_revenue)
--   Average Revenue Per Trip: SUM(gross_revenue) / SUM(trip_count)
--   Demand by Hour:           pickup_hour, SUM(trip_count)
--   Top Pickup Zones:         zone, SUM(trip_count), sorted descending, top 10
--   Revenue & Driver Pay:     borough, SUM(gross_revenue), SUM(driver_pay)
--   Airport Revenue Per Trip: filter is_airport = true, group by zone,
--                             SUM(gross_revenue) / SUM(trip_count)
--
-- Map the global controls only once to this dataset:
--   Date range picker -> pickup_date
--   Borough           -> borough
--   License Provider  -> license_provider
--
-- The date picker may allow users to choose dates outside the source range.
-- When it is mapped correctly, such a selection returns no trip data rather
-- than leaving stale totals on screen.

-- DATA COVERAGE CHECK
-- Use this query to set and verify the dashboard's default date range.
SELECT
  MIN(pickup_date) AS min_pickup_date,
  MAX(pickup_date) AS max_pickup_date
FROM workspace.ride_hailing.dashboard_trip_metrics;

-- QUALITY DATASET
-- Keep the latest pipeline-quality result as a separate dataset. It describes
-- a pipeline run rather than individual pickup dates, so the trip-date filter
-- should not be mapped to this dataset.
WITH latest AS (
  SELECT MAX(checked_at) AS checked_at
  FROM workspace.ride_hailing.pipeline_quality_audit
)
SELECT q.*
FROM workspace.ride_hailing.pipeline_quality_audit AS q
INNER JOIN latest USING (checked_at)
ORDER BY q.passed, q.check_name;
