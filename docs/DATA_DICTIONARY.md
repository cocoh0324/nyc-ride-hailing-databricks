# Data dictionary

## Bronze tables

### `bronze_hvfhv_trips`

Source-aligned NYC TLC HVFHV records with:

- `trip_id`: deterministic SHA-256 business key.
- `_source_file`: source provenance.
- `_source_month`: extracted `YYYY-MM` partition label.
- `_ingested_at`: processing timestamp.

### `bronze_taxi_zones`

Raw zone lookup plus ingestion metadata.

### `bronze_weather`

Raw NOAA Local Climatological Data plus ingestion metadata.

### `bronze_pluto_lots` (optional)

Raw NYC PLUTO records when `include_pluto=true`.

## Silver tables

### `silver_trips`

Validated, deduplicated trips. Important derived fields:

| Field | Description |
|---|---|
| `trip_duration_minutes` | Trip duration converted from seconds |
| `gross_revenue` | Fare plus tolls, taxes, surcharges, airport fee and tips |
| `pickup_date` | Calendar date of pickup |
| `pickup_hour` | Hour of day, 0–23 |
| `pickup_hour_ts` | Pickup timestamp truncated to the hour |
| `day_of_week` | Short weekday name |
| `is_weekend` | Saturday/Sunday flag |
| `shared_request` | Numeric shared-ride request flag |
| `wav_request` | Numeric wheelchair-accessible request flag |

### `silver_trips_quarantine`

Rejected rows plus `dq_reasons`, an array containing one or more of:

- `missing_trip_id`
- `invalid_pickup_zone`
- `invalid_dropoff_zone`
- `missing_trip_timestamp`
- `request_after_pickup`
- `pickup_not_before_dropoff`
- `invalid_trip_miles`
- `invalid_trip_duration`
- `negative_base_fare`
- `negative_driver_pay`

### `silver_taxi_zones`

One record per location ID with standardised `borough`, `zone` and `service_zone` values.

### `silver_weather_hourly`

Hourly average temperature and wind speed plus maximum hourly precipitation.

## Gold tables

### `gold_hourly_zone_metrics`

Hourly demand, distance, duration, fare, revenue, driver pay, shared-ride and accessibility metrics by pickup zone and provider.

### `gold_daily_zone_metrics`

Daily trips, revenue, driver pay, trip characteristics and revenue per trip by pickup zone and provider.

### `gold_pricing_feature_mart`

Hourly zone metrics enriched with weather and commercial ratios. Intended for pricing analysis and modelling.

### `gold_airport_hotspots`

Daily zone metrics restricted to Newark, JFK and LaGuardia airport location IDs.

## Dashboard view

### `dashboard_trip_metrics`

A shared date-aware view over `gold_hourly_zone_metrics` for all trip-based AI/BI dashboard visuals. It retains `pickup_date`, hour, borough, zone and provider dimensions so one global filter can consistently update every trip, revenue, zone, borough and airport widget. The `is_airport` flag identifies Newark, JFK and LaGuardia pickup zones.

## Operational table

### `pipeline_quality_audit`

| Field | Description |
|---|---|
| `check_name` | Stable quality rule name |
| `passed` | Boolean result |
| `observed_value` | Numeric observation |
| `expectation` | Human-readable threshold |
| `run_id` | Unique pipeline-run identifier |
| `checked_at` | Check execution timestamp |
| `source_months` | Source period processed by the run |
