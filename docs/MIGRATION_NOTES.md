# Ride-hailing project: Databricks migration notes

## Why this design fits the Suncorp Analytics Engineer role

The original university project is a strong Spark analytics project, but its notebook chain is exploratory, uses machine-specific paths, repeatedly overwrites intermediate Parquet folders, and mixes engineering, EDA and modelling. The Databricks rewrite turns it into a governed analytical data product:

- **Bronze:** source-aligned HVFHV trips, taxi zones and NOAA weather stored as Delta with source and ingestion metadata.
- **Silver:** validated and deduplicated trips, a quarantine table with row-level failure reasons, standardised zones and hourly weather.
- **Gold:** hourly and daily zone metrics, airport hotspot metrics and a pricing feature mart.
- **Operations:** parameterised monthly ingestion, idempotent Delta `MERGE`, data-quality assertions, an append-only audit table and Delta history for traceability.
- **Governance:** all tables and raw files live in a Unity Catalog catalog/schema/volume, allowing Databricks lineage and access controls to apply.

## Original-to-new mapping

| Original project | Databricks table or mechanism |
|---|---|
| `data/raw/hvfhv_data/*.parquet` | `bronze_hvfhv_trips` |
| taxi zone lookup | `bronze_taxi_zones` → `silver_taxi_zones` |
| NOAA weather | `bronze_weather` → `silver_weather_hourly` |
| preprocessing notebooks | `silver_trips` + `silver_trips_quarantine` |
| demand/revenue feature notebooks | `gold_hourly_zone_metrics`, `gold_daily_zone_metrics` |
| weather + demand merge | `gold_pricing_feature_mart` |
| airport analysis | `gold_airport_hotspots` |
| manual row-count checks | `pipeline_quality_audit` + failing assertions |

## Recommended demo sequence

1. Run with the default month (`2023-07`) to validate Free Edition resources.
2. Inspect quarantined rows and explain why invalid data is isolated instead of silently discarded.
3. Show the Gold pricing feature mart and Delta table history.
4. Change `months` to `07,08,09,10,11,12` and rerun when capacity permits.
5. If Jobs/Workflows are available, schedule this notebook monthly and alert on failed runs.

PLUTO ingestion is optional because it is a large source and is not essential to demonstrating the core analytical-engineering pattern. Enable it with the `include_pluto` widget only after the core run succeeds.
