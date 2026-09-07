# Operations runbook

## Standard run

1. Confirm the SQL warehouse or serverless compute is available.
2. Check the catalog, schema, year, month and ingestion-mode parameters.
3. Confirm required source files exist in the configured Unity Catalog Volume.
4. Run the notebook from the first cell.
5. Review `pipeline_quality_audit` and `silver_trips_quarantine`.
6. Confirm Gold tables refreshed successfully.
7. Refresh the AI/BI dashboard and validate its KPI totals.

## Quality gates

The notebook fails when:

- Bronze is empty.
- Silver is empty.
- Duplicate `trip_id` values exist in Silver.
- The quarantine rate reaches 10%.

Warnings that do not breach a gate remain queryable through the audit and quarantine tables.

## Incident triage

| Symptom | First checks | Typical resolution |
|---|---|---|
| Source read fails | Volume path, file name, permissions | Restage source and rerun |
| NOAA columns missing | `bronze_weather` schema | Update source-field mapping after verifying the upstream change |
| Quarantine spike | Group by `dq_reasons` | Isolate affected source month and inspect source quality |
| Duplicate-key failure | `trip_id`, ingestion timestamps | Validate key construction and latest-record window |
| Dashboard totals stale | Gold table history, dashboard refresh time | Refresh the dataset after confirming the pipeline completed |
| Full run exceeds capacity | Source months and cluster size | Process one month at a time or use larger compute |

## Recovery

Bronze and Silver trip ingestion are idempotent. After correcting the source or transformation, rerun the same month. Gold tables are rebuilt from Silver and should not be edited manually.

Use the following for traceability:

```sql
DESCRIBE HISTORY workspace.ride_hailing.silver_trips;

SELECT *
FROM workspace.ride_hailing.pipeline_quality_audit
ORDER BY checked_at DESC, check_name;
```

## Scheduling recommendation

For production, schedule monthly ingestion as a Databricks Workflow. Add task-level retries, a failure notification, a separate quality task and a final dashboard-refresh task. Use development, test and production catalogs rather than changing the same tables in place.

