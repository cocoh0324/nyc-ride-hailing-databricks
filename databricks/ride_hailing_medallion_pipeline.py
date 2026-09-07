# Databricks notebook source
# MAGIC %md
# MAGIC # NYC Ride-Hailing Medallion Pipeline
# MAGIC
# MAGIC A production-style rewrite of the MAST30034 ride-hailing project for an Analytics Engineering portfolio.
# MAGIC
# MAGIC **Flow:** public source files → Bronze Delta → validated/quarantined Silver → analysis-ready Gold.
# MAGIC
# MAGIC The notebook is intentionally rerunnable: source files are cached in a Unity Catalog volume and trip data is upserted with Delta `MERGE`.

# COMMAND ----------

from datetime import datetime
from pathlib import Path
from urllib.request import urlretrieve

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Runtime parameters and governed storage
# MAGIC
# MAGIC Start with one month in Free Edition. Change `months` to `07,08,09,10,11,12` after the first successful run.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Unity Catalog catalog")
dbutils.widgets.text("schema", "ride_hailing", "Schema")
dbutils.widgets.text("year", "2023", "Source year")
dbutils.widgets.text("months", "07", "Months (comma separated)")
dbutils.widgets.dropdown("include_pluto", "false", ["false", "true"], "Include large PLUTO source")
dbutils.widgets.dropdown("source_mode", "uploaded_sample", ["uploaded_sample", "public_download"], "Ingestion mode")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
YEAR = dbutils.widgets.get("year").strip()
MONTHS = [m.strip().zfill(2) for m in dbutils.widgets.get("months").split(",") if m.strip()]
INCLUDE_PLUTO = dbutils.widgets.get("include_pluto").lower() == "true"
SOURCE_MODE = dbutils.widgets.get("source_mode")
VOLUME = "ride_hailing_raw"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

assert CATALOG.replace("_", "").isalnum(), "Unsafe catalog name"
assert SCHEMA.replace("_", "").isalnum(), "Unsafe schema name"
assert YEAR.isdigit() and len(YEAR) == 4, "year must be YYYY"
assert all(m in {f"{i:02d}" for i in range(1, 13)} for m in MONTHS), "Invalid month"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`{VOLUME}`")
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")

print({"catalog": CATALOG, "schema": SCHEMA, "year": YEAR, "months": MONTHS, "include_pluto": INCLUDE_PLUTO, "source_mode": SOURCE_MODE})

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Bronze — source ingestion with audit metadata
# MAGIC
# MAGIC Files are downloaded by Databricks from authoritative public sources, so the 14GB local project folder does not need to be uploaded.

# COMMAND ----------

def download_once(url: str, destination: str) -> str:
    """Cache a public source in the governed volume and return its path."""
    if not Path(destination).exists():
        print(f"Downloading {url} -> {destination}")
        urlretrieve(url, destination)
    else:
        print(f"Using cached file {destination}")
    return destination


if SOURCE_MODE == "uploaded_sample":
    # Free Edition serverless compute blocks these public URLs, so a representative
    # 250k-row batch from the original local project is staged in the UC volume.
    trip_paths = [f"{VOLUME_PATH}/fhvhv_tripdata_2023-07_sample.parquet"]
    zone_path = f"{VOLUME_PATH}/taxi_zone_lookup.parquet"
    weather_path = f"{VOLUME_PATH}/weather_2023.parquet"
    pluto_path = None
else:
    trip_paths = []
    for month in MONTHS:
        trip_paths.append(
            download_once(
                f"https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_{YEAR}-{month}.parquet",
                f"{VOLUME_PATH}/fhvhv_tripdata_{YEAR}-{month}.parquet",
            )
        )
    zone_path = download_once(
        "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
        f"{VOLUME_PATH}/taxi_zone_lookup.csv",
    )
    weather_path = download_once(
        f"https://www.ncei.noaa.gov/oa/local-climatological-data/v2/access/{YEAR}/LCD_USW00094728_{YEAR}.csv",
        f"{VOLUME_PATH}/weather_{YEAR}.csv",
    )
    pluto_path = None
    if INCLUDE_PLUTO:
        pluto_path = download_once(
            "https://data.cityofnewyork.us/api/views/64uk-42ks/rows.csv?accessType=DOWNLOAD",
            f"{VOLUME_PATH}/pluto.csv",
        )

# COMMAND ----------

bronze_batch = (
    spark.read.parquet(*trip_paths)
    .withColumn("_source_file", F.lit(",".join(trip_paths)))
    .withColumn("_source_month", F.regexp_extract("_source_file", r"(\d{4}-\d{2})", 1))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn(
        "trip_id",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("hvfhs_license_num"), F.lit("")),
                F.coalesce(F.col("dispatching_base_num"), F.lit("")),
                F.coalesce(F.col("request_datetime").cast("string"), F.lit("")),
                F.coalesce(F.col("pickup_datetime").cast("string"), F.lit("")),
                F.coalesce(F.col("dropoff_datetime").cast("string"), F.lit("")),
                F.coalesce(F.col("PULocationID").cast("string"), F.lit("")),
                F.coalesce(F.col("DOLocationID").cast("string"), F.lit("")),
                F.coalesce(F.col("trip_miles").cast("string"), F.lit("")),
            ),
            256,
        ),
    )
)

if spark.catalog.tableExists("bronze_hvfhv_trips"):
    (
        DeltaTable.forName(spark, "bronze_hvfhv_trips").alias("target")
        .merge(bronze_batch.alias("source"), "target.trip_id = source.trip_id")
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    (bronze_batch.write.format("delta").mode("overwrite").saveAsTable("bronze_hvfhv_trips"))

def read_tabular(path: str, infer_schema: bool = True):
    if path.lower().endswith(".parquet"):
        return spark.read.parquet(path)
    return spark.read.option("header", True).option("inferSchema", infer_schema).csv(path)


(
    read_tabular(zone_path, infer_schema=True)
    .withColumn("_source_file", F.lit(zone_path))
    .withColumn("_ingested_at", F.current_timestamp())
    .write.format("delta").mode("overwrite").option("overwriteSchema", True)
    .saveAsTable("bronze_taxi_zones")
)

(
    read_tabular(weather_path, infer_schema=False)
    .withColumn("_source_file", F.lit(weather_path))
    .withColumn("_ingested_at", F.current_timestamp())
    .write.format("delta").mode("overwrite").option("overwriteSchema", True)
    .saveAsTable("bronze_weather")
)

if pluto_path:
    (
        spark.read.option("header", True).option("inferSchema", True).csv(pluto_path)
        .withColumn("_source_file", F.lit(pluto_path))
        .withColumn("_ingested_at", F.current_timestamp())
        .write.format("delta").mode("overwrite").option("overwriteSchema", True)
        .saveAsTable("bronze_pluto_lots")
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Silver — validation, quarantine, standardisation and deduplication
# MAGIC
# MAGIC Validation rules retain useful trips while rejecting impossible timestamps, zone IDs, distances, durations and negative financial values.

# COMMAND ----------

bronze = spark.table("bronze_hvfhv_trips")

validated = (
    bronze
    .withColumn("trip_duration_minutes", F.round(F.col("trip_time") / 60.0, 2))
    .withColumn(
        "gross_revenue",
        F.round(
            sum(
                F.coalesce(F.col(c).cast("double"), F.lit(0.0))
                for c in [
                    "base_passenger_fare", "tolls", "bcf", "sales_tax",
                    "congestion_surcharge", "airport_fee", "tips",
                ]
            ),
            2,
        ),
    )
    .withColumn("pickup_date", F.to_date("pickup_datetime"))
    .withColumn("pickup_hour", F.hour("pickup_datetime"))
    .withColumn("pickup_hour_ts", F.date_trunc("hour", F.col("pickup_datetime")))
    .withColumn("day_of_week", F.date_format("pickup_datetime", "E"))
    .withColumn("is_weekend", F.dayofweek("pickup_datetime").isin(1, 7))
    .withColumn("shared_request", F.when(F.col("shared_request_flag") == "Y", 1).otherwise(0))
    .withColumn("wav_request", F.when(F.col("wav_request_flag") == "Y", 1).otherwise(0))
    .withColumn(
        "dq_reasons",
        F.array_compact(
            F.array(
                F.when(F.col("trip_id").isNull(), F.lit("missing_trip_id")),
                F.when(~F.col("PULocationID").between(1, 263), F.lit("invalid_pickup_zone")),
                F.when(~F.col("DOLocationID").between(1, 263), F.lit("invalid_dropoff_zone")),
                F.when(F.col("pickup_datetime").isNull() | F.col("dropoff_datetime").isNull(), F.lit("missing_trip_timestamp")),
                F.when(F.col("request_datetime") > F.col("pickup_datetime"), F.lit("request_after_pickup")),
                F.when(F.col("pickup_datetime") >= F.col("dropoff_datetime"), F.lit("pickup_not_before_dropoff")),
                F.when(~F.col("trip_miles").between(0.01, 200.0), F.lit("invalid_trip_miles")),
                F.when(~F.col("trip_duration_minutes").between(0.1, 300.0), F.lit("invalid_trip_duration")),
                F.when(F.col("base_passenger_fare") < 0, F.lit("negative_base_fare")),
                F.when(F.col("driver_pay") < 0, F.lit("negative_driver_pay")),
            )
        ),
    )
)

quarantine = validated.filter(F.size("dq_reasons") > 0)
clean = validated.filter(F.size("dq_reasons") == 0).drop("dq_reasons")

latest = Window.partitionBy("trip_id").orderBy(F.col("_ingested_at").desc())
silver_batch = (
    clean.withColumn("_row_number", F.row_number().over(latest))
    .filter(F.col("_row_number") == 1)
    .drop("_row_number", "originating_base_num", "on_scene_datetime")
)

if spark.catalog.tableExists("silver_trips"):
    (
        DeltaTable.forName(spark, "silver_trips").alias("target")
        .merge(silver_batch.alias("source"), "target.trip_id = source.trip_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    silver_batch.write.format("delta").mode("overwrite").saveAsTable("silver_trips")

(
    quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", True)
    .saveAsTable("silver_trips_quarantine")
)

(
    spark.table("bronze_taxi_zones")
    .select(
        F.col("LocationID").cast("int").alias("location_id"),
        F.trim("Borough").alias("borough"),
        F.trim("Zone").alias("zone"),
        F.trim("service_zone").alias("service_zone"),
    )
    .dropDuplicates(["location_id"])
    .write.format("delta").mode("overwrite").option("overwriteSchema", True)
    .saveAsTable("silver_taxi_zones")
)

# NOAA Central Park station observations, normalised to hourly timestamps.
weather_cols = spark.table("bronze_weather").columns
required_weather = {"DATE", "HourlyDryBulbTemperature", "HourlyPrecipitation", "HourlyWindSpeed"}
missing_weather = required_weather.difference(weather_cols)
assert not missing_weather, f"NOAA schema changed; missing columns: {sorted(missing_weather)}"

weather = (
    spark.table("bronze_weather")
    .select(
        F.date_trunc("hour", F.to_timestamp("DATE")).alias("weather_hour_ts"),
        F.regexp_extract("HourlyDryBulbTemperature", r"-?\d+(?:\.\d+)?", 0).cast("double").alias("temperature_f"),
        F.when(F.upper(F.col("HourlyPrecipitation")) == "T", F.lit(0.001))
         .otherwise(F.regexp_extract("HourlyPrecipitation", r"\d+(?:\.\d+)?", 0).cast("double"))
         .alias("precipitation_inches"),
        F.regexp_extract("HourlyWindSpeed", r"\d+(?:\.\d+)?", 0).cast("double").alias("wind_speed_mph"),
    )
    .filter(F.col("weather_hour_ts").isNotNull())
    .groupBy("weather_hour_ts")
    .agg(
        F.avg("temperature_f").alias("temperature_f"),
        F.max("precipitation_inches").alias("precipitation_inches"),
        F.avg("wind_speed_mph").alias("wind_speed_mph"),
    )
)
weather.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable("silver_weather_hourly")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Gold — trusted pricing and operations data products

# COMMAND ----------

trips = spark.table("silver_trips")
zones = spark.table("silver_taxi_zones")

hourly = (
    trips.groupBy(
        "pickup_hour_ts", "pickup_date", "pickup_hour", "day_of_week", "is_weekend",
        "PULocationID", "hvfhs_license_num",
    )
    .agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg("trip_miles"), 2).alias("avg_trip_miles"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_trip_duration_minutes"),
        F.round(F.avg("base_passenger_fare"), 2).alias("avg_base_fare"),
        F.round(F.sum("gross_revenue"), 2).alias("gross_revenue"),
        F.round(F.sum("driver_pay"), 2).alias("driver_pay"),
        F.round(F.avg("shared_request"), 4).alias("shared_request_rate"),
        F.round(F.avg("wav_request"), 4).alias("wav_request_rate"),
    )
    .join(zones, F.col("PULocationID") == F.col("location_id"), "left")
    .drop("location_id")
)
hourly.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable("gold_hourly_zone_metrics")

daily = (
    trips.groupBy("pickup_date", "PULocationID", "hvfhs_license_num")
    .agg(
        F.count("*").alias("trip_count"),
        F.round(F.sum("gross_revenue"), 2).alias("gross_revenue"),
        F.round(F.sum("driver_pay"), 2).alias("driver_pay"),
        F.round(F.avg("trip_miles"), 2).alias("avg_trip_miles"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_trip_duration_minutes"),
    )
    .withColumn("revenue_per_trip", F.round(F.col("gross_revenue") / F.col("trip_count"), 2))
    .join(zones, F.col("PULocationID") == F.col("location_id"), "left")
    .drop("location_id")
)
daily.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable("gold_daily_zone_metrics")

pricing_features = (
    hourly.join(
        spark.table("silver_weather_hourly"),
        hourly.pickup_hour_ts == F.col("weather_hour_ts"),
        "left",
    )
    .drop("weather_hour_ts")
    .withColumn("revenue_per_trip", F.round(F.col("gross_revenue") / F.col("trip_count"), 2))
    .withColumn("driver_pay_share", F.round(F.col("driver_pay") / F.col("gross_revenue"), 4))
)
pricing_features.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable("gold_pricing_feature_mart")

(
    daily.filter(F.col("PULocationID").isin(1, 132, 138))
    .write.format("delta").mode("overwrite").option("overwriteSchema", True)
    .saveAsTable("gold_airport_hotspots")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Data quality monitoring and operational audit

# COMMAND ----------

bronze_count = spark.table("bronze_hvfhv_trips").count()
silver_count = spark.table("silver_trips").count()
quarantine_count = spark.table("silver_trips_quarantine").count()
duplicate_silver_keys = (
    spark.table("silver_trips").groupBy("trip_id").count().filter(F.col("count") > 1).count()
)
invalid_rate = quarantine_count / max(bronze_count, 1)

checks = [
    ("bronze_not_empty", bronze_count > 0, float(bronze_count), "> 0"),
    ("silver_not_empty", silver_count > 0, float(silver_count), "> 0"),
    ("silver_trip_id_unique", duplicate_silver_keys == 0, float(duplicate_silver_keys), "= 0"),
    ("quarantine_rate_below_10pct", invalid_rate < 0.10, float(invalid_rate), "< 0.10"),
]

quality_run = spark.createDataFrame(checks, ["check_name", "passed", "observed_value", "expectation"])
quality_run = (
    quality_run.withColumn("run_id", F.lit(datetime.utcnow().isoformat()))
    .withColumn("checked_at", F.current_timestamp())
    .withColumn("source_months", F.lit(",".join(f"{YEAR}-{m}" for m in MONTHS)))
)

quality_run.write.format("delta").mode("append").option("mergeSchema", True).saveAsTable("pipeline_quality_audit")
display(quality_run.orderBy("passed", "check_name"))

failed_checks = [row.check_name for row in quality_run.filter(~F.col("passed")).collect()]
assert not failed_checks, f"Pipeline failed data-quality checks: {failed_checks}"

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Consumer validation
# MAGIC
# MAGIC These queries mirror how pricing analysts, data scientists and actuaries consume the Gold layer.

# COMMAND ----------

spark.sql("""
SELECT
  pickup_date,
  borough,
  zone,
  SUM(trip_count) AS trips,
  ROUND(SUM(gross_revenue), 2) AS gross_revenue,
  ROUND(SUM(gross_revenue) / SUM(trip_count), 2) AS revenue_per_trip
FROM gold_daily_zone_metrics
GROUP BY pickup_date, borough, zone
ORDER BY gross_revenue DESC
LIMIT 20
""").display()

# COMMAND ----------

spark.sql("DESCRIBE HISTORY silver_trips").select(
    "version", "timestamp", "operation", "operationParameters", "operationMetrics"
).display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Portfolio visualisations
# MAGIC
# MAGIC Operational and pricing views built only from governed Gold tables.

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np

hourly_viz = spark.sql("""
SELECT pickup_hour, SUM(trip_count) AS trips, ROUND(SUM(gross_revenue), 2) AS revenue
FROM gold_hourly_zone_metrics
GROUP BY pickup_hour
ORDER BY pickup_hour
""").toPandas()

zone_viz = spark.sql("""
SELECT CONCAT(borough, ' — ', zone) AS zone_name,
       SUM(trip_count) AS trips,
       ROUND(SUM(gross_revenue), 2) AS revenue
FROM gold_daily_zone_metrics
WHERE zone IS NOT NULL
GROUP BY borough, zone
ORDER BY trips DESC
LIMIT 10
""").toPandas().sort_values("trips")

borough_viz = spark.sql("""
SELECT borough,
       ROUND(SUM(gross_revenue), 2) AS gross_revenue,
       ROUND(SUM(driver_pay), 2) AS driver_pay
FROM gold_daily_zone_metrics
WHERE borough IS NOT NULL
GROUP BY borough
ORDER BY gross_revenue DESC
""").toPandas()

airport_viz = spark.sql("""
SELECT zone,
       SUM(trip_count) AS trips,
       ROUND(SUM(gross_revenue) / SUM(trip_count), 2) AS revenue_per_trip
FROM gold_airport_hotspots
GROUP BY zone
ORDER BY trips DESC
""").toPandas()

assert all(not frame.empty for frame in [hourly_viz, zone_viz, borough_viz, airport_viz]), "A visualisation source is empty"

plt.style.use("seaborn-v0_8-whitegrid")
fig, axes = plt.subplots(2, 2, figsize=(18, 12))
fig.suptitle("NYC Ride-Hailing: Trusted Gold-Layer Insights", fontsize=20, fontweight="bold", y=1.02)

axes[0, 0].plot(hourly_viz["pickup_hour"], hourly_viz["trips"], marker="o", linewidth=2.5, color="#1f77b4")
axes[0, 0].set(title="Demand profile by pickup hour", xlabel="Hour of day", ylabel="Trips")
axes[0, 0].set_xticks(range(0, 24, 2))

axes[0, 1].barh(zone_viz["zone_name"], zone_viz["trips"], color="#2ca02c")
axes[0, 1].set(title="Top 10 pickup zones", xlabel="Trips", ylabel="")

x = np.arange(len(borough_viz))
width = 0.38
axes[1, 0].bar(x - width / 2, borough_viz["gross_revenue"], width, label="Gross revenue", color="#ff7f0e")
axes[1, 0].bar(x + width / 2, borough_viz["driver_pay"], width, label="Driver pay", color="#9467bd")
axes[1, 0].set(title="Revenue and driver pay by borough", xlabel="Borough", ylabel="USD")
axes[1, 0].set_xticks(x, borough_viz["borough"], rotation=25, ha="right")
axes[1, 0].legend()

airport_colors = ["#d62728", "#17becf", "#bcbd22"][:len(airport_viz)]
bars = axes[1, 1].bar(airport_viz["zone"], airport_viz["revenue_per_trip"], color=airport_colors)
axes[1, 1].set(title="Airport revenue per trip", xlabel="Pickup airport", ylabel="USD per trip")
axes[1, 1].tick_params(axis="x", rotation=20)
axes[1, 1].bar_label(bars, fmt="$%.2f", padding=3)

for ax in axes.flat:
    ax.spines[["top", "right"]].set_visible(False)

fig.text(0.01, 0.01, "Source: NYC TLC HVFHV, taxi zones and NOAA weather • Gold Delta tables", fontsize=10, color="#555555")
plt.tight_layout()
plt.show()
