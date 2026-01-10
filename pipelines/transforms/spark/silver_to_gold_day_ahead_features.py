from __future__ import annotations

import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_spark(app_name: str) -> SparkSession:
    lakefs_s3_endpoint = os.environ["LAKEFS_S3_ENDPOINT_INTERNAL"]
    aws_key = os.environ["LAKEFS_ACCESS_KEY_ID"]
    aws_secret = os.environ["LAKEFS_SECRET_ACCESS_KEY"]

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.hadoop.fs.s3a.endpoint", lakefs_s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", aws_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )


def main() -> None:
    run_date_str = os.environ["RUN_DATE"]  # YYYY-MM-DD
    repo = os.environ["LAKEFS_REPO"]
    branch = os.environ["LAKEFS_BRANCH"]

    # History to load for lags/rolling windows (in days)
    # NOTE: assumes FEATURE_HISTORY_DAYS is set in env
    history_days = int(os.environ.get("FEATURE_HISTORY_DAYS"))

    run_date = datetime.strptime(run_date_str, "%Y-%m-%d").date()
    print(f"### DEBUG: RUN_DATE passed from Airflow = {run_date_str}")
    history_start = run_date - timedelta(days=history_days)
    # IMPORTANT: we also need NEXT day for target_load_mw = lead(24h)
    history_end = run_date + timedelta(days=1)

    demand_path = f"s3a://{repo}/{branch}/silver/demand/source=smard/granularity=hour/"
    weather_path = (
        f"s3a://{repo}/{branch}/silver/weather/source=open-meteo/granularity=hour/"
    )
    calendar_path = f"s3a://{repo}/{branch}/silver/calendar/"

    spark = build_spark("silver_to_gold_day_ahead_features")

    # 1) Read silver tables (all dt partitions, then filter)

    demand_df = spark.read.parquet(demand_path)
    # expected cols: ["event_ts_utc","dt","source","filter_id","region","resolution","load_mw"]

    weather_df = spark.read.parquet(weather_path)
    # expected cols: ["event_ts_utc","dt","location_name","latitude","longitude", weather vars...]

    calendar_df = spark.read.parquet(calendar_path)
    # expected row per dt: ["dt","day_of_week","day_of_month","week_of_year","month","year",
    #                       "is_weekend","is_holiday","is_working_day","season"]

    # dt should be a proper date in all dfs
    demand_df = demand_df.withColumn("dt", F.to_date("dt"))
    weather_df = weather_df.withColumn("dt", F.to_date("dt"))
    calendar_df = calendar_df.withColumn("dt", F.to_date("dt"))

    # Filter to [history_start, history_end] range
    demand_df = demand_df.filter(
        (F.col("dt") >= F.lit(history_start)) & (F.col("dt") <= F.lit(history_end))
    )
    weather_df = weather_df.filter(
        (F.col("dt") >= F.lit(history_start)) & (F.col("dt") <= F.lit(history_end))
    )
    calendar_df = calendar_df.filter(
        (F.col("dt") >= F.lit(history_start)) & (F.col("dt") <= F.lit(history_end))
    )

    # 2) Join demand + weather + calendar

    base = (
        demand_df.alias("d")
        .join(
            weather_df.alias("w"),
            on=["event_ts_utc", "dt"],
            how="left",
        )
        .join(
            calendar_df.alias("c"),
            on=["dt"],
            how="left",
        )
    )

    # Ensure an explicit series identifier column for TFT/LightGBM
    if "region" in base.columns:
        base = base.withColumn("series_id", F.col("region"))
    else:
        base = base.withColumn("series_id", F.lit("DE"))

    # Hour-of-day etc from timestamp (useful for both LightGBM and TFT)
    base = (
        base.withColumn("hour", F.hour("event_ts_utc")).withColumn(
            "minute", F.minute("event_ts_utc")
        )
        # Spark-safe: no week-based date_format pattern (no 'u')
        .withColumn("day_of_week_ts", F.dayofweek("event_ts_utc"))
    )

    # 3) Window specs for lags/rolling

    # Base time window (NO frame) – allowed for lag/lead
    time_window_base = Window.partitionBy("series_id").orderBy(
        F.col("event_ts_utc").cast("long")
    )

    # Base window for "same hour" stats
    hour_window_base = Window.partitionBy("series_id", "hour").orderBy(
        F.col("event_ts_utc").cast("long")
    )

    # History windows WITH frame – only for rolling aggregates
    # Whole-series history (for e.g. 7d, 30d stats)
    time_window_7d = time_window_base.rowsBetween(-24 * 7, -1)

    # Same-hour history
    hour_window_7d = hour_window_base.rowsBetween(-24 * 7, -1)
    hour_window_30d = hour_window_base.rowsBetween(-24 * 30, -1)

    feat = base

    # 4) Demand lags + rolling features

    feat = (
        feat
        # short lags – use base time window (no frame)
        .withColumn("load_lag_1h", F.lag("load_mw", 1).over(time_window_base))
        .withColumn("load_lag_2h", F.lag("load_mw", 2).over(time_window_base))
        .withColumn("load_lag_3h", F.lag("load_mw", 3).over(time_window_base))
        .withColumn("load_lag_6h", F.lag("load_mw", 6).over(time_window_base))
        .withColumn("load_lag_12h", F.lag("load_mw", 12).over(time_window_base))
        # daily/weekly lags
        .withColumn("load_lag_24h", F.lag("load_mw", 24).over(time_window_base))
        .withColumn("load_lag_48h", F.lag("load_mw", 48).over(time_window_base))
        .withColumn("load_lag_7d", F.lag("load_mw", 24 * 7).over(time_window_base))
        .withColumn("load_lag_14d", F.lag("load_mw", 24 * 14).over(time_window_base))
        # rolling stats (same hour) – use framed hour windows
        .withColumn(
            "load_mean_7d_same_hour",
            F.avg("load_mw").over(hour_window_7d),
        )
        .withColumn(
            "load_mean_30d_same_hour",
            F.avg("load_mw").over(hour_window_30d),
        )
        .withColumn(
            "load_std_30d_same_hour",
            F.stddev("load_mw").over(hour_window_30d),
        )
    )

    # 5) Weather-derived features

    if "temperature_2m" in feat.columns:
        feat = (
            feat.withColumn(
                "temp_lag_24h",
                F.lag("temperature_2m", 24).over(time_window_base),
            )
            .withColumn(
                "temp_mean_7d",
                F.avg("temperature_2m").over(time_window_7d),
            )
            .withColumn(
                "heating_degree",
                F.when(
                    F.col("temperature_2m") < 18, 18 - F.col("temperature_2m")
                ).otherwise(F.lit(0.0)),
            )
            .withColumn(
                "cooling_degree",
                F.when(
                    F.col("temperature_2m") > 22, F.col("temperature_2m") - 22
                ).otherwise(F.lit(0.0)),
            )
        )

    # 6) Target: day-ahead load (24h ahead)
    feat = feat.withColumn(
        "target_load_mw",
        F.lead("load_mw", 24).over(time_window_base),
    )

    # 7) Filter to rows whose "dt" is exactly RUN_DATE

    feat_out = feat.filter(
        (F.col("dt") == F.lit(run_date))
        & F.col("target_load_mw").isNotNull()
        & F.col("load_lag_24h").isNotNull()
    )

    # 8) Write gold features for this RUN_DATE

    gold_uri = (
        f"s3a://{repo}/{branch}/gold/day_ahead_features/"
        f"granularity=hour/dt={run_date_str}/"
    )

    feat_out.repartition(1).write.mode("overwrite").parquet(gold_uri)

    print(f"Gold day-ahead features written to: {gold_uri}")
    spark.stop()


if __name__ == "__main__":
    main()
