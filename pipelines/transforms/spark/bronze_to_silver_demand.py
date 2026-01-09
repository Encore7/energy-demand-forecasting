from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name: str) -> SparkSession:
    """
    Spark configured to read/write through lakeFS S3 gateway (S3A).
    """
    lakefs_s3_endpoint = os.environ["LAKEFS_S3_ENDPOINT_INTERNAL"]
    aws_key = os.environ["LAKEFS_ACCESS_KEY_ID"]
    aws_secret = os.environ["LAKEFS_SECRET_ACCESS_KEY"]

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # S3A settings for lakeFS gateway
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
    run_date = os.environ["RUN_DATE"]  # YYYY-MM-DD
    repo = os.environ["LAKEFS_REPO"]
    branch = os.environ["LAKEFS_BRANCH"]

    bronze_uri = (
        f"s3a://{repo}/{branch}/bronze/demand/source=smard/"
        f"filter=410/region=DE/res=hour/ingestion_date={run_date}/*.json"
    )
    silver_uri = f"s3a://{repo}/{branch}/silver/demand/source=smard/granularity=hour/dt={run_date}/"

    spark = build_spark(app_name="bronze_to_silver_demand_smard")

    schema = T.StructType(
        [
            T.StructField(
                "metadata",
                T.StructType(
                    [
                        T.StructField("ingested_at_utc", T.StringType(), True),
                        T.StructField("source", T.StringType(), True),
                        T.StructField("filter_id", T.StringType(), True),
                        T.StructField("region", T.StringType(), True),
                        T.StructField("resolution", T.StringType(), True),
                        T.StructField("chunk_timestamp_ms", T.LongType(), True),
                        T.StructField("run_date", T.StringType(), True),
                    ]
                ),
                True,
            ),
            T.StructField(
                "data",
                T.ArrayType(
                    T.StructType(
                        [
                            T.StructField("ts_ms", T.LongType(), False),
                            T.StructField("load_mw", T.DoubleType(), True),
                        ]
                    )
                ),
                True,
            ),
            T.StructField("raw", T.MapType(T.StringType(), T.StringType()), True),
        ]
    )

    raw = spark.read.schema(schema).json(bronze_uri)

    df = (
        raw.select("metadata", F.explode_outer("data").alias("p"))
        .select(
            F.col("metadata.source").alias("source"),
            F.col("metadata.filter_id").alias("filter_id"),
            F.col("metadata.region").alias("region"),
            F.col("metadata.resolution").alias("resolution"),
            F.to_timestamp(F.col("metadata.ingested_at_utc")).alias("ingested_at_utc"),
            F.col("p.ts_ms").cast("long").alias("ts_ms"),
            F.col("p.load_mw").cast("double").alias("load_mw"),
        )
        .withColumn("event_ts_utc", (F.col("ts_ms") / F.lit(1000)).cast("timestamp"))
        .withColumn("dt", F.to_date("event_ts_utc"))
        .filter(F.col("dt") == F.to_date(F.lit(run_date)))
    )

    df_hourly = (
        df.withColumn("hour_ts_utc", F.date_trunc("hour", F.col("event_ts_utc")))
        .groupBy("hour_ts_utc", "dt", "source", "filter_id", "region", "resolution")
        .agg(F.max("load_mw").alias("load_mw"))
        .select(
            F.col("hour_ts_utc").alias("event_ts_utc"),
            "dt",
            "source",
            "filter_id",
            "region",
            "resolution",
            "load_mw",
        )
    )

    start_ts = F.to_timestamp(F.lit(f"{run_date} 00:00:00"))
    end_ts = F.to_timestamp(F.lit(f"{run_date} 23:00:00"))

    expected = (
        spark.range(1)
        .select(
            F.explode(F.sequence(start_ts, end_ts, F.expr("interval 1 hour"))).alias(
                "event_ts_utc"
            )
        )
        .withColumn("dt", F.to_date("event_ts_utc"))
    )

    meta = (
        df_hourly.select("source", "filter_id", "region", "resolution")
        .limit(1)
        .collect()
    )
    if meta:
        source, filter_id, region, resolution = meta[0]
    else:
        source, filter_id, region, resolution = ("smard", "410", "DE", "hour")

    enriched = (
        expected.join(df_hourly, on=["event_ts_utc", "dt"], how="left")
        .withColumn("source", F.coalesce(F.col("source"), F.lit(source)))
        .withColumn("filter_id", F.coalesce(F.col("filter_id"), F.lit(filter_id)))
        .withColumn("region", F.coalesce(F.col("region"), F.lit(region)))
        .withColumn("resolution", F.coalesce(F.col("resolution"), F.lit(resolution)))
    )

    enriched.repartition(1).write.mode("overwrite").parquet(silver_uri)

    print(f"Silver written to: {silver_uri}")
    spark.stop()


if __name__ == "__main__":
    main()
