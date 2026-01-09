from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


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
    run_date = os.environ["RUN_DATE"]
    repo = os.environ["LAKEFS_REPO"]
    branch = os.environ["LAKEFS_BRANCH"]

    bronze_uri = (
        f"s3a://{repo}/{branch}/bronze/weather/source=open-meteo/*/"
        f"ingestion_date={run_date}/*.json"
    )
    silver_uri = (
        f"s3a://{repo}/{branch}/silver/weather/source=open-meteo/"
        f"granularity=hour/dt={run_date}/"
    )

    spark = build_spark("bronze_to_silver_weather_openmeteo")

    # Bronze schema (metadata + raw)
    schema = T.StructType(
        [
            T.StructField(
                "metadata",
                T.StructType(
                    [
                        T.StructField("ingested_at_utc", T.StringType(), True),
                        T.StructField("source", T.StringType(), True),
                        T.StructField("location_name", T.StringType(), True),
                        T.StructField("latitude", T.DoubleType(), True),
                        T.StructField("longitude", T.DoubleType(), True),
                        T.StructField("run_date", T.StringType(), True),
                        T.StructField("timezone", T.StringType(), True),
                    ]
                ),
                True,
            ),
            T.StructField("raw", T.MapType(T.StringType(), T.StringType()), True),
        ]
    )

    raw_df = spark.read.schema(schema).json(bronze_uri)

    # Turn metadata+raw into a single JSON string column
    row_json = F.to_json(F.struct(F.col("metadata"), F.col("raw"))).alias("row_json")
    dfj = raw_df.select(row_json)

    # Schema for the hourly block inside "raw.hourly"
    hourly_schema = T.StructType(
        [
            T.StructField("time", T.ArrayType(T.StringType()), True),
            T.StructField("temperature_2m", T.ArrayType(T.DoubleType()), True),
            T.StructField("relative_humidity_2m", T.ArrayType(T.DoubleType()), True),
            T.StructField("wind_speed_10m", T.ArrayType(T.DoubleType()), True),
            T.StructField("cloud_cover", T.ArrayType(T.DoubleType()), True),
            T.StructField("precipitation", T.ArrayType(T.DoubleType()), True),
            T.StructField("surface_pressure", T.ArrayType(T.DoubleType()), True),
        ]
    )

    # Pull metadata fields (if any rows exist)
    meta = (
        raw_df.select(
            F.col("metadata.location_name").alias("location_name"),
            F.col("metadata.latitude").alias("latitude"),
            F.col("metadata.longitude").alias("longitude"),
        )
        .limit(1)
        .collect()
    )

    location_name, latitude, longitude = (
        (meta[0].location_name, meta[0].latitude, meta[0].longitude)
        if meta
        else ("berlin", 52.52, 13.405)
    )

    # Extract "raw.hourly" JSON and parse into arrays
    hourly_json = F.get_json_object(dfj["row_json"], "$.raw.hourly")
    hourly = dfj.select(F.from_json(hourly_json, hourly_schema).alias("h")).select(
        "h.*"
    )

    # Explode arrays to one row per hour
    zipped = hourly.select(
        F.posexplode(F.col("time")).alias("idx", "time_str"),
        "temperature_2m",
        "relative_humidity_2m",
        "wind_speed_10m",
        "cloud_cover",
        "precipitation",
        "surface_pressure",
    )

    out = zipped.select(
        F.to_timestamp("time_str").alias("event_ts_utc"),
        F.to_date(F.to_timestamp("time_str")).alias("dt"),
        F.lit(location_name).alias("location_name"),
        F.lit(float(latitude)).alias("latitude"),
        F.lit(float(longitude)).alias("longitude"),
        F.element_at("temperature_2m", F.col("idx") + 1).alias("temperature_2m"),
        F.element_at("relative_humidity_2m", F.col("idx") + 1).alias(
            "relative_humidity_2m"
        ),
        F.element_at("wind_speed_10m", F.col("idx") + 1).alias("wind_speed_10m"),
        F.element_at("cloud_cover", F.col("idx") + 1).alias("cloud_cover"),
        F.element_at("precipitation", F.col("idx") + 1).alias("precipitation"),
        F.element_at("surface_pressure", F.col("idx") + 1).alias("surface_pressure"),
    ).filter(F.col("dt") == F.to_date(F.lit(run_date)))

    # Build expected hourly grid 00:00–23:00 for the day
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
        .withColumn("location_name", F.lit(location_name))
        .withColumn("latitude", F.lit(float(latitude)))
        .withColumn("longitude", F.lit(float(longitude)))
    )

    # Left join so missing hours show up as nulls
    final_df = expected.join(
        out,
        on=["event_ts_utc", "dt", "location_name", "latitude", "longitude"],
        how="left",
    )

    final_df.repartition(1).write.mode("overwrite").parquet(silver_uri)
    print(f"Silver weather written to: {silver_uri}")

    spark.stop()


if __name__ == "__main__":
    main()
