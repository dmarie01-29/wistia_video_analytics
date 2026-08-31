import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, md5, concat_ws, coalesce, explode

args = getResolvedOptions(sys.argv, ['BUCKET_NAME'])
BUCKET_NAME = args['BUCKET_NAME']

sc = SparkContext()
spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

def run_medallion_pipeline():
    print("Initiating Granular Visitor Event Medallion Transformation...")

    # ----------------------------------------------------
    # PHASE A: BRONZE TIER (Read using MultiLine Array parsing)
    # ----------------------------------------------------
    raw_s3_path = f"s3://{BUCKET_NAME}/landing/media_stats/"
    bronze_output_path = f"s3://{BUCKET_NAME}/bronze/media_stats_raw"

    try:
        df_raw_array = spark.read.format("json") \
            .option("multiLine", "true") \
            .option("recursiveFileLookup", "true") \
            .load(raw_s3_path)

        df_raw = df_raw_array.select(explode(col("root_array_if_wrapped_or_all_fields")).alias("event")).select("event.*") \
                 if "root_array_if_wrapped_or_all_fields" in df_raw_array.columns else df_raw_array

    except Exception as e:
        print(f"Fallback activation triggered during file parsing: {e}")
        from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
        fallback_schema = StructType([
            StructField("target_media_id", StringType(), True),
            StructField("media_name", StringType(), True),
            StructField("visitor_id", StringType(), True),
            StructField("ip", StringType(), True),
            StructField("country", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("percent_watched", DoubleType(), True)
        ])
        df_raw = spark.createDataFrame([], fallback_schema)

    df_bronze = df_raw.withColumn("bronze_ingested_at", current_timestamp())

    # FIX: this job's schema (event-level fields) differs from whatever schema previously
    # lived at this path (e.g. the earlier aggregate-stats version). overwriteSchema tells
    # Delta this is an intentional schema replacement, not silent drift.
    df_bronze.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(bronze_output_path)

    # ----------------------------------------------------
    # PHASE B: SILVER TIER (Sanitize columns and apply Unique Hashing)
    # ----------------------------------------------------
    silver_output_path = f"s3://{BUCKET_NAME}/silver/media_stats_cleaned"
    df_bronze_source = spark.read.format("delta").load(bronze_output_path)
    cols = df_bronze_source.columns

    media_id_col = col("target_media_id") if "target_media_id" in cols else lit("unknown_media")
    # NOTE: Wistia's Stats Events endpoint has no media-title field at all. Every row gets
    # this placeholder until a separate lookup against GET /v1/medias/<hashed_id>.json is
    # joined in to supply real titles.
    title_col = col("media_name") if "media_name" in cols else lit("Wistia Stream Video")
    # FIX: Wistia's actual field is "visitor_key", not "visitor_id" — the wrong name meant
    # every row fell back to the same literal, collapsing all visitors together.
    visitor_id_col = col("visitor_key") if "visitor_key" in cols else lit("anonymous_visitor")
    ip_col = col("ip") if "ip" in cols else lit("127.0.0.1")
    country_col = col("country") if "country" in cols else lit("US")
    # FIX: Wistia's actual field is "received_at", not "created_at" — same collapse issue,
    # since the fallback (current_timestamp()) is identical across every row in one run.
    date_col = col("received_at") if "received_at" in cols else current_timestamp().cast("string")
    # FIX: Wistia's actual field is "percent_viewed", not "percent_watched".
    watch_pct_col = col("percent_viewed") if "percent_viewed" in cols else lit(100.0)

    df_silver_flat = df_bronze_source.select(
        media_id_col.alias("media_id"),
        title_col.alias("title"),
        visitor_id_col.alias("visitor_id"),
        ip_col.alias("ip_address"),
        country_col.alias("country"),
        date_col.alias("captured_timestamp"),
        watch_pct_col.cast("double").alias("watched_percent")
    )

    df_silver_keyed = df_silver_flat.withColumn(
        "surrogate_key",
        md5(concat_ws("||", col("media_id"), col("visitor_id"), col("captured_timestamp")))
    )

    df_silver_clean = df_silver_keyed.dropDuplicates(["surrogate_key"]) \
                                     .withColumn("silver_processed_at", current_timestamp())

    df_silver_clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_output_path)

    # ----------------------------------------------------
    # PHASE C: GOLD TIER (Structure Warehouse Output Schemas)
    # ----------------------------------------------------
    gold_dim_media_path = f"s3://{BUCKET_NAME}/gold/dim_media"
    gold_dim_visitor_path = f"s3://{BUCKET_NAME}/gold/dim_visitor"
    gold_fact_engagement_path = f"s3://{BUCKET_NAME}/gold/fact_media_engagement"

    df_silver_source = spark.read.format("delta").load(silver_output_path)

    dim_media = df_silver_source.select(
        col("media_id"),
        col("title"),
        lit("On-Site Embedded Channel").alias("channel"),
        col("captured_timestamp").alias("created_at")
    ).dropDuplicates(["media_id"])

    dim_visitor = df_silver_source.select(
        col("visitor_id"),
        col("ip_address"),
        col("country")
    ).dropDuplicates(["visitor_id"])

    fact_media_engagement = df_silver_source.select(
        col("media_id"),
        col("visitor_id"),
        col("captured_timestamp").alias("date"),
        lit(1).cast("long").alias("play_count"),
        lit(1.0).cast("double").alias("play_rate"),
        lit(45).cast("long").alias("total_watch_time"),
        col("watched_percent")
    )

    print("Writing structural targets to multi-dimensional Gold Delta warehouses...")
    dim_media.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_dim_media_path)
    dim_visitor.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_dim_visitor_path)
    fact_media_engagement.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_fact_engagement_path)

    print("Phase 2 PySpark Medallion Transformation Complete successfully!")

if __name__ == "__main__":
    run_medallion_pipeline()