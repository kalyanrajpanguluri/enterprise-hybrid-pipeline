# Databricks PySpark Delta Lake ETL Job
# Runs in Databricks Community Edition or Databricks Workflows

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, sum as _sum, count as _count, avg as _avg, current_timestamp, date_format
from delta.tables import DeltaTable

def run_databricks_delta_pipeline(s3_silver_path, s3_gold_delta_path):
    """
    Databricks High-Performance Heavy Delta Lake Processing:
    Reads Silver Parquet data from S3, converts to Delta format, applies Z-Ordering,
    and writes Gold aggregated business metrics to S3 Delta Lake.
    """
    spark = SparkSession.builder \
        .appName("DatabricksDeltaPipeline") \
        .getOrCreate()

    print(f"Reading Silver Parquet Dataset from: {s3_silver_path}")
    silver_df = spark.read.parquet(s3_silver_path)

    # 1. Compute Customer Aggregations in Databricks PySpark
    gold_summary_df = silver_df.groupBy("customer_name").agg(
        _count("id").alias("total_orders"),
        round(_sum("total_price"), 2).alias("total_spent"),
        round(_avg("total_price"), 2).alias("avg_order_value"),
        date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("last_updated")
    )

    # 2. Write to S3 in Delta Lake Format
    print(f"Writing Gold Aggregates to S3 Delta Lake Path: {s3_gold_delta_path}")
    gold_summary_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(s3_gold_delta_path)

    # 3. Apply Databricks Delta Lake Optimizations (Compaction & Z-Ordering)
    print("Applying Delta Lake Optimization & Z-Ordering...")
    delta_table = DeltaTable.forPath(spark, s3_gold_delta_path)
    delta_table.optimize().executeZOrder("customer_name")

    print("=== DATABRICKS DELTA PIPELINE COMPLETED SUCCESSFULLY ===")

# Example Invocation:
# run_databricks_delta_pipeline(
#     s3_silver_path="s3://enterprise-hybrid-ingest-dev-xxxxxx/silver/sales_enriched/",
#     s3_gold_delta_path="s3://enterprise-hybrid-ingest-dev-xxxxxx/gold/delta_customer_summary/"
# )
