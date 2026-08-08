import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, current_timestamp, date_format, round, sum as _sum, count as _count, avg as _avg, coalesce, lit, trim
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("MedallionGlueETLJob")

def main():
    # 1. Parse job arguments passed from EventBridge / Glue Default Arguments
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'S3_INPUT_PATH',            # e.g., s3://bucket-name/raw/ or s3://bucket-name/
        'S3_SILVER_PATH',           # e.g., s3://bucket-name/silver/sales_enriched/
        'S3_GOLD_PATH',             # e.g., s3://bucket-name/gold/customer_summary/
        'S3_PRODUCTS_CATALOG_PATH'  # e.g., s3://asset-bucket-name/data/products.csv
    ])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    s3_input_path = args['S3_INPUT_PATH']
    s3_silver_path = args['S3_SILVER_PATH']
    s3_gold_path = args['S3_GOLD_PATH']
    s3_products_catalog_path = args['S3_PRODUCTS_CATALOG_PATH']

    logger.info("=== STARTING MEDALLION GLUE ETL JOB ===")
    logger.info(f"Raw Input Path: {s3_input_path}")
    logger.info(f"Silver Parquet Destination: {s3_silver_path}")
    logger.info(f"Gold Parquet Destination: {s3_gold_path}")
    logger.info(f"Product Catalog Lookup: {s3_products_catalog_path}")

    try:
        # ---------------------------------------------------------------------
        # STEP 1: RAW LAYER (Ingest Raw Sales File or Folder)
        # ---------------------------------------------------------------------
        raw_sales_df = None

        if s3_input_path.endswith('.json'):
            raw_sales_df = spark.read.json(s3_input_path)
        elif s3_input_path.endswith('.csv'):
            raw_sales_df = spark.read.option("header", "true").option("inferSchema", "true").csv(s3_input_path)
        else:
            # Check raw/ subfolder first, then bucket root *.csv
            try:
                raw_sales_df = spark.read.option("header", "true").option("inferSchema", "true").csv(s3_input_path.rstrip('/') + "/raw/")
            except Exception:
                try:
                    raw_sales_df = spark.read.option("header", "true").option("inferSchema", "true").csv(s3_input_path.rstrip('/') + "/*.csv")
                except Exception as read_err:
                    logger.warning(f"[RAW LAYER] No input CSV files found at {s3_input_path}: {str(read_err)}")
                    job.commit()
                    return

        if raw_sales_df is None:
            logger.warning("[RAW LAYER] Could not initialize DataFrame from input path. Exiting.")
            job.commit()
            return

        raw_count = raw_sales_df.count()
        logger.info(f"[RAW LAYER] Successfully ingested {raw_count} raw order records.")

        if raw_count == 0:
            logger.warning("[RAW LAYER] Input file is empty. Exiting.")
            job.commit()
            return

        # ---------------------------------------------------------------------
        # STEP 2: TRANSFORMATIONS & DATA CLEANING (Dynamic Column Mapping)
        # ---------------------------------------------------------------------
        cols = raw_sales_df.columns

        # Dynamically map flexible column aliases
        customer_col = "customer_name" if "customer_name" in cols else ("customer_id" if "customer_id" in cols else "id")
        status_col = "status" if "status" in cols else ("category" if "category" in cols else lit("COMPLETED"))
        quantity_expr = col("quantity").cast("integer") if "quantity" in cols else lit(1)
        amount_expr = col("amount").cast("double") if "amount" in cols else lit(0.0)
        id_col = "id" if "id" in cols else customer_col

        cleaned_sales_df = raw_sales_df \
            .withColumn("id", col(id_col)) \
            .withColumn("product", trim(col("product"))) \
            .withColumn("customer_name", trim(col(customer_col))) \
            .withColumn("amount", amount_expr) \
            .withColumn("quantity", quantity_expr) \
            .withColumn("status", col(status_col)) \
            .withColumn("total_price", round(col("amount") * col("quantity"), 2)) \
            .withColumn("processed_at", date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

        # ---------------------------------------------------------------------
        # STEP 3: READ LOOKUP DATASET & PERFORM PYSPARK JOIN
        # ---------------------------------------------------------------------
        products_df = spark.read.option("header", "true").option("inferSchema", "true").csv(s3_products_catalog_path) \
            .withColumn("product", trim(col("product")))

        # Left outer join sales orders with product catalog metadata
        silver_df = cleaned_sales_df.join(
            products_df,
            on="product",
            how="left"
        ).select(
            col("id"),
            col("customer_name"),
            col("product"),
            coalesce(products_df["category"], cleaned_sales_df["status"], lit("General")).alias("category"),
            coalesce(products_df["supplier"], lit("Unknown")).alias("supplier"),
            col("amount"),
            col("quantity"),
            col("total_price"),
            col("status"),
            col("processed_at")
        ).fillna({
            "category": "Uncategorized",
            "supplier": "Unknown"
        })

        logger.info("[SILVER LAYER] Successfully applied transformations & joined sales with product catalog.")

        # ---------------------------------------------------------------------
        # STEP 4: WRITE TO SILVER LAYER (S3 in Parquet Format, Partitioned by Status)
        # ---------------------------------------------------------------------
        silver_df.write \
            .mode("overwrite") \
            .partitionBy("status") \
            .parquet(s3_silver_path)

        logger.info(f"[SILVER LAYER] Wrote partitioned Parquet dataset to S3: {s3_silver_path}")

        # ---------------------------------------------------------------------
        # STEP 5: GOLD LAYER (PySpark Aggregations Stored as Parquet in S3)
        # ---------------------------------------------------------------------
        customer_summary_df = silver_df.groupBy("customer_name").agg(
            _count("id").alias("total_orders"),
            round(_sum("total_price"), 2).alias("total_spent"),
            round(_avg("total_price"), 2).alias("avg_order_value"),
            date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("last_updated")
        )

        gold_count = customer_summary_df.count()
        logger.info(f"[GOLD LAYER] Computed aggregates for {gold_count} unique customers.")

        # Write Gold Aggregate dataset to S3 in Snappy Parquet format
        customer_summary_df.write \
            .mode("overwrite") \
            .parquet(s3_gold_path)

        logger.info(f"[GOLD LAYER] Successfully wrote {gold_count} customer aggregate Parquet records to S3: {s3_gold_path}")

    except Exception as e:
        logger.error(f"[ETL FAILURE] Error executing Medallion Glue ETL job: {str(e)}", exc_info=True)
        raise e

    # Commit job execution state
    job.commit()

if __name__ == "__main__":
    main()
