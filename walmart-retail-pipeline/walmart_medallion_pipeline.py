# Databricks notebook source
# MAGIC %md
# MAGIC # Walmart Retail Sales Pipeline — Medallion Architecture
# MAGIC **ELT pipeline using Bronze → Silver → Gold layers with Delta tables.**
# MAGIC
# MAGIC | Layer | Purpose | Table |
# MAGIC |-------|---------|-------|
# MAGIC | Bronze | Raw data ingestion | `bronze_grocery_sales`, `bronze_extra_data` |
# MAGIC | Silver | Merged, cleaned, filtered | `silver_clean_sales` |
# MAGIC | Gold | Business-ready aggregation (Mart) | `gold_monthly_sales` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 - Configuration

# COMMAND ----------

# ── Source file paths ──────────────────────────────────────
GROCERY_CSV_PATH   = "/Workspace/walmart-retail-pipeline/grocery_sales.csv"
EXTRA_PARQUET_PATH = "/Workspace/walmart-retail-pipeline/extra_data.parquet"

# ── Database name ─────────────────────────────────────────
DATABASE = "walmart_retail"

# ── Table names ───────────────────────────────────────────
BRONZE_GROCERY   = f"{DATABASE}.bronze_grocery_sales"
BRONZE_EXTRA     = f"{DATABASE}.bronze_extra_data"
SILVER_CLEAN     = f"{DATABASE}.silver_clean_sales"
GOLD_MONTHLY     = f"{DATABASE}.gold_monthly_sales"

print("Configuration loaded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 - Create Database

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
spark.sql(f"USE {DATABASE}")
print(f"Using database: {DATABASE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 - BRONZE Layer (Raw Ingestion)
# MAGIC Load raw data as-is into Delta tables. No cleaning, no transformations.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 - Ingest Grocery Sales

# COMMAND ----------

import pandas as pd

# Read with pandas (Workspace paths not accessible by Spark directly)
grocery_pdf = pd.read_csv(GROCERY_CSV_PATH)

# Convert to Spark DataFrame
bronze_grocery = spark.createDataFrame(grocery_pdf)

# Write to Delta table
bronze_grocery.write.format("delta").mode("overwrite").saveAsTable(BRONZE_GROCERY)

print(f"[BRONZE] {BRONZE_GROCERY} — {bronze_grocery.count():,} rows")
bronze_grocery.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 - Ingest Extra Data

# COMMAND ----------

# Read with pandas
extra_pdf = pd.read_parquet(EXTRA_PARQUET_PATH)

# Convert to Spark DataFrame
bronze_extra = spark.createDataFrame(extra_pdf)

# Write to Delta table
bronze_extra.write.format("delta").mode("overwrite").saveAsTable(BRONZE_EXTRA)

print(f"[BRONZE] {BRONZE_EXTRA} — {bronze_extra.count():,} rows")
bronze_extra.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 - Verify Bronze Layer

# COMMAND ----------

print("Bronze tables:")
spark.sql(f"SHOW TABLES IN {DATABASE} LIKE 'bronze_*'").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 - SILVER Layer (Clean & Transform)
# MAGIC Read from Bronze, merge, clean, and store as a curated Delta table.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 - Read from Bronze

# COMMAND ----------

# Read from Bronze Delta tables (not raw files anymore)
bronze_grocery = spark.table(BRONZE_GROCERY)
bronze_extra   = spark.table(BRONZE_EXTRA)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 - Merge Datasets

# COMMAND ----------

merged_df = bronze_grocery.join(bronze_extra, on="index", how="inner")
print(f"Merged rows: {merged_df.count():,}")
merged_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 - Transform

# COMMAND ----------

from pyspark.sql import functions as F

def transform(raw_data):
    """Clean and transform the merged data.
    
    Steps:
      1. Parse Date and extract Month
      2. Fill missing CPI and Unemployment with mean
      3. Filter Weekly_Sales > 10,000
      4. Select required columns
    """
    # Parse date and extract month
    df = raw_data.withColumn("Date", F.to_timestamp("Date"))
    df = df.withColumn("Month", F.month("Date"))

    # Calculate means for filling nulls
    cpi_mean = df.select(F.mean("CPI")).first()[0]
    unemp_mean = df.select(F.mean("Unemployment")).first()[0]

    # Fill missing values
    df = df.fillna({"CPI": cpi_mean, "Unemployment": unemp_mean})

    # Filter high-performing sales
    df = df.filter(F.col("Weekly_Sales") > 10000)

    # Select required columns
    df = df.select(
        "Store_ID",
        "Month",
        "Dept",
        "IsHoliday",
        "Weekly_Sales",
        "CPI",
        "Unemployment"
    )

    return df

silver_clean = transform(merged_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 - Write Silver Table

# COMMAND ----------

silver_clean.write.format("delta").mode("overwrite").saveAsTable(SILVER_CLEAN)

print(f"[SILVER] {SILVER_CLEAN} — {silver_clean.count():,} rows")
silver_clean.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 - GOLD Layer — Mart (Business-Ready)
# MAGIC Aggregated metrics ready for dashboards and reporting.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 - Read from Silver

# COMMAND ----------

# Read from Silver Delta table (not raw DataFrames)
silver_data = spark.table(SILVER_CLEAN)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 - Aggregate Monthly Sales

# COMMAND ----------

gold_monthly = (
    silver_data
    .groupBy("Month")
    .agg(F.round(F.mean("Weekly_Sales"), 2).alias("Avg_Sales"))
    .orderBy("Month")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 - Write Gold Mart Table

# COMMAND ----------

gold_monthly.write.format("delta").mode("overwrite").saveAsTable(GOLD_MONTHLY)

print(f"[GOLD/MART] {GOLD_MONTHLY} — {gold_monthly.count()} rows")
gold_monthly.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 - Validate

# COMMAND ----------

def validate_table(table_name):
    """Check that a Delta table exists and has rows."""
    try:
        count = spark.table(table_name).count()
        print(f"[OK]     {table_name} — {count:,} rows")
        return True
    except Exception as e:
        print(f"[FAILED] {table_name} — {e}")
        return False

print("Validating all layers...")
print("=" * 50)
validate_table(BRONZE_GROCERY)
validate_table(BRONZE_EXTRA)
validate_table(SILVER_CLEAN)
validate_table(GOLD_MONTHLY)
print("=" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5 - Query the Mart
# MAGIC The Gold table is now queryable by anyone — analysts, dashboards, BI tools.

# COMMAND ----------

spark.sql(f"SELECT * FROM {GOLD_MONTHLY} ORDER BY Month").show()

# COMMAND ----------

# Bonus: query Silver directly with SQL
spark.sql(f"""
    SELECT Store_ID, Month, 
           ROUND(AVG(Weekly_Sales), 2) AS Avg_Sales,
           COUNT(*) AS Num_Records
    FROM {SILVER_CLEAN}
    GROUP BY Store_ID, Month
    ORDER BY Store_ID, Month
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6 - Pipeline Summary

# COMMAND ----------

print("=" * 55)
print("  WALMART RETAIL PIPELINE — MEDALLION ARCHITECTURE")
print("=" * 55)
print(f"  BRONZE  bronze_grocery_sales : {spark.table(BRONZE_GROCERY).count():>10,} rows")
print(f"  BRONZE  bronze_extra_data    : {spark.table(BRONZE_EXTRA).count():>10,} rows")
print(f"  SILVER  silver_clean_sales   : {spark.table(SILVER_CLEAN).count():>10,} rows")
print(f"  GOLD    gold_monthly_sales   : {spark.table(GOLD_MONTHLY).count():>10,} rows")
print("=" * 55)
print("  All tables stored as DELTA in walmart_retail DB")
print("  Pipeline completed successfully!")
print("=" * 55)