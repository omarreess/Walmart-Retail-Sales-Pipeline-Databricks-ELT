# Walmart Retail Sales Pipeline — Medallion Architecture ELT

## Description

An end-to-end ELT data pipeline built on Databricks that processes Walmart grocery sales data using the Medallion Architecture (Bronze → Silver → Gold). The pipeline ingests raw sales and store-level economic data from multiple sources, merges and cleans the data through structured layers, and produces business-ready aggregated insights stored as Delta Lake tables.

built with PySpark and Delta Lake to demonstrate scalable data engineering practices on a lakehouse platform.

## What This Project Does

- **Ingests** grocery sales (CSV) and store/economic indicators (Parquet) into a Bronze layer as raw Delta tables
- **Transforms** by joining datasets, parsing dates, handling missing values, and filtering for high-performing sales into a Silver layer
- **Aggregates** monthly average sales metrics into a Gold Mart layer ready for dashboards and BI tools
- **Validates** each layer to ensure data integrity across the pipeline

## Tech Stack

- **Apache Spark (PySpark)** — distributed data processing
- **Delta Lake** — ACID-compliant lakehouse storage format
- **Databricks Community Edition** — cloud compute platform
- **Medallion Architecture** — Bronze / Silver / Gold data organization pattern

## Data Flow

```
grocery_sales.csv ──→ bronze_grocery_sales ──┐
                                              ├──→ silver_clean_sales ──→ gold_monthly_sales
extra_data.parquet ──→ bronze_extra_data ────┘
      (raw files)          (Bronze)              (Silver)                  (Gold/Mart)
```

## Medallion Layers
<img width="386" height="439" alt="Screenshot 2026-02-23 at 7 33 12 AM" src="https://github.com/user-attachments/assets/456988b9-c84a-4fda-b6e2-c1da788f61d4" />


| Layer | Table | Rows | Description |
|-------|-------|------|-------------|
| Bronze | `bronze_grocery_sales` | 20,000 | Raw CSV data — store sales by department and week |
| Bronze | `bronze_extra_data` | 231,522 | Raw Parquet data — holidays, CPI, fuel prices, markdowns |
| Silver | `silver_clean_sales` | ~106,000 | Merged, cleaned, filtered (Weekly_Sales > $10K) |
| Gold | `gold_monthly_sales` | 12 | Average weekly sales per month (Mart) |
