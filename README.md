# Sales Data Engineering Pipeline

A Databricks/PySpark ETL pipeline that ingests raw sales data from AWS S3 and builds a
star-schema data warehouse using the **medallion architecture** (bronze → silver → gold) on
Delta Lake, supporting both full historical loads and ongoing incremental (CDC-style) loads.

## Architecture

```
S3 (raw CSVs)
   │
   ▼
Bronze   — raw ingest, append-only Delta tables
   │
   ▼
Silver   — cleaned, deduplicated, standardized, type-cast
   │
   ▼
Gold     — star-schema dimension & fact tables, merged into the
           parent warehouse via Delta Lake MERGE (upsert)
```

## Pipeline Stages

| Stage | Description |
|---|---|
| [Dimension Data Preprocessing](./pipeline/1_dim_data_preprocessing) | Builds the `customers`, `products`, and `gross_price` dimension tables — deduplication, category/city standardization, surrogate key generation (SHA-256), and multi-format date parsing. |
| [Date Dimension Creation](./pipeline/2_setup_folder/dim_creation) | Generates a monthly `date` dimension table from a configured date range using Spark SQL. |
| [Orders Fact Table Preprocessing](./pipeline/3_fact_data_preprocessing) | Builds the `orders` fact table, with a **full-load** path for historical backfill and an **incremental-load** path that ingests new daily files, re-aggregates only affected months, and merges them into the parent monthly fact table. |

## Tech Stack

- **Databricks** (notebook-based PySpark jobs)
- **PySpark** — DataFrame transformations, Spark SQL
- **Delta Lake** — ACID tables, Change Data Feed, `MERGE INTO` upserts
- **AWS S3** — raw data lake source

## Skills Demonstrated

- Medallion (bronze/silver/gold) architecture design
- Star-schema (dimension/fact) data modeling
- Data quality & standardization: deduplication, fuzzy value correction (e.g. inconsistent
  city name spellings), regex-based cleansing, multi-format date parsing
- Surrogate key generation via SHA-256 hashing
- Delta Lake `MERGE INTO` upsert patterns for both dimension and fact tables
- Incremental / CDC-style loading with staging tables and file lifecycle management
  (landing → processed)
- Aggregating daily-grain data to a monthly-grain parent fact table, with selective
  re-aggregation of only affected partitions

## Repository Structure

```
data-engineering/
└── pipeline/
    ├── 1_dim_data_preprocessing/
    │   ├── customer_data_preprocessing.py
    │   ├── products_data_preprocesing.py
    │   ├── gross_price_data_preprocessing.py
    │   └── README.md
    ├── 2_setup_folder/
    │   └── dim_creation/
    │       ├── dim_data_table_creation.py
    │       └── README.md
    ├── 3_fact_data_preprocessing/
    │   ├── full_load/
    │   │   └── fact_data_preprocessing.py
    │   ├── incremental_load/
    │   │   └── incremental_load_preprocessing.py
    │   └── README.md
    └── README.md
```

## Contact

**Sahil Mane** — Data Engineer & Data Analyst
[LinkedIn](https://www.linkedin.com/in/sahil-mane-502a161aa) · [GitHub](https://github.com/bakugou123-ai)
