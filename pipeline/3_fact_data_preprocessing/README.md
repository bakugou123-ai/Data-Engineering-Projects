# Orders Fact Table Preprocessing — Full Load & Incremental Load

**Scripts:** [`full_load/fact_data_preprocessing.py`](./full_load/fact_data_preprocessing.py) ·
[`incremental_load/incremental_load_preprocessing.py`](./incremental_load/incremental_load_preprocessing.py)
**Tools:** Databricks · PySpark · Delta Lake · AWS S3

## Objective

Build the `orders` fact table at the center of the star schema, supporting both a one-time full
historical load and an ongoing incremental (CDC-style) load as new order files arrive.

## Full Load (`full_load/fact_data_preprocessing.py`)

Reads all historical order CSVs and appends them to a bronze Delta table, then cleans the data:

- Replaces invalid, non-numeric `customer_id` values with a placeholder (`999999`)
- Strips weekday-name prefixes from date strings (e.g. `"Tuesday, July 01, 2025"`)
- Parses multiple date formats with `try_to_date` + `coalesce`
- Deduplicates on the natural order key (`order_id`, date, customer, product, quantity)

The cleaned orders are joined against the products dimension to resolve `product_code`, written
to a silver table, projected into a gold `orders` fact table, and finally aggregated to the
parent warehouse's **monthly grain** (`fact_orders`) via a Delta `MERGE`.

## Incremental Load (`incremental_load/incremental_load_preprocessing.py`)

The production/ongoing version of the same pipeline:

1. New files are read from a `landing/` path, timestamped, appended to bronze, and staged.
2. Processed files are moved from `landing/` to `processed/` via `dbutils.fs.mv` so they aren't
   reprocessed on the next run.
3. The same cleaning/parsing logic as the full load is applied to the staged batch, which is
   merged into the shared silver/gold `orders` tables.
4. Because the parent fact table (`fact_orders`) is stored at **monthly** grain while incoming
   data is **daily**, the pipeline identifies which months were touched by the incremental
   batch, re-aggregates *only* those months from the gold `orders` table, and merges the
   recalculated monthly totals back into the parent table — avoiding a full table rebuild.
5. Staging tables are dropped at the end of the run.

## Techniques Demonstrated

Bronze/silver/gold incremental (CDC-style) loading, staging tables, file lifecycle management
(landing → processed), Delta Lake `MERGE INTO` upserts, selective re-aggregation of only the
affected partitions/months, multi-format date parsing, and data validation/cleansing.
