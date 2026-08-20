# Dimension Data Preprocessing

**Scripts:** [`customer_data_preprocessing.py`](./customer_data_preprocessing.py) ·
[`products_data_preprocesing.py`](./products_data_preprocesing.py) ·
[`gross_price_data_preprocessing.py`](./gross_price_data_preprocessing.py)
**Tools:** Databricks · PySpark · Delta Lake · AWS S3

## Objective

Transform raw dimension source files (customers, products, gross price) landed in S3 into
clean, deduplicated, standardized Delta tables through a bronze → silver → gold medallion
pipeline, ready for the star-schema warehouse.

## Customers

- Reads raw customer CSV from S3 into a **bronze** Delta table.
- Deduplicates on `customer_id`, trims whitespace from names.
- Standardizes inconsistent city name variants via an explicit mapping
  (e.g. `"Bengalore"`, `"Bengaluruu"` → `"Bengaluru"`) and nulls out any city not on an
  allow-list.
- Backfills a handful of known customer IDs with corrected cities via a lookup join.
- Casts `customer_id` to `bigint` and derives a composite `customer` key plus static
  `market`/`platform`/`channel` attributes.
- Writes to **silver**, then a **gold** table, and merges into the shared `dim_customers`
  table with a Delta `MERGE` (`whenMatchedUpdateAll` / `whenNotMatchedInsertAll`).

## Products

- Reads raw product CSV into **bronze**, deduplicates, and standardizes category values
  (casing + regex fix for the misspelling `"Protien"` → `"Protein"`).
- Corrects a known bad `product_id` value and derives a business `division` from `category`
  via a lookup table.
- Extracts a `variant` from the product name using regex, and generates a stable
  `product_code` surrogate key via SHA-256 hashing of the product name.
- Writes to **silver**, then **gold**, and merges into `dim_products` with an explicit
  matched/unmatched column-mapped Delta `MERGE`.

## Gross Price

- Reads raw pricing data with schema inference into **bronze**.
- Parses multiple inconsistent date formats (`yyyy/MM/dd`, `dd/MM/yyyy`, `yyyy-MM-dd`,
  `dd-MM-yyyy`) using `try_to_date` + `coalesce`.
- Validates price values with regex, converting negative price strings to positive floats and
  defaulting invalid values to `0`.
- Joins against the cleaned products dimension to resolve `product_code`, then writes to
  **silver** and **gold**.

## Techniques Demonstrated

PySpark DataFrame transformations, `dropDuplicates`, `regexp_replace` / `regexp_extract`,
multi-format date parsing, SHA-256 surrogate key generation, Delta Lake `MERGE INTO` upserts,
and bronze/silver/gold medallion layering.
