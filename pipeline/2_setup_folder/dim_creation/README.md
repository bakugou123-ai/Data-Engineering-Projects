# Date Dimension Creation

**Script:** [`dim_data_table_creation.py`](./dim_data_table_creation.py)
**Tools:** Databricks · PySpark · Spark SQL · Delta Lake

## Objective

Generate a reusable monthly date dimension (`dim_date`) for the warehouse, spanning a
configured date range, so fact tables can join against a standard calendar dimension instead
of computing date attributes ad hoc.

## Approach

Uses Spark SQL's `sequence()` + `explode()` to generate one row per month between a
configurable `start_date` and `end_date`, then derives standard date-dimension attributes for
each row:

- `date_key` — a `yyyyMM`-format surrogate key
- `year`
- `month_name` / `month_short_name`
- `quarter` and `year_quarter` (e.g. `2024-Q1`)

The result is written as a Delta table (`masterdata.gold.dim_date`).

## Techniques Demonstrated

Spark SQL date-sequence generation, calendar/date-dimension modeling, and Delta table creation.
