# Pipeline

A bronze → silver → gold ETL pipeline that builds a star-schema sales data warehouse from raw
S3 source files. Stages run in numeric order:

| Stage | Description |
|---|---|
| [1_dim_data_preprocessing](./1_dim_data_preprocessing) | Builds the `customers`, `products`, and `gross_price` dimension tables. |
| [2_setup_folder/dim_creation](./2_setup_folder/dim_creation) | Generates the `date` dimension table. |
| [3_fact_data_preprocessing](./3_fact_data_preprocessing) | Builds the `orders` fact table, with both full-load and incremental-load entry points. |
