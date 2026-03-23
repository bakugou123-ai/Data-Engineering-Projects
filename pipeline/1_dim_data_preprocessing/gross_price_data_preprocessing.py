# Databricks notebook source
base_path = 's3://sales-s3-master-bucket/gross_price/gross_price.csv'

# COMMAND ----------

df= spark.read.format('csv').option("inferSchema",True).load(base_path,header=True)

display(df)

# COMMAND ----------

df.write.format('delta').option("delta.enablechangeDataFeed","true").mode("overwrite").saveAsTable(f"masterdata.bronze.gross_price")

# COMMAND ----------



# COMMAND ----------

df_silver = spark.sql(f"select * from masterdata.bronze.gross_price")


# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

date_formats = ['yyyy/MM/dd', 'dd/MM/yyyy','yyyy-MM-dd','dd-MM-YYYY']

df_silver = df_silver.withColumn('month',F.coalesce(
F.try_to_date(F.col('month'),'yyyy/MM/dd'),
F.try_to_date(F.col('month'),'dd/MM/yyyy'),
F.try_to_date(F.col('month'),'yyyy-MM-dd'),
F.try_to_date(F.col('month'),'dd-MM-yyyy')
))


# COMMAND ----------

display(df_silver)

# COMMAND ----------

df_silver = df_silver.withColumn("gross_price",F.when(F.col("gross_price").rlike(r'^-?\d+(\.\d+)?$'),
                                                      F.when(F.col("gross_price").cast("double")<0,-1*F.col("gross_price").cast("double")).otherwise(F.col("gross_price").cast("double")
                                                      )).otherwise(0))

# COMMAND ----------

df_products = spark.table("masterdata.silver.products")
df_joined = df_silver.join(df_products.select("product_id","product_code"), on="product_id", how="inner")
df_joined = df_joined.select("product_id","product_code","month","gross_price")
df_joined.show(5)

# COMMAND ----------

df_joined.write.format('delta').option("delta.enablechangeDataFeed","true").mode("overwrite").saveAsTable(f"masterdata.silver.gross_price")

# COMMAND ----------

df_silver = spark.sql(f"SELECT * FROM masterdata.silver.gross_price")

# COMMAND ----------

df_gold = df_silver.select("product_code","month","gross_price")

# COMMAND ----------

df_gold.write.format('delta').option("delta.enablechangeDataFeed","true").mode("overwrite").saveAsTable(f"masterdata.gold.sb_dim_gross_price")