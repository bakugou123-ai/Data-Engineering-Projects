# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import functions as F

# COMMAND ----------

bronze_schema = "bronze"
silver_schema = "silver"
gold_schema = "gold"

# COMMAND ----------

print(bronze_schema,silver_schema,gold_schema)

# COMMAND ----------

base_path=f's3://sales-s3-master-bucket/orders/'
landing_path=f'{base_path}landing/'
processed_path=f'{base_path}processed_path/'

print(base_path,landing_path,processed_path)

# COMMAND ----------

bronze_table = "masterdata.bronze.orders"
silver_table = "masterdata.silver.orders"
gold_table = "masterdata.gold.orders"

# COMMAND ----------

df = spark.read.options(header=True, inferSchema=True).csv(f"{landing_path}/*.csv")

df.show(5)

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format("delta").option("delta.enableChangeDataFeed","true").mode("append").saveAsTable(f"masterdata.bronze.facts_table")

# COMMAND ----------

# MAGIC %md
# MAGIC EDA

# COMMAND ----------

display(df)

# COMMAND ----------

df.groupby("product_id").count().where("count>1").show()

# COMMAND ----------

df_orders = df.filter(F.col("order_qty").isNotNull())

df_orders = df.withColumn("customer_id", F.when(F.col("customer_id").rlike("^[0-9]+$"),F.col("customer_id")).otherwise("999999").cast("string"))

# COMMAND ----------

df_orders.show()

# COMMAND ----------

df_orders = df_orders.withColumn("order_placement_date", F.regexp_replace(F.col("order_placement_date"),r"^[A-Za-z]+,\s*",""))

# COMMAND ----------

df_orders.show()

# COMMAND ----------

df_orders = df_orders.withColumn("order_placement_date", F.coalesce(
F.try_to_date("order_placement_date","yyyy/MM/dd"),
F.try_to_date("order_placement_date","dd-MM-yyyy"),
F.try_to_date("order_placement_date","dd/MM/yyyy"),
F.try_to_date("order_placement_date","MMMM dd, yyyy"),
))

# COMMAND ----------

df_orders = df_orders.dropDuplicates(['order_id','order_placement_date','customer_id','product_id','order_qty'])

df_orders = df_orders.withColumn("product_id", F.col("product_id").cast("string"))

# COMMAND ----------

df_orders = df_orders.filter(F.col("order_qty").isNotNull())

# COMMAND ----------

display(df_orders)

# COMMAND ----------

df_products = spark.table("masterdata.silver.products")

# COMMAND ----------

display(df_products)

# COMMAND ----------

final_df = df_orders.join(df_products, on="product_id", how="inner").select(df_orders["*"],df_products["product_code"])

# COMMAND ----------

final_df.show()

# COMMAND ----------

if not (spark.catalog.tableExists(silver_table)):
    final_df.write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).option("mergeSchema", "true").mode("overwrite").saveAsTable(silver_table)
else:
    silver_delta = DeltaTable.forName(spark, silver_table)
    silver_delta.alias("silver").merge(final_df.alias("bronze"), "silver.order_placement_date = bronze.order_placement_date AND silver.order_id = bronze.order_id AND silver.product_code = bronze.product_code AND silver.customer_id = bronze.customer_id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC GOLD 

# COMMAND ----------

df_gold = spark.sql(f"SELECT order_id, order_placement_date as date, customer_id as customer_code, product_code, product_id, order_qty as sold_quantity FROM {silver_table};")

# COMMAND ----------

if not (spark.catalog.tableExists(gold_table)):
    print("creating New Table")
    df_gold.write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).option("mergeSchema", "true").mode("overwrite").saveAsTable(gold_table)
else:
    gold_delta = DeltaTable.forName(spark, gold_table)
    gold_delta.alias("source").merge(df_gold.alias("gold"), "source.date = gold.date AND source.order_id = gold.order_id AND source.product_code = gold.product_code AND source.customer_code = gold.customer_code").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC MERGE WITH PARENT TABLE 

# COMMAND ----------

df_child = spark.sql(f"SELECT date, product_code, customer_code, sold_quantity FROM {gold_table}")
df_child.show()

# COMMAND ----------

df_monthly = df_child.withColumn("month_start", F.trunc("date","MM")).groupby("month_start","product_code","customer_code").agg(F.sum("sold_quantity").alias("sold_quantity")).withColumnRenamed("month_start","date")

# COMMAND ----------

df_monthly.show()

# COMMAND ----------

gold_parent_delta = DeltaTable.forName(spark, "masterdata.gold.fact_orders")
gold_parent_delta.alias("parent_gold").merge(df_monthly.alias("child_gold"),"parent_gold.date = child_gold.date AND parent_gold.product_code = child_gold.product_code AND parent_gold.customer_code = child_gold.customer_code").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
