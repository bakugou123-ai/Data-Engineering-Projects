# Databricks notebook source
base_path= 's3://sales-s3-master-bucket/products/products.csv'


# COMMAND ----------

df = spark.read.format('csv').load(base_path,header=True)

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write.format('delta').option("data.enablechangeDataFeed","true").mode("overwrite").saveAsTable(f"masterdata.bronze.products")

# COMMAND ----------

df_bronze = spark.sql(f"SELECT * FROM masterdata.bronze.products")

# COMMAND ----------

display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC DROP DUPLICATES

# COMMAND ----------

df_bronze.groupBy("product_id").count().where("count > 1").display()

# COMMAND ----------

df_silver = df_bronze.dropDuplicates()

# COMMAND ----------

df_silver.select("category").distinct().show()

# COMMAND ----------

from pyspark.sql import functions as F;
from delta.tables import DeltaTable;

# COMMAND ----------

df_silver = df_silver.withColumn("category",F.when(F.col("category").isNull(),None).otherwise(F.initcap("category")))

# COMMAND ----------

df_silver = df_silver.withColumn("category",F.regexp_replace(F.col("category"), "(?i)Protien","Protein")).withColumn("product_name",F.regexp_replace(F.col("product_name"), "(?i)Protien","Protein"))

# COMMAND ----------

df_silver.display()

# COMMAND ----------

df_silver = df_silver.withColumn("product_id",F.when(F.col("product_id")=='XYZ123',F.lit('25891502')).otherwise(F.col("product_id")))

# COMMAND ----------

df_silver = df_silver.withColumn("division", F.when(F.col("category") == 'Energy Bars', 'Nutrition Bars')
                                 .when(F.col("category")=='Protein Bars', 'Nutrition Bars')
                                 .when(F.col("category")=='Granola & Cereals', 'Breakfast Foods')
                                 .when(F.col("category")=='Recovery Dairy', 'Dairy & Recovery')
                                 .when(F.col("category")=='Healthy Snacks', 'Healthy Snacks')
                                 .when(F.col("category")=='Electrolyte Mix','Hydration & Electrolytes').otherwise("Other"))

# COMMAND ----------

df_silver.show()

# COMMAND ----------

df_silver = df_silver.withColumn("variant", F.regexp_extract(F.col("product_name"),r"\(([^)]+)\)",1))

# COMMAND ----------

display(df_silver)

# COMMAND ----------

df_silver = df_silver.withColumn("product_code", F.sha2(F.col("product_name"),256)).withColumnRenamed("product_name","product")

# COMMAND ----------

df_silver = df_silver.select("product_code","division","category","product","variant","product_id")

# COMMAND ----------

df_silver.write.format('delta').option("data.enablechangeDataFeed","true").mode("overwrite").saveAsTable(f"masterdata.silver.products")

# COMMAND ----------

df_gold = df_silver.select("product_code","product_id","division","category","product","variant")

# COMMAND ----------

display(df_gold)

# COMMAND ----------

df_gold.write.format('delta').option("data.enablechangeDataFeed","true").mode("overwrite").saveAsTable(f"masterdata.gold.sb_dim_products")

# COMMAND ----------

delta_table = DeltaTable.forName(spark, "masterdata.gold.dim_products")
df_child_products = spark.sql(f"SELECT product_code, division, category, product, variant FROM masterdata.gold.sb_dim_products;")

df_child_products.show(5)

# COMMAND ----------

delta_table.alias("t").merge(source=df_child_products.alias("s"),condition="t.product_code = s.product_code").whenMatchedUpdate(
    set={
        "division":"s.division",
        "category":"s.category",
        "product":"s.product",
        "variant":"s.variant" 
    }).whenNotMatchedInsert(
    values={
        "product_code":"s.product_code",
        "division":"s.division",
        "category":"s.category",
        "product":"s.product",
        "variant":"s.variant"
    }).execute()