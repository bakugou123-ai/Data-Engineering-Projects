base_path = f's3://sales-s3-master-bucket/customers/customers.csv'

# COMMAND ----------

df= spark.read.format('csv').load(base_path, header=True)
display(df)

# COMMAND ----------

df.write.format('delta').option("delta.enablechangeDataFeed","true").mode("overwrite").saveAsTable(f"masterdata.bronze.customers")

# COMMAND ----------

df_bronze = spark.sql(f"SELECT * FROM masterdata.bronze.customers");


# COMMAND ----------

df_duplicates = df_bronze.groupBy('customer_id').count().where("count > 1")
display(df_duplicates)

# COMMAND ----------

df_silver = df_bronze.dropDuplicates(['customer_id'])
display(df_silver)


# COMMAND ----------

from pyspark.sql import functions as F;
from delta.tables import DeltaTable;

# COMMAND ----------

display(df_silver.filter(F.col("customer_name")!=F.trim(F.col("customer_name"))))

# COMMAND ----------

df_silver = df_silver.withColumn("customer_name",F.trim(F.col("customer_name")))

# COMMAND ----------

display(df_silver.filter(F.col("customer_name")!=F.trim(F.col("customer_name"))))

# COMMAND ----------

df_silver.select("city").distinct().show()

# COMMAND ----------

city_mapping = {
"Bengalore" : "Bengaluru",
"Bengaluruu" : "Bengaluru",
"NewDelhee" : "New Delhi",
"NewDheli" : "New Delhi",
"NewDelhi" : "New Delhi",
"Hyderabadd" : "Hyderabad",
"Hyderbad" : "Hyderabad"
}
allowed= ['Bengaluru', 'New Delhi', 'Hyderabad']

df_silver = df_silver.replace(city_mapping,subset=['city'])

df_silver.withColumn("city",F.when(F.col("city").isNull(),None).when(F.col("city").isin(allowed),F.col("city")).otherwise(None))



# COMMAND ----------

df_silver.show()

# COMMAND ----------

df_silver = df_silver.withColumn("customer_name",F.when(F.col("customer_name").isNull(),None).otherwise(F.initcap("customer_name")))

# COMMAND ----------

display(df_silver)

# COMMAND ----------

df_silver.filter(F.col("city").isNull()).show()

# COMMAND ----------

null_customer_names = ['Sprintax Nutrition', 'Zenathlete Foods', 'Primefuel Nutrition', 'Recovery Lane']

df_silver.filter(F.col("customer_name").isin(null_customer_names)).show()


# COMMAND ----------

fixed_city = {'789420' : 'Bengaluru','789521':'Hyderabad','789603':'Hyderabad'}

fixed_df = spark.createDataFrame([(k,v) for k,v in fixed_city.items()],["customer_id","fixed_city"])

# COMMAND ----------

display(fixed_df)

# COMMAND ----------

df_silver = (df_silver.join(fixed_df,"customer_id","left").withColumn("city",F.coalesce("city","fixed_city")))


# COMMAND ----------

df_silver = df_silver.drop("fixed_city")

# COMMAND ----------

df_silver = df_silver.filter(F.col("city").isNotNull())


# COMMAND ----------

df_silver = df_silver.withColumn("customer_id",F.col("customer_id").cast("bigint"))

# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

df_silver = (df_silver
    .withColumn("customer", F.concat_ws("-", "customer_name", F.coalesce(F.col("city"), F.lit("Unknown"))))
    .withColumn("market", F.lit("India"))
    .withColumn("platform", F.lit("Sports Bar"))
    .withColumn("channel", F.lit("Acquisition"))
)

# COMMAND ----------

display(df_silver)

# COMMAND ----------

df_silver.write.format('delta').option("delta.enablechangeDataFeed","true").mode("overwrite").saveAsTable(f"masterdata.silver.customers")

# COMMAND ----------

# MAGIC %md
# MAGIC GOLD PROCESSING

# COMMAND ----------

df_gold = spark.sql(f"SELECT * FROM masterdata.gold.dim_customers",header=True);


# COMMAND ----------

df_gold.display()

# COMMAND ----------

df_gold = df_silver.select("customer_id","customer_name","city","customer","market","platform","channel")

# COMMAND ----------

df_gold.write.format('delta').option("delta.enablechangeDataFeed","true").mode("overwrite").saveAsTable(f"masterdata.gold.sb_dim_customers")

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

# COMMAND ----------

delta_table = DeltaTable.forName(spark, "masterdata.gold.dim_customers")
df_child = spark.table("masterdata.gold.sb_dim_customers").select(F.col("customer_id").alias("customer_code"),"customer","market","platform","channel")

# COMMAND ----------

delta_table.alias("t").merge(source=df_child.alias("s"),condition="t.customer_code = s.customer_code").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
