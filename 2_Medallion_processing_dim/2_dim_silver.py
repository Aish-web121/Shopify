# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import StringType,IntegerType,DateType,TimestampType,FloatType
catalog_name="ecomm"

# COMMAND ----------

# MAGIC %md
# MAGIC ###DimensionTable:Brand

# COMMAND ----------

df_bronze=spark.table(f"{catalog_name}.bronze.brz_brand")
display(df_bronze.show(truncate=False))

# COMMAND ----------

#Trimming spaces from brand_name
df_silver=df_bronze.withColumn("brand_name",F.trim(F.col("brand_name")))

display(df_silver.show(30))

# COMMAND ----------

#remove non alphanumeric characters from brand_code
df_silver=df_silver.withColumn("brand_code",F.regexp_replace(F.col("brand_code"),"[^a-zA-Z0-9]", ""))

display(df_silver.limit(20))


# COMMAND ----------

df_bronze.select("category_code").distinct().show(truncate=False)

# COMMAND ----------

anomalies={
    "GROCERY":"GRCY",
    "TOYS":"TOY",
    "BOOKS":"BKS"
}
df_silver=df_silver.replace(to_replace=anomalies,subset=["category_code"])
display(df_silver)


# COMMAND ----------

df_silver.select("category_code").distinct().show(truncate=False)


# COMMAND ----------

df_silver.write.format("delta")\
    .mode("overwrite")\
    .saveAsTable(f"{catalog_name}.silver.slv_brands")

# COMMAND ----------

# MAGIC %md
# MAGIC ###DimensionTable: Category

# COMMAND ----------

df_bronze=spark.table(f"{catalog_name}.bronze.brz_category")
display(df_bronze)

# COMMAND ----------

df_silver=df_bronze.dropDuplicates(["category_code"])
df_silver.show()

# COMMAND ----------

df_silver=df_silver.withColumn("category_code",F.upper(F.col("category_code")))

display(df_silver)

# COMMAND ----------

df_silver.columns

# COMMAND ----------

df_silver.write.format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema","true")\
    .saveAsTable(f"{catalog_name}.silver.slv_category")

# COMMAND ----------

# MAGIC %md
# MAGIC ###DimensionTable:Customers
# MAGIC

# COMMAND ----------

df_bronze=spark.table(f"{catalog_name}.bronze.brz_customers")
display(df_bronze.count())
display(df_bronze.limit(10))


# COMMAND ----------

df_bronze.groupBy("customer_id").count().filter("count > 1").show()  # no duplicates


# COMMAND ----------

#now null values 
df_bronze.filter(F.col("customer_id").isNull()).show()



# COMMAND ----------

#dropping null values in customer_id
df_silver=df_bronze.dropna(subset=["customer_id"])
display(df_silver.count())
display(df_silver.limit(10))



# COMMAND ----------

# writing not available for null phone numbers
df_silver=df_silver.withColumn("phone",F.when(F.col("phone").isNull(),F.lit("Not Available")).otherwise(F.col("phone")))
display(df_silver.count())
display(df_silver.limit(10))

# COMMAND ----------

print(df_silver.filter(F.col("phone").isNull()).count())


# COMMAND ----------

#writing back to silver layer
df_silver.write.format("delta")\
    .mode("overwrite")\
    .option("mergeSchema","true")\
        .saveAsTable(f"{catalog_name}.silver.slv_customers")


# COMMAND ----------

# MAGIC %md
# MAGIC ###DimensionTable:Calender

# COMMAND ----------

df_bronze=spark.table(f"{catalog_name}.bronze.brz_Calendar")
display(df_bronze.limit(20))


# COMMAND ----------

##checking duplicates in date
df_bronze.groupBy("date").count().filter("count >1").show()



# COMMAND ----------

#dropping duplicates :)
df_silver=df_bronze.dropDuplicates(["date"])




# COMMAND ----------

#check
df_silver.groupBy("date").count().filter("count >1").show()

# COMMAND ----------

#making week of the year positive
df_silver=df_silver.withColumn("week_of_year",F.abs(F.col("week_of_year")))
display(df_silver.limit(10))

# COMMAND ----------

#starting letter capital of day name
df_silver=df_silver.withColumn("day_name",F.initcap(F.col("day_name")))
display(df_silver.limit(10))


# COMMAND ----------

#enhancing week_of_year and quarter
df_silver=df_silver.withColumn("week_of_year",F.concat_ws("-",F.concat(F.lit("Week"),F.col("week_of_year"),F.lit("-"),F.col("year"))))
df_silver=df_silver.withColumn("quarter",F.concat_ws("",F.concat(F.lit("Q"),F.col("quarter"),F.lit("-"),F.col("year"))))
df_silver=df_silver.withColumnRenamed("week_of_year","week")
display(df_silver.limit(10))



# COMMAND ----------

df_silver.write.format("delta")\
    .mode("overwrite")\
    .option("mergeSchema","true")\
        .saveAsTable(f"{catalog_name}.silver.slv_calendar")

# COMMAND ----------

# MAGIC %md
# MAGIC ###DimensionTable:Products

# COMMAND ----------

df_bronze=spark.table(f"{catalog_name}.bronze.brz_Products")
display(df_bronze.limit(20))

# COMMAND ----------

df_silver = df_bronze.withColumn(
    "weight_grams",
    F.regexp_replace(F.col("weight_grams"), "g", "").cast(IntegerType())
)
df_silver.select("weight_grams").show(5, truncate=False)

# COMMAND ----------

df_silver = df_silver.withColumn(
    "length_cm",
    F.regexp_replace(F.col("length_cm"), ",", ".").cast(FloatType())
)
df_silver.select("length_cm").show(3)

# COMMAND ----------

# convert category_code and brand_code to upper case
df_silver = df_silver.withColumn(
    "category_code",
    F.upper(F.col("category_code"))
).withColumn(
    "brand_code",
    F.upper(F.col("brand_code"))
)
df_silver.select("category_code", "brand_code").show(2)

# COMMAND ----------

 #Fix spelling mistakes
df_silver = df_silver.withColumn(
    "material",
    F.when(F.col("material") == "Coton", "Cotton")
     .when(F.col("material") == "Alumium", "Aluminum")
     .when(F.col("material") == "Ruber", "Rubber")
     .otherwise(F.col("material"))
)
df_silver.select("material").distinct().show()    

# COMMAND ----------

 #Convert negative rating_count to positive
df_silver = df_silver.withColumn(
    "rating_count",
    F.when(F.col("rating_count").isNotNull(), F.abs(F.col("rating_count")))
     .otherwise(F.lit(0))  # if null, replace with 0
)

# COMMAND ----------


df_silver.select(
    "weight_grams",
    "length_cm",
    "category_code",
    "brand_code",
    "material",
    "rating_count"
).show(10, truncate=False)

# COMMAND ----------

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_products")

# COMMAND ----------

# MAGIC %md