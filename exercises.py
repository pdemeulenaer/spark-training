# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Exercises
# MAGIC 
# MAGIC This notebook is intended to contain a series of exercises taken from Databricks academy, from books, or personal examples

# COMMAND ----------

# Test-takers should have a basic understanding of Spark architecture and be able to apply the Spark DataFrame API to complete individual data manipulation taste like selecting, renaming and manipulating columns; filtering, dropping, sorting and aggregating rows; joining, reading, and writing partitioning DataFrames; and working with UDFs and Spark SQL functions. Databricks expects that developers who have used the Spark DataFrame API for at least 6 months should be able to pass this certification exam.

# Some of the tasks the minimally-qualified candidate should be able to perform include:

# Selecting, renaming and manipulating columns
# Filtering, dropping, sorting and aggregating rows
# Joining, reading, writing and partitioning DataFrames
# Working with UDFs and Spark SQL functions

# Exam FAQ: https://academy.databricks.com/training-faq#cert-faq.

# COMMAND ----------

# MAGIC %md
# MAGIC -sandbox
# MAGIC 
# MAGIC ### Dataframe creation from small data extracts

# COMMAND ----------

# Create a dataframe based on a range between 1000 and 10000

integerDF = spark.range(1000, 10000)

display(integerDF)

# COMMAND ----------

# MAGIC %md
# MAGIC -sandbox
# MAGIC 
# MAGIC ### Reading/writing

# COMMAND ----------

# Create a Min/Max scaler from scratch and apply it to the following data
integerDF = spark.range(1000, 10000)
display(integerDF)

# COMMAND ----------

from pyspark.sql.functions import col, max, min

colMin = integerDF.select(min("id")).first()[0]
colMax = integerDF.select(max("id")).first()[0]

normalizedIntegerDF = (integerDF
  .withColumn("normalizedValue", (col("id") - colMin) / (colMax - colMin) )
)

display(normalizedIntegerDF)

# COMMAND ----------

# Read from the CSV usersCsvPath = "/mnt/training/ecommerce/users/users-500k.csv" to usersDF, using delimiter "tab", with header, inferring the schema, and print the schema of the df

# COMMAND ----------

usersDF = (spark.read
  .option("sep", "\t")
  .option("header", True)
  .option("inferSchema", True)
  .csv(usersCsvPath))

usersDF.printSchema()

# COMMAND ----------

# Fabricate the same schema from scratch, and read using that schema

# COMMAND ----------

from pyspark.sql.types import LongType, StringType, StructType, StructField

userDefinedSchema = StructType([
  StructField("user_id", StringType(), True),  
  StructField("user_first_touch_timestamp", LongType(), True),
  StructField("email", StringType(), True)
])

usersDF = (spark.read
  .option("sep", "\t")
  .option("header", True)
  .schema(userDefinedSchema)
  .csv(usersCsvPath))

# COMMAND ----------

# again redefine the same schema, but this time using a DDL formatted string

# COMMAND ----------

DDLSchema = "user_id string, user_first_touch_timestamp long, email string"

usersDF = (spark.read
  .option("sep", "\t")
  .option("header", True)
  .schema(DDLSchema)
  .csv(usersCsvPath))

# COMMAND ----------

# read from the json file /mnt/training/ecommerce/events/events-500k.json to a df eventsDF, inferring the schema, and print the schema

# COMMAND ----------

eventsJsonPath = "/mnt/training/ecommerce/events/events-500k.json"

eventsDF = (spark.read
  .option("inferSchema", True)
  .json(eventsJsonPath))

eventsDF.printSchema()

# COMMAND ----------

# in scala, read the parquet file /mnt/training/ecommerce/events/events.parquet and export the schema to DDL

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.read.parquet("/mnt/training/ecommerce/events/events.parquet").schema.toDDL

# COMMAND ----------

# use this string to read (in python) the json file

# COMMAND ----------

DDLSchema = "`device` STRING,`ecommerce` STRUCT<`purchase_revenue_in_usd`: DOUBLE, `total_item_quantity`: BIGINT, `unique_items`: BIGINT>,`event_name` STRING,`event_previous_timestamp` BIGINT,`event_timestamp` BIGINT,`geo` STRUCT<`city`: STRING, `state`: STRING>,`items` ARRAY<STRUCT<`coupon`: STRING, `item_id`: STRING, `item_name`: STRING, `item_revenue_in_usd`: DOUBLE, `price_in_usd`: DOUBLE, `quantity`: BIGINT>>,`traffic_source` STRING,`user_first_touch_timestamp` BIGINT,`user_id` STRING"

eventsDF = (spark.read
  .schema(DDLSchema)
  .json(eventsJsonPath))

# COMMAND ----------

# Write usersDF to parquet with DataFrameWriter's parquet method and the following configurations:
# Snappy compression, overwrite mode
usersOutputPath = workingDir + "/users.parquet"

# COMMAND ----------

(usersDF.write
  .option("compression", "snappy")
  .mode("overwrite")
  .parquet(usersOutputPath)
)

# COMMAND ----------

# Write eventsDF to Delta with DataFrameWriter's save method and the following configurations:
# Delta format, overwrite mode

# COMMAND ----------

eventsOutputPath = workingDir + "/delta/events"

(eventsDF.write
  .format("delta")
  .mode("overwrite")
  .save(eventsOutputPath)
)

# COMMAND ----------

# Write eventsDF to a table events_p using the DataFrameWriter method saveAsTable

# COMMAND ----------

eventsDF.write.mode("overwrite").saveAsTable("events_p")

# COMMAND ----------

# read the csv file from productsCsvPath = "/mnt/training/ecommerce/products/products.csv" and use the header as well as infer the schema, and print schema

# COMMAND ----------

productsCsvPath = "/mnt/training/ecommerce/products/products.csv"
productsDF = (spark.read
             .option("header","true")
             .option("inferSchema","true")
             .csv(productsCsvPath))

productsDF.printSchema()

# COMMAND ----------

# read again the same file, but now define first the schema (using StructType)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

userDefinedSchema = StructType([
  StructField("item_id", StringType(), True),
  StructField("name", StringType(), True),
  StructField("price", DoubleType(), True)
])

productsDF2 = (spark.read
              .option("header","true")
              .schema(userDefinedSchema)
              .csv(productsCsvPath))

productsDF2.printSchema()

# COMMAND ----------

# do the same using the DDL schema declaration

# COMMAND ----------

DDLSchema = "item_id string, name string, price double"
# or DDLSchema = "`item_id` STRING,`name` STRING,`price` DOUBLE"

productsDF3 = (spark.read
              .option("header","true")
              .schema(DDLSchema)
              .csv(productsCsvPath))

productsDF3.printSchema()

# COMMAND ----------

productsDF3.show(5)

# COMMAND ----------

# using selectExpr, select the item_id, the price and a new column "high_price" which is true if price>1000, else false

# COMMAND ----------

productsDF3.selectExpr("item_id","price","price > 1000 as high_price").show(5)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC -sandbox
# MAGIC 
# MAGIC ### Imputing Null or Missing Data

# COMMAND ----------

# Take a look at the following DataFrame, which has missing values.

corruptDF = spark.createDataFrame([
  (11, 66, 5),
  (12, 68, None),
  (1, None, 6),
  (2, 72, 7)], 
  ["hour", "temperature", "wind"]
)

display(corruptDF)

# COMMAND ----------

# Drop any records that have null values.

#corruptDroppedDF = corruptDF.dropna("any")
corruptDroppedDF = corruptDF.na.drop("any")

display(corruptDroppedDF)

# COMMAND ----------

# Impute values with the mean.

# COMMAND ----------

corruptImputedDF = corruptDF.na.fill({"temperature": 68, "wind": 6})
display(corruptImputedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deduplicating Data

# COMMAND ----------

# Take a look at the following DataFrame that has duplicate values.
duplicateDF = spark.createDataFrame([
  (15342, "Conor", "red"),
  (15342, "conor", "red"),
  (12512, "Dorothy", "blue"),
  (5234, "Doug", "aqua")], 
  ["id", "name", "favorite_color"]
)

#display(duplicateDF)
duplicateDF.show()

# COMMAND ----------

# Drop duplicates on id and favorite_color.

# COMMAND ----------

duplicateDedupedDF = duplicateDF.dropDuplicates(["id", "favorite_color"])
display(duplicateDedupedDF)

# COMMAND ----------

# here some other data
dupedDF = (spark.read
           .option("delimiter",":")   
           .option("header", "true")   
           .option("inferSchema", "true")            
           .csv("/mnt/training/dataframes/people-with-dups.txt"))
display(dupedDF)

# COMMAND ----------

# Add Columns to Filter Duplicates
# Add columns following to allow you to filter duplicate values. Add the following:

# lcFirstName: first name lower case
# lcLastName: last name lower case
# lcMiddleName: middle name lower case
# ssnNums: social security number without hyphens between numbers
# Save the results to dupedWithColsDF.

from pyspark.sql.functions import col, lower, translate

dupedWithColsDF = (dupedDF.withColumn("lcFirstName", lower(col("firstName")))
                   .withColumn("lcLastName",lower(col("lastName")))
                   .withColumn("lcMiddleName",lower(col("middleName")))
                   .withColumn("ssnNums",translate(col("ssn"),"-",""))
                  )

display(dupedWithColsDF)

# COMMAND ----------

# Deduplicate the data by dropping duplicates of all records except for the original names (first, middle, and last) and the original ssn. Save the result to dedupedDF. Drop the columns you added in step 2.

# COMMAND ----------

dedupedDF = (dupedWithColsDF
  .dropDuplicates(["lcFirstName", "lcMiddleName", "lcLastName", "ssnNums", "gender", "birthDate", "salary"])
  .drop("lcFirstName", "lcMiddleName", "lcLastName", "ssnNums")
)

display(dedupedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining Data

# COMMAND ----------

# Let's have 2 df:
countryLookupDF = spark.read.parquet("/mnt/training/countries/ISOCountryCodes/ISOCountryLookup.parquet")
logWithIPDF = spark.read.parquet("/mnt/training/EDGAR-Log-20170329/enhanced/logDFwithIP.parquet")

# COMMAND ----------

# Complete the following:

# Create a new DataFrame logWithIPEnhancedDF
# Get the full country name by performing a broadcast join that broadcasts the lookup table to the server log
# Drop all columns other than EnglishShortName

# COMMAND ----------

from pyspark.sql.functions import broadcast

logWithIPEnhancedDF = (logWithIPDF
  .join(broadcast(countryLookupDF), logWithIPDF.IPLookupISO2 == countryLookupDF.alpha2Code)
  .drop("alpha2Code", "alpha3Code", "numericCode", "ISO31662SubdivisionCode", "independentTerritory")
)

display(logWithIPEnhancedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to database

# COMMAND ----------

# set number of partitions to 8

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

# Parallel Database Writes
# Database writes are the inverse of what was covered in Lesson 4 of ETL Part 1. In that lesson you defined the number of partitions in the call to the database.

# By contrast and when writing to a database, the number of active connections to the database is determined by the number of partitions of the DataFrame.

# COMMAND ----------

# get the number of partitions of dataframe df

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

# coalesce the dataframe to 2 partitions 

# COMMAND ----------

dfCoalesced = df.coalesce(2)

# COMMAND ----------

# Create a function that counts the number of records per partition in a datagframe

def check_partition(df):

  print("Num partition: {0}".format(df.rdd.getNumPartitions()))
   
  def count_partition(index, iterator):
      yield (index, len(list(iterator)))
       
  data = (df.rdd.mapPartitionsWithIndex(count_partition, True).collect())
   
  for index, count in data:
      print("partition {0:2d}: {1} bytes".format(index, count))
            
check_partition(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing to a Managed/Unmanaged Table
# MAGIC 
# MAGIC Managed tables allow access to data using the Spark SQL API.

# COMMAND ----------

# some data. save it to a managed table
df = spark.range(1, 100)
display(df)

# COMMAND ----------

df.write.mode("OVERWRITE").saveAsTable("myTableManaged")

# COMMAND ----------

# describe the metadata of this table in sql

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED myTableManaged

# COMMAND ----------

# Write to an unmanaged table by adding an .option() that includes a path.
unmanagedPath = f"{workingDir}/myTableUnmanaged"

# COMMAND ----------

df.write.mode("OVERWRITE").option('path', unmanagedPath).saveAsTable("myTableUnmanaged")

# COMMAND ----------

# DIVERS

# COMMAND ----------

# Create a databricks table from a parquet file /mnt/training/ecommerce/events/events.parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS events USING parquet OPTIONS (path "/mnt/training/ecommerce/events/events.parquet");

# COMMAND ----------

# DATAFRAMES EXERCISES

# COMMAND ----------

# print the schema of a dataframe

# COMMAND ----------

df.printSchema()
df.schema

# COMMAND ----------

# Translate the next query into pure dataframe API
budgetDF = spark.sql("""
SELECT name, price
FROM products
WHERE price < 200
ORDER BY price
""")

budgetDF.show()

# COMMAND ----------

budgetDF = (productsDF
  .select("name", "price")
  .where("price < 200")
  .orderBy("price")
)
budgetDF.show()

# COMMAND ----------

# create a temp view from that dataframe, and query it

# COMMAND ----------

budgetDF.createOrReplaceTempView("budget")
display(spark.sql("SELECT * FROM budget"))

# COMMAND ----------

# Load the table "events" into eventDF

# COMMAND ----------

eventDF = spark.table("events")
eventDF.show(5)

# COMMAND ----------

# Filter for rows where device is macOS and then Sort rows by event_timestamp

# COMMAND ----------

macDF = (eventDF
  .where("device == 'macOS'")
  .sort("event_timestamp")
)

# or equivalently:

macDF = (eventDF
  .filter(col("device")=="macOS")
  .orderBy("event_timestamp")
)

macDF.show(5)

# COMMAND ----------

# Count results and take first 5 rows

# COMMAND ----------

numRows = macDF.count()
rows = macDF.take(5)

# COMMAND ----------

# Create the same DataFrame using SQL query
#- Use SparkSession to run a sql query on the `events` table
#- Use SQL commands above to write the same filter and sort query used earlier

# COMMAND ----------

macSQLDF = spark.sql("SELECT * FROM events WHERE device == 'macOS' ORDER BY event_timestamp")
macSQLDF.show(5)

# COMMAND ----------

# using following dataframe, find the minimum and maximum
df = (spark.createDataFrame([
  "2017-01-01", "2018-02-08", "2019-01-03"], "string"
).selectExpr("CAST(value AS date) AS date"))

# COMMAND ----------

from pyspark.sql.functions import min, max

df = spark.createDataFrame([
  "2017-01-01", "2018-02-08", "2019-01-03"], "string"
).selectExpr("CAST(value AS date) AS date")

min_date, max_date = df.select(min("date"), max("date")).first()
min_date, max_date    

# COMMAND ----------

# in next dataframe, drop duplicate rows
df = spark.createDataFrame([(1, 4, 3), (2, 8, 1), (2, 8, 1), (2, 8, 3), (3, 2, 1)], ["A", "B", "C"])  
df.show()

# COMMAND ----------

df.dropDuplicates().show()

# COMMAND ----------

# then drop only the identical rows for columns A and B

# COMMAND ----------

df.dropDuplicates(['A','B']).show()

# COMMAND ----------

# drop columns A and B

# COMMAND ----------

df.drop('A','B').show()

# COMMAND ----------

# you have 2 dataframes, df1 and df2. Find the common columns between them

# COMMAND ----------

list(set(df1.columns).intersection(set(df2.columns)))

# COMMAND ----------

# You have such dataframe. Create column C, indicating if the sum of A and B is higher than 7, as a boolean.
df = spark.createDataFrame([(1, 4), (2, 8), (2, 6)], ["A", "B"]) 

# COMMAND ----------

df.selectExpr("A", "B", "A+B > 7 as high").show(5)

# COMMAND ----------

# Sort the dataframe on column B, descending order

# COMMAND ----------

from pyspark.sql.functions import col
df.sort(col("B").desc()).show()

# COMMAND ----------

# now sort the df along col A in desc, and then along col B in ascending order

# COMMAND ----------

df.sort(col("A").desc(), col("B").asc()).show()

# COMMAND ----------

# You have such dataframe. Sort along B column in desc order, keeping NULL at the end
df = spark.createDataFrame([(1, 4), (2, 8), (3, None), (2, 6)], ["A", "B"]) 

# COMMAND ----------

df.sort(col("B").desc_nulls_last()).show()

# COMMAND ----------

# filter the previous dataframe so that to keep column B without NULL

# COMMAND ----------

df.filter(col("B").isNotNull()).show()

# COMMAND ----------

# find the unique values in column A

# COMMAND ----------

df.select("A").distinct().show()

# COMMAND ----------

# Aggregations
# Example: we have a given dataframe like
df = spark.createDataFrame([(1, 4), (2, 5), (2, 8), (3, 6), (3, 2)], ["A", "B"])
df.show()
# Group by A and build the average of B, min of B, max of B

# COMMAND ----------

from pyspark.sql import functions as F
df.groupBy("A").agg(F.avg("B"), F.min("B"), F.max("B")).show()

# COMMAND ----------

# group by column A and get the distinct number of rows in column B for each group. Then order by that quantity, descending

# COMMAND ----------

df.groupBy('A').agg(F.countDistinct('B').alias('countB')).orderBy('countB',ascending=False).show()

# COMMAND ----------

# now same, but using the approxmative distinct count function instead of the deterministic distinct count function distinctCount

# COMMAND ----------

df.groupBy('A').agg(F.approx_count_distinct('B').alias('countB')).orderBy('countB',ascending=False).show()

# COMMAND ----------

# group by A and get the first, last, and sum of colunm B. Rename these columns as "my first", "my last", "my everything"

# COMMAND ----------

df.groupBy("A").agg(
  F.first("B").alias("my first"),
  F.last("B").alias("my last"),
  F.sum("B").alias("my everything")
).show()

# COMMAND ----------

# You have such dataframe. Recreate columns A and B and limit them to 2 decimals
df = spark.createDataFrame([(1.787, 4.3434), (2.5655, 8.67676), (2.23245, 6.676746)], ["A", "B"]) 

# COMMAND ----------

from pyspark.sql.functions import col, round
df.withColumn("A", round(col("A"),2)).withColumn("B", round(col("B"),2)).show()

# COMMAND ----------

# cast the first column to "long" type

# COMMAND ----------

df.withColumn("A", col("A").cast("long")).show()

# COMMAND ----------

