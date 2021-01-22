# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Reading/writing 

# COMMAND ----------

# Read the file /mnt/training/dataframes/people-with-dups.txt, with a delimiter ":", a header, and infer the schema.

# Expected
# +---------+----------+---------+------+----------+------+-----------+
# |firstName|middleName|lastName |gender|birthDate |salary|ssn        |
# +---------+----------+---------+------+----------+------+-----------+
# |Emanuel  |Wallace   |Panton   |M     |1988-03-04|101255|935-90-7627|
# |Eloisa   |Rubye     |Cayouette|F     |2000-06-20|204031|935-89-9009|
# |Cathi    |Svetlana  |Prins    |F     |2012-12-22|35895 |959-30-7957|
# +---------+----------+---------+------+----------+------+-----------+



# Answer
df = (spark.read
      .option("delimiter",":")   
      .option("header", "true")   
      .option("inferSchema", "true")            
      .csv("/mnt/training/dataframes/people-with-dups.txt"))
df.show(3, truncate=False)

# COMMAND ----------

# Read from the CSV 
csvPath = "/mnt/training/ecommerce/users/users-500k.csv" 
# to usersDF, using delimiter "tab", with header, inferring the schema, and print the schema of the df

# Expected
# root
#  |-- user_id: string (nullable = true)
#  |-- user_first_touch_timestamp: long (nullable = true)
#  |-- email: string (nullable = true)


# Answer
df = (spark.read
      .option("sep", "\t")
      .option("header", True)
      .option("inferSchema", True)
      .csv(csvPath))

df.printSchema()

# COMMAND ----------

# Fabricate the same schema from scratch, and read using that schema
# Expected
# +-----------------+--------------------------+-----+
# |user_id          |user_first_touch_timestamp|email|
# +-----------------+--------------------------+-----+
# |UA000000102357305|1592182691348767          |null |
# |UA000000102357308|1592183287634953          |null |
# |UA000000102357309|1592183302736627          |null |
# +-----------------+--------------------------+-----+



# Answer
from pyspark.sql.types import LongType, StringType, StructType, StructField

definedSchema = StructType([
  StructField("user_id", StringType(), True),  
  StructField("user_first_touch_timestamp", LongType(), True),
  StructField("email", StringType(), True)
])

df = (spark.read
  .option("sep", "\t")
  .option("header", True)
  .schema(definedSchema)
  .csv(csvPath))

df.show(3, truncate=False)

# COMMAND ----------

# again redefine the same schema, but this time using a DDL formatted string


# Answer
DDLSchema = "user_id string, user_first_touch_timestamp long, email string"

df = (spark.read
      .option("sep", "\t")
      .option("header", True)
      .schema(DDLSchema)
      .csv(csvPath))

# COMMAND ----------

# Read from the json file /mnt/training/ecommerce/events/events-500k.json to a df eventsDF, inferring the schema, and print the schema


# Answer
jsonPath = "/mnt/training/ecommerce/events/events-500k.json"

df = (spark.read
  .option("inferSchema", True)
  .json(jsonPath))

df.printSchema()

# COMMAND ----------

# in scala, read the parquet file /mnt/training/ecommerce/events/events.parquet and export the schema to DDL
# Expected
# res0: String = `device` STRING,`ecommerce` STRUCT<`purchase_revenue_in_usd`: DOUBLE, `total_item_quantity`: BIGINT, `unique_items`: BIGINT>,`event_name` STRING,`event_previous_timestamp` BIGINT,`event_timestamp` BIGINT,`geo` STRUCT<`city`: STRING, `state`: STRING>,`items` ARRAY<STRUCT<`coupon`: STRING, `item_id`: STRING, `item_name`: STRING, `item_revenue_in_usd`: DOUBLE, `price_in_usd`: DOUBLE, `quantity`: BIGINT>>,`traffic_source` STRING,`user_first_touch_timestamp` BIGINT,`user_id` STRING



# Answer

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.read.parquet("/mnt/training/ecommerce/events/events.parquet").schema.toDDL

# COMMAND ----------

# Use this string schema to read (in python) the json file
# Expected
# +-------+---------+----------+------------------------+----------------+-----------------+--------------------+--------------+--------------------------+-----------------+
# | device|ecommerce|event_name|event_previous_timestamp| event_timestamp|              geo|               items|traffic_source|user_first_touch_timestamp|          user_id|
# +-------+---------+----------+------------------------+----------------+-----------------+--------------------+--------------+--------------------------+-----------------+
# |  macOS|     [,,]|  warranty|        1593878899217692|1593878946592107|   [Montrose, MI]|                  []|        google|          1593878899217692|UA000000107379500|
# |Windows|     [,,]|     press|        1593876662175340|1593877011756535|[Northampton, MA]|                  []|        google|          1593876662175340|UA000000107359357|
# |  macOS|     [,,]|  add_item|        1593878792892652|1593878815459100|    [Salinas, CA]|[[, M_STAN_T, Sta...|       youtube|          1593878455472030|UA000000107375547|
# +-------+---------+----------+------------------------+----------------+-----------------+--------------------+--------------+--------------------------+-----------------+



# Answer
DDLSchema = "`device` STRING,`ecommerce` STRUCT<`purchase_revenue_in_usd`: DOUBLE, `total_item_quantity`: BIGINT, `unique_items`: BIGINT>,`event_name` STRING,`event_previous_timestamp` BIGINT,`event_timestamp` BIGINT,`geo` STRUCT<`city`: STRING, `state`: STRING>,`items` ARRAY<STRUCT<`coupon`: STRING, `item_id`: STRING, `item_name`: STRING, `item_revenue_in_usd`: DOUBLE, `price_in_usd`: DOUBLE, `quantity`: BIGINT>>,`traffic_source` STRING,`user_first_touch_timestamp` BIGINT,`user_id` STRING"

df = (spark.read
  .schema(DDLSchema)
  .json(jsonPath))

df.show(3)

# COMMAND ----------

# You have such a schema:
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
SCHEMA = StructType([
            StructField("name",StringType(),True),
            StructField("age",StringType(),True),
            StructField("height",IntegerType(),True)
])
# Append a new column "stuff" with integer type to the schema, and print it
# Expected
# StructType(List(StructField(name,StringType,true),StructField(age,StringType,true),StructField(height,IntegerType,true),StructField(stuff,IntegerType,true)))


# Answer
SCHEMA.add("stuff", IntegerType(),True)
SCHEMA

# COMMAND ----------

# Read the csv file from productsCsvPath = "/mnt/training/ecommerce/products/products.csv" and use the header as well as infer the schema, and print schema
# Expected
# root
#  |-- item_id: string (nullable = true)
#  |-- name: string (nullable = true)
#  |-- price: double (nullable = true)



# Answer
csvPath = "/mnt/training/ecommerce/products/products.csv"
df = (spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv(csvPath))

df.printSchema()

# COMMAND ----------

# read again the same file, but now define first the schema (using StructType). Read & output the schema
# Expected: same as above



# Answer
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

definedSchema = StructType([
  StructField("item_id", StringType(), True),
  StructField("name", StringType(), True),
  StructField("price", DoubleType(), True)
])

df = (spark.read
      .option("header","true")
      .schema(definedSchema)
      .csv(csvPath))

df.printSchema()

# COMMAND ----------

# Do the same using the DDL schema declaration
# Expected: same as above



# Answer
DDLSchema = "item_id string, name string, price double"
# or DDLSchema = "`item_id` STRING,`name` STRING,`price` DOUBLE"

df = (spark.read
      .option("header","true")
      .schema(DDLSchema)
      .csv(csvPath))

df.printSchema()

# COMMAND ----------

# Write df to parquet with DataFrameWriter's parquet method and the following configurations:
# Snappy compression, overwrite mode. Use the following path:
outputPath = workingDir + "/users.parquet"


# Answer
(df.write
  .option("compression", "snappy")
  .mode("overwrite")
  .parquet(outputPath)
)

# COMMAND ----------

# Write eventsDF to Delta with DataFrameWriter's save method and the following configurations:
# Delta format, overwrite mode. As output path, use:
outputPath = workingDir + "/delta/events"


# Answer
(df.write
  .format("delta")
  .mode("overwrite")
  .save(outputPath)
)

# COMMAND ----------

# Write eventsDF to a table events using the DataFrameWriter method saveAsTable


# Answer
df.write.mode("overwrite").saveAsTable("someData")

# COMMAND ----------

# Load the table "someData" into df and print first 3 rows
# Expected
# +--------+--------------------+------+
# | item_id|                name| price|
# +--------+--------------------+------+
# |M_PREM_Q|Premium Queen Mat...|1795.0|
# |M_STAN_F|Standard Full Mat...| 945.0|
# |M_PREM_F|Premium Full Matt...|1695.0|
# +--------+--------------------+------+



# Answer
df = spark.table("someData")
df.show(3)

# COMMAND ----------

# Write to an unmanaged table by adding an .option() that includes a path.
unmanagedPath = f"{workingDir}/myTableUnmanaged"



# Answer
df.write.mode("overwrite").option('path', unmanagedPath).saveAsTable("myTableUnmanaged")

# COMMAND ----------

# Create a databricks table from a parquet file /mnt/training/ecommerce/events/events.parquet



# Answer

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS events USING parquet OPTIONS (path "/mnt/training/ecommerce/events/events.parquet");

# COMMAND ----------

# Register the dataframe df into a temporary view "tempView". The select the columns "item_id" and "price" from tempView and display first 3 rows
# Expected
# +--------+------+
# | item_id| price|
# +--------+------+
# |M_PREM_Q|1795.0|
# |M_STAN_F| 945.0|
# |M_PREM_F|1695.0|
# +--------+------+



# Answer
df.createOrReplaceTempView("tempView")
df2 = spark.sql("SELECT item_id, price FROM tempView")
df2.show(3)

# COMMAND ----------

TODO: more on tables...

# COMMAND ----------

Dataframes creation

# COMMAND ----------

# Create a dataframe containing one string column and 3 rows with the items "2017-01-01", "2018-02-08", "2019-01-03". Then cast the value as date using selectExpr. Print it
# Expected
# +----------+
# |      date|
# +----------+
# |2017-01-01|
# |2018-02-08|
# |2019-01-03|
# +----------+



# Answer
df = (spark.createDataFrame([
  "2017-01-01", "2018-02-08", "2019-01-03"], "string"
).selectExpr("CAST(value AS date) AS date"))

df.show(3)

# COMMAND ----------

# Create a dataframe based on a range between 1000 and 10000. Show the first 3 rows
# Expected
# +----+
# |  id|
# +----+
# |1000|
# |1001|
# |1002|
# +----+



# Answer
df = spark.range(1000, 10000)
df.show(3)

# COMMAND ----------

# Create a dataframe with column A containing rows 1, 2, 2; and column B containing rows 4, 8, 6. Show it.
# Expected
# +---+---+
# |  A|  B|
# +---+---+
# |  1|  4|
# |  2|  8|
# |  2|  6|
# +---+---+



# Answer
df = spark.createDataFrame([(1, 4), (2, 8), (2, 6)], ["A", "B"]) 
df.show()

# COMMAND ----------

# Create the dataframe containing the following table:
# Expected
# +----------+--------------+
# |  itemName|sales_quantity|
# +----------+--------------+
# |        TV|           200|
# |Headphones|           400|
# |    Phones|           300|
# +----------+--------------+



# Answer
df= spark.createDataFrame([
                ['TV',200],
                ['Headphones',400],
                ['Phones',300]],['itemName','sales_quantity'])
df.show()

# COMMAND ----------

