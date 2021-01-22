# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Aggregations: GroupBy

# COMMAND ----------

# You have such a df. 

df = spark.createDataFrame([("1991-11-15",'a',23),
                               ("1991-11-16",'a',24),
                               ("1991-11-17",'a',32),
                               ("1991-11-25",'b',13),
                               ("1991-11-26",'b',14)], schema=['date', 'customer', 'balance_day'])
# Produce a groupby operation which allows to collect the dates and the values in 2 columns, time_series_dates and time_series_values. During the aggregation, build also a 3rd column which is made of both date and value, like tuples. Show the result without truncating it.
# Expected:
# +--------+------------------------------------+------------------+------------------------------------------------------+
# |customer|time_series_dates                   |time_series_values|time_series_tuples                                    |
# +--------+------------------------------------+------------------+------------------------------------------------------+
# |a       |[1991-11-15, 1991-11-16, 1991-11-17]|[23, 24, 32]      |[[1991-11-15, 23], [1991-11-16, 24], [1991-11-17, 32]]|
# |b       |[1991-11-25, 1991-11-26]            |[13, 14]          |[[1991-11-25, 13], [1991-11-26, 14]]                  |
# +--------+------------------------------------+------------------+------------------------------------------------------+



# Answer
df = df.groupby("customer").agg(F.collect_list('date').alias('time_series_dates'),
                                F.collect_list('balance_day').alias('time_series_values'),
                                F.collect_list(F.struct('date','balance_day')).alias('time_series_tuples'))
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Aggregations: Window functions

# COMMAND ----------

# Import the Window class and the sql functions
from pyspark.sql import Window
import pyspark.sql.functions as F

# COMMAND ----------

# With such a dataframe
df = spark.createDataFrame([(1, 4), (2, 5), (2, 8), (3, 6), (3, 2)], ["A", "B"])
# Group by A and build the average of B, min of B, max of B
# Expected:
# +---+------+------+------+
# |  A|avg(B)|min(B)|max(B)|
# +---+------+------+------+
# |  1|   4.0|     4|     4|
# |  2|   6.5|     5|     8|
# |  3|   4.0|     2|     6|
# +---+------+------+------+



# Answer
df.groupBy("A").agg(F.avg("B"), F.min("B"), F.max("B")).show()

# COMMAND ----------

# Using same df as above, group by column A and get the distinct number of rows in column B for each group, called "countB". Then order by that quantity, descending
# Expected:
# +---+------+
# |  A|countB|
# +---+------+
# |  2|     2|
# |  3|     2|
# |  1|     1|
# +---+------+



# Answer
df.groupBy('A').agg(F.countDistinct('B').alias('countB')).orderBy('countB',ascending=False).show()

# COMMAND ----------

# now same, but using the approxmative distinct count function instead of the deterministic distinct count function distinctCount
# Expected
# +---+------+
# |  A|countB|
# +---+------+
# |  3|     2|
# |  2|     2|
# |  1|     1|
# +---+------+



# Answer
df.groupBy('A').agg(F.approx_count_distinct('B').alias('countB')).orderBy('countB',ascending=False).show()

# COMMAND ----------

# group by A and get the first, last, and sum of colunm B. Rename these columns as "my first", "my last", "my everything"
# Expected:
# +---+--------+-------+-------------+
# |  A|my first|my last|my everything|
# +---+--------+-------+-------------+
# |  1|       4|      4|            4|
# |  2|       5|      8|           13|
# |  3|       6|      2|            8|
# +---+--------+-------+-------------+



# Answer
df.groupBy("A").agg(
  F.first("B").alias("my first"),
  F.last("B").alias("my last"),
  F.sum("B").alias("my everything")
).show()

# COMMAND ----------

# from following dataframe, group by A and for each group, give the list of distinct elements
df = spark.createDataFrame([(1, 4), (2, 5), (2, 8), (2, 8), (2, 9), (3, 6), (3, 2)], ["A", "B"])
# Expected:
# +---+---------+
# |  A|        B|
# +---+---------+
# |  1|      [4]|
# |  2|[9, 5, 8]|
# |  3|   [2, 6]|
# +---+---------+


# Answer
df.groupBy("A").agg(F.collect_set("B").alias("B")).show()

# COMMAND ----------

# You have the following dataframe. 

dfSales= spark.createDataFrame([
                ['TV',200],
                ['Headphones',400],
                ['Phones',300],
                ['Kitchen',500],
                ['Office',300]],('itemName','sales_quantity'))

# Create a new column salesDenseRank which is the dense_rank over the sales_quantity (use window function)
# Expected
# +----------+--------------+--------------+
# |  itemName|sales_quantity|salesDenseRank|
# +----------+--------------+--------------+
# |        TV|           200|             1|
# |    Phones|           300|             2|
# |    Office|           300|             2|
# |Headphones|           400|             3|
# |   Kitchen|           500|             4|
# +----------+--------------+--------------+



# Answer
from pyspark.sql import Window
from pyspark.sql.functions import dense_rank,col

windowSpec = Window.orderBy("sales_quantity")
dfSales.select(
       col("itemName"),
       col("sales_quantity"),
       dense_rank().over(windowSpec).alias("salesDenseRank")
).show()

# COMMAND ----------

# You have such a dataframe
tup = [(1, "a"), (1, "a"), (1, "b"), (2, "a"), (2, "b"), (3, "b")]
df = spark.createDataFrame(tup, ["id", "category"])
df.show()
# Using a window function so that, for each category, ordering each category by the id, compute the sum of each row with the following one.  
# Given:
# +---+--------+
# | id|category|
# +---+--------+
# |  1|       a|
# |  1|       a|
# |  1|       b|
# |  2|       a|
# |  2|       b|
# |  3|       b|
# +---+--------+
# Expected:
# +---+--------+---+
# | id|category|sum|
# +---+--------+---+
# |  1|       a|  2|
# |  1|       a|  3|
# |  1|       b|  3|
# |  2|       a|  2|
# |  2|       b|  5|
# |  3|       b|  3|
# +---+--------+---+



# Answer
window = Window.partitionBy("category").orderBy("id").rowsBetween(Window.currentRow, 1)
df.withColumn("sum", F.sum("id").over(window)).sort("id", "category", "sum").show()

# COMMAND ----------

# With this dataframe:
df = spark.createDataFrame([(1, 4), (2, 5), (2, 8), (3, 6), (3, 2)], ["A", "B"])
# Using a window function, for each group within A column, ordered by B, compute the difference of each row of column B with previous row of same column
# Expected:
# +---+---+----+
# |  A|  B|diff|
# +---+---+----+
# |  1|  4|null|
# |  2|  5|   3|
# |  2|  8|null|
# |  3|  2|   4|
# |  3|  6|null|
# +---+---+----+



# Answer
from pyspark.sql.window import Window
window_over_A = Window.partitionBy("A").orderBy("B")
df.withColumn("diff", F.lead("B").over(window_over_A) - df.B).show()

# COMMAND ----------

# In the following df, produce a column count which counts the number of items in each group of x column. Order the final result by both column
data = [('a', 5), ('a', 8), ('a', 7), ('b', 1),]
df = spark.createDataFrame(data, ["x", "y"])
# Expected:
# +---+---+-----+
# |  x|  y|count|
# +---+---+-----+
# |  a|  5|    3|
# |  a|  7|    3|
# |  a|  8|    3|
# |  b|  1|    1|
# +---+---+-----+



# Answer
w = Window.partitionBy('x')
df.select('x', 'y', F.count('x').over(w).alias('count')).sort('x', 'y').show()

# COMMAND ----------

# Register the dataframe in a temporary view and perform the same operation in pure SQL (using spark.sql)



# Answer
df.createOrReplaceTempView('table')
spark.sql(
  'SELECT x, y, COUNT(x) OVER (PARTITION BY x) AS n FROM table ORDER BY x, y'
).show()

# COMMAND ----------

# You have this df. Groupby Product and pivot by Country, and sum by Amount

data = [("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"), \
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"), \
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"), \
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico")]

columns= ["Product","Amount","Country"]
df = spark.createDataFrame(data = data, schema = columns)
# Given:
# +-------+------+-------+
# |Product|Amount|Country|
# +-------+------+-------+
# |Banana |1000  |USA    |
# |Carrots|1500  |USA    |
# |Beans  |1600  |USA    |
# |Orange |2000  |USA    |
# |Orange |2000  |USA    |
# |Banana |400   |China  |
# |Carrots|1200  |China  |
# |Beans  |1500  |China  |
# |Orange |4000  |China  |
# |Banana |2000  |Canada |
# |Carrots|2000  |Canada |
# |Beans  |2000  |Mexico |
# +-------+------+-------+
# Expected:
# +-------+------+-----+------+----+
# |Product|Canada|China|Mexico|USA |
# +-------+------+-----+------+----+
# |Orange |null  |4000 |null  |4000|
# |Beans  |null  |1500 |2000  |1600|
# |Banana |2000  |400  |null  |1000|
# |Carrots|2000  |1200 |null  |1500|
# +-------+------+-----+------+----+



# Answer
pivotDF = df.groupBy("Product").pivot("Country").sum("Amount")
pivotDF.show(truncate=False)

# COMMAND ----------

# Do the same as above, but forcing the order of the columns to "USA","China","Canada","Mexico"
# Expected:
# +-------+----+-----+------+------+
# |Product|USA |China|Canada|Mexico|
# +-------+----+-----+------+------+
# |Orange |4000|4000 |null  |null  |
# |Beans  |1600|1500 |null  |2000  |
# |Banana |1000|400  |2000  |null  |
# |Carrots|1500|1200 |2000  |null  |
# +-------+----+-----+------+------+

countries = ["USA","China","Canada","Mexico"]
pivotDF = df.groupBy("Product").pivot("Country", countries).sum("Amount")
pivotDF.show(truncate=False)

# COMMAND ----------

# Unpivot for Canada, China and Mexico
# Expected:
# +-------+------+-------+
# |Product|Amount|Country|
# +-------+------+-------+
# |Orange |4000  |China  |
# |Beans  |1500  |China  |
# |Beans  |2000  |Mexico |
# |Banana |2000  |Canada |
# |Banana |400   |China  |
# |Carrots|2000  |Canada |
# |Carrots|1200  |China  |
# +-------+------+-------+



# Answer
from pyspark.sql.functions import expr
unpivotExpr = "stack(3, 'Canada', Canada, 'China', China, 'Mexico', Mexico) as (Country,Amount)"
unPivotDF = pivotDF.select("Product", expr(unpivotExpr)) \
    .where("Amount is not null").select("Product","Amount","Country")
unPivotDF.show(truncate=False)

# COMMAND ----------

# Using a window function, get the ntile of the groups on id: for each id group, give the ntile(2) (category is not important column here)

from pyspark.sql import Window
tup = [(1, "a"), (1, "a"), (2, "a"), (1, "b"), (2, "b"), (3, "b"), (3, "c"), (3, "d"), (3, "e")]
df = spark.createDataFrame(tup, ["id", "category"])

# Expected:
# +---+--------+-----+
# | id|category|ntile|
# +---+--------+-----+
# |  1|       a|    1|
# |  1|       a|    1|
# |  1|       b|    2|
# |  2|       a|    1|
# |  2|       b|    2|
# |  3|       b|    1|
# |  3|       c|    1|
# |  3|       d|    2|
# |  3|       e|    2|
# +---+--------+-----+



# Answer
window = Window.partitionBy("id").orderBy("id")
df.withColumn("ntile", F.ntile(2).over(window)).sort("id", "category").show()

# COMMAND ----------

# CUBE, ROLLUP functions

# COMMAND ----------

# You have such a dataframe, build the cube function with count aggregation on both columns:
data=[("item1",2),("item2",5),("item3",20),("item2",20),("item1",10),("item1",5)]
df=spark.createDataFrame(data,["Item_Name","Quantity"])
# Expected
# +---------+--------+-----+
# |Item_Name|Quantity|count|
# +---------+--------+-----+
# |     null|    null|    6|
# |     null|       2|    1|
# |     null|       5|    2|
# |     null|      10|    1|
# |     null|      20|    2|
# |    item1|    null|    3|
# |    item1|       2|    1|
# |    item1|       5|    1|
# |    item1|      10|    1|
# |    item2|    null|    2|
# |    item2|       5|    1|
# |    item2|      20|    1|
# |    item3|    null|    1|
# |    item3|      20|    1|
# +---------+--------+-----+



# Answer
#COUNT FUNCTION
df.cube(df["Item_Name"],df["Quantity"]).count().sort("Item_Name","Quantity").show()

# COMMAND ----------

# You have such a dataframe, build the sum function with count aggregation on both columns:
data=[("item1",2),("item2",5),("item3",20),("item2",20),("item1",10),("item1",5)]
df=spark.createDataFrame(data,["Item_Name","Quantity"])
# Expected
# +---------+--------+-------------+
# |Item_Name|Quantity|sum(Quantity)|
# +---------+--------+-------------+
# |     null|    null|           62|
# |     null|       2|            2|
# |     null|       5|           10|
# |     null|      10|           10|
# |     null|      20|           40|
# |    item1|    null|           17|
# |    item1|       2|            2|
# |    item1|       5|            5|
# |    item1|      10|           10|
# |    item2|    null|           25|
# |    item2|       5|            5|
# |    item2|      20|           20|
# |    item3|    null|           20|
# |    item3|      20|           20|
# +---------+--------+-------------+


# Answer
df.cube(df["Item_Name"],df["Quantity"]).sum().sort("Item_Name","Quantity").show()

# COMMAND ----------

# build the rollup + count of the same dataframe: 
# Expected
# +---------+--------+-----+
# |Item_Name|Quantity|count|
# +---------+--------+-----+
# |     null|    null|    6|
# |    item1|    null|    3|
# |    item1|       2|    1|
# |    item1|       5|    1|
# |    item1|      10|    1|
# |    item2|    null|    2|
# |    item2|       5|    1|
# |    item2|      20|    1|
# |    item3|    null|    1|
# |    item3|      20|    1|
# +---------+--------+-----+

# Answer
df.rollup("Item_Name","Quantity").count().sort("Item_Name","Quantity").show()

# COMMAND ----------

# You have this dataframe. Produce the rollup of it according to the age. Give the grouping column g of the name, and the sum of the age. Order by the name
df = spark.createDataFrame([("Alice", 2), ("Bob", 5)], ["name", "age"])
# Expected:
# +-----+----+---+---+
# | name| age|  g|age|
# +-----+----+---+---+
# | null|null|  1|  7|
# |Alice|null|  1|  2|
# |Alice|   2|  0|  2|
# |  Bob|   5|  0|  5|
# |  Bob|null|  1|  5|
# +-----+----+---+---+



# Answer
df.rollup("name", "age").agg(F.grouping("age").alias("g"), F.sum("age").alias("age")).orderBy("name").show()

# COMMAND ----------

# Build the cube function of the same dataframe as above, and produce the grouping of the name column, and the sum over age column. Sort by name
# Expected:
# +-----+--------------+--------+
# | name|grouping(name)|sum(age)|
# +-----+--------------+--------+
# | null|             1|       7|
# |Alice|             0|       2|
# |  Bob|             0|       5|
# +-----+--------------+--------+



# Answer
df.cube("name").agg(F.grouping("name"), F.sum("age")).orderBy("name").show()

# COMMAND ----------

