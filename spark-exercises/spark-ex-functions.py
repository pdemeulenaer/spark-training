# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Spark SQL functions

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC #### Exercises on columns

# COMMAND ----------

# You have this df, create a column age_test that returns true if age is in [2,5]:
df = spark.createDataFrame([(1,),(2,),(3,),(4,),(5,)], ["age"])
# Expected:
# +---+--------+
# |age|age_test|
# +---+--------+
# |  1|   false|
# |  2|    true|
# |  3|    true|
# |  4|    true|
# |  5|   false|
# +---+--------+



# Answer
df.withColumn("age_test", F.col('age').between(2, 4)).show()

# COMMAND ----------

# from this df, return the rows that ends has "ton$" regex format:
data = [("Denton", 21), ("Jensen", 25)]
df = spark.createDataFrame(data, ["name", "age"])
# Expected:
# +------+---+
# |  name|age|
# +------+---+
# |Denton| 21|
# +------+---+



# Answer
df.filter(df.name.rlike('ton$')).show()

# COMMAND ----------

# In previous df, extract the substring of 3 characters (starting from position 1)
# Expected:
# +---+
# |col|
# +---+
# |Den|
# |Jen|
# +---+



# Answer
df.select(df.name.substr(1, 3).alias("col")).show()

# COMMAND ----------

# You have this df. Select the rows of the df where the dept_name contains letter "a"
dept = [("Finance",10), 
        ("Marketing",20), 
        ("Sales",30), 
        ("IT",40) 
      ]
deptColumns = ["name","dept_id"]
df = spark.createDataFrame(data=dept, schema = deptColumns)
# Expected:
# +---------+-------+
# |     name|dept_id|
# +---------+-------+
# |  Finance|     10|
# |Marketing|     20|
# |    Sales|     30|
# +---------+-------+



# Answer
df.filter(df.name.contains('a')).show()

# COMMAND ----------

# From same df, capture rows where name ends in "ance"
# Expected
# +-------+-------+
# |   name|dept_id|
# +-------+-------+
# |Finance|     10|
# +-------+-------+



# Answer
df.filter(df.name.endswith('ance')).show()

# COMMAND ----------

# you have such a dataframe with a map:
data = [("Messi", {"goals": "93", "ballondOr": "6"}), ("Ronaldo", {"ballondOr": "5", "team": "juventus"})]
df = spark.createDataFrame(data, ["player", "data"])
# create a new column "data" which contains the value of the key "ballondOr":
# Expected:
# +-------+----------------------------------+---------+
# |player |data                              |ballondOr|
# +-------+----------------------------------+---------+
# |Messi  |[ballondOr -> 6, goals -> 93]     |6        |
# |Ronaldo|[ballondOr -> 5, team -> juventus]|5        |
# +-------+----------------------------------+---------+



# Answer
df.withColumn("ballondOr", F.col("data")["ballondOr"]).show(truncate=False)

# COMMAND ----------

# In this df, compare the value with the string "Ronaldo", first using ==, then using a safe Null comparator. Also compare with Null:

from pyspark.sql import Row
df1 = spark.createDataFrame([
    Row(id=1, value='Ronaldo'),
    Row(id=2, value=None)
])
# Expected:
# +-----------------+-------------------+----------------+
# |(value = Ronaldo)|(value <=> Ronaldo)|(value <=> NULL)|
# +-----------------+-------------------+----------------+
# |             true|               true|           false|
# |             null|              false|            true|
# +-----------------+-------------------+----------------+



# Answer
df1.select(
    df1['value'] == 'Ronaldo',
    df1['value'].eqNullSafe('Ronaldo'),
    df1['value'].eqNullSafe(None)
).show()

# COMMAND ----------

# You have these df's. Join on the value of df1 to the one of df2, using ==, then with a null-safe comparator

from pyspark.sql import Row
df1 = spark.createDataFrame([
    Row(id=1, value='Ronaldo'),
    Row(id=2, value=None)
])

df2 = spark.createDataFrame([
    Row(value = 'Messi'),
    Row(value = None)
])
# Expected:
# +---+-----+-----+
# | id|value|value|
# +---+-----+-----+
# +---+-----+-----+

# +---+-----+-----+
# | id|value|value|
# +---+-----+-----+
# |  2| null| null|
# +---+-----+-----+



# Answer
df1.join(df2, df1["value"] == df2["value"]).show()

df1.join(df2, df1["value"].eqNullSafe(df2["value"])).show()

# COMMAND ----------

# In this df, test if NUll, nan, and 42 in a null-safe way
df = spark.createDataFrame([
    Row(id=1, value=float('NaN')),
    Row(id=2, value=42.0),
    Row(id=3, value=None)
])
# Expected:
# +----------------+---------------+----------------+
# |(value <=> NULL)|(value <=> NaN)|(value <=> 42.0)|
# +----------------+---------------+----------------+
# |           false|           true|           false|
# |           false|          false|            true|
# |            true|          false|           false|
# +----------------+---------------+----------------+



# Answer
df.select(
    df['value'].eqNullSafe(None),
    df['value'].eqNullSafe(float('NaN')),
    df['value'].eqNullSafe(42.0)
).show()

# COMMAND ----------

# You have this df. Select the rows of the df where the name is in the list ["Finance", "Math"]
data = [("Ronaldo",2500), 
        ("Messi",2800), 
        ("Hazard",300), 
        ("Zlatan",400) 
      ]
df = spark.createDataFrame(data=data, schema = ["name","score"])
# Expected:
# +-------+-----+
# |   name|score|
# +-------+-----+
# |Ronaldo| 2500|
# |  Messi| 2800|
# +-------+-----+



# Answer
df.filter(df.name.isin(["Ronaldo", "Messi"])).show()

# COMMAND ----------

# You have this df. Select the rows of the df where the name is like Zl% pattern
data = [("Ronaldo",2500), 
        ("Messi",2800), 
        ("Hazard",300), 
        ("Zlatan",400) 
      ]
df = spark.createDataFrame(data=data, schema = ["name","score"])
# Expected:
# +------+-----+
# |  name|score|
# +------+-----+
# |Zlatan|  400|
# +------+-----+



# Answer
df.filter(df.name.like("Zl%")).show()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC #### Exercises on pyspark.sql functions

# COMMAND ----------

# You have such dataframe. Recreate columns A and B and limit them to 2 decimals, and show the updated dataframe
df = spark.createDataFrame([(1.787, 4.3434), (2.5655, 8.67676), (2.23245, 6.676746)], ["A", "B"]) 
# Expected:
# +----+----+
# |   A|   B|
# +----+----+
# |1.79|4.34|
# |2.57|8.68|
# |2.23|6.68|
# +----+----+



# Answer
df = df.withColumn("A", F.round(F.col("A"),2)).withColumn("B", F.round(F.col("B"),2))
df.show()

# COMMAND ----------

# Using previous dataframe, cast the first column to "long" type
# Expected:
# +---+----+
# |  A|   B|
# +---+----+
# |  1|4.34|
# |  2|8.68|
# |  2|6.68|
# +---+----+


# Answer
df.withColumn("A", F.col("A").cast("long")).show()

# COMMAND ----------

# From following dataframe, group by A and for each group, give the list of distinct elements
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

# in this df, select the dept, salary and a new column splitName, which contains the names splitted, as an array
import pyspark.sql.functions as F
df = spark.createDataFrame([
                    ['Junker D',"Germany",2000],
                    ['Wulf F',"Germany",13000],
                    ['Mary J','US',20000]],('name','country','salary'))
# Expected:
# +-------+------+-----------+
# |country|salary| split_name|
# +-------+------+-----------+
# |Germany|  2000|[Junker, D]|
# |Germany| 13000|  [Wulf, F]|
# |     US| 20000|  [Mary, J]|
# +-------+------+-----------+

df = df.withColumn("split_name", F.split(F.col("name"), " "))
df.select("country", "salary", "split_name").show() 

# COMMAND ----------

# You have this df. Select the rows of the df where the name is above 1000 and give a value 1, otherwise 0
data = [("Ronaldo",2500), 
        ("Messi",2800), 
        ("Hazard",300), 
        ("Zlatan",400) 
      ]
df = spark.createDataFrame(data=data, schema = ["name","score"])
# Expected:
# +-------+-----+------+
# |   name|score|status|
# +-------+-----+------+
# |Ronaldo| 2500|winner|
# |  Messi| 2800|winner|
# | Hazard|  300|looser|
# | Zlatan|  400|looser|
# +-------+-----+------+



# Answer
df.withColumn("status", F.when(F.col("score")>=1000, "winner").otherwise("looser")).show()

# COMMAND ----------

# You have this dataframe. Create a new column myInt which is the exploded version of the array in column myList. Display the new, full dataframe
from pyspark.sql import Row
df = spark.createDataFrame([Row(a=1, myList=[1,2,3], myMap={"a": "b"})])
# Expected:
# +---+---------+--------+-----+
# |  a|   myList|   myMap|myInt|
# +---+---------+--------+-----+
# |  1|[1, 2, 3]|[a -> b]|    1|
# |  1|[1, 2, 3]|[a -> b]|    2|
# |  1|[1, 2, 3]|[a -> b]|    3|
# +---+---------+--------+-----+



# Answer
df = df.select('*',F.explode("mylist").alias("myInt"))
df.show()

# COMMAND ----------

# From previous dataframe, explode the map myMap in 2 columns, key and value. Display the new, full dataframe
# Expected:
# +---+---------+--------+-----+---+-----+
# |  a|   myList|   myMap|myInt|key|value|
# +---+---------+--------+-----+---+-----+
# |  1|[1, 2, 3]|[a -> b]|    1|  a|    b|
# |  1|[1, 2, 3]|[a -> b]|    2|  a|    b|
# |  1|[1, 2, 3]|[a -> b]|    3|  a|    b|
# +---+---------+--------+-----+---+-----+



# Answer
df.select('*',F.explode("myMap").alias("key", "value")).show()

# COMMAND ----------

# From the following dataframe, create a map based on 2 columns, one which will be used for the key playerName, the other as the value goalsScored, call that map playerMap. Show the full map
df= spark.createDataFrame([
                ['Messi',11000],
                ['Ronaldo',12000],
                ['Hazard',300],
                ['Griezmann',200],
                ['Benzema',400]],('playerName','goalsScored'))
# Expected:
# +------------------+
# |playerMap         |
# +------------------+
# |[Messi -> 11000]  |
# |[Ronaldo -> 12000]|
# |[Hazard -> 300]   |
# |[Griezmann -> 200]|
# |[Benzema -> 400]  |
# +------------------+



# Answer
df.select(F.create_map('playerName','goalsScored').alias('playerMap')).show(truncate=False)

# COMMAND ----------

# You have this dataframe with an array. Get the first  element of each row. Call that column "first" 
df = spark.createDataFrame([(["a", "b", "c"],), (["f", "d", "e"],), ([],)], ['data'])
# Expected:
# +-----+
# |first|
# +-----+
# |    a|
# |    f|
# | null|
# +-----+



# Answer
df.select(F.element_at(df.data, 1).alias("first")).show()

# COMMAND ----------

# same for a map
df = spark.createDataFrame([({"Hazard": 1.0, "De Bruyne": 2.0},), ({},)], ['data'])
# Expected:
# +-----+
# |first|
# +-----+
# |  1.0|
# | null|
# +-----+



# Answer
df.select(F.element_at(df.data, F.lit("Hazard")).alias("first")).show()

# COMMAND ----------

# You have the dataframe given by this:
df = spark.sql("SELECT sequence(to_date('2018-01-01'), to_date('2018-03-01'), interval 1 month) as date")
# Now explode this array, to get all the dates into one column
# Expected:
# +----------+
# |      date|
# +----------+
# |2018-01-01|
# |2018-02-01|
# |2018-03-01|
# +----------+



# Answer
df.withColumn("date", F.explode(F.col("date"))).show()

# COMMAND ----------

# you have the following df, try to "explode" both columns time and value simultaneously
from pyspark.sql import Row
df = spark.createDataFrame([Row(customer=1, time=[1,2,3],value=[7,8,9]), Row(customer=2, time=[4,5,6],value=[10,11,12])])
# Expected:
# +--------+----+-----+
# |customer|time|value|
# +--------+----+-----+
# |       1|   1|    7|
# |       1|   2|    8|
# |       1|   3|    9|
# |       2|   4|   10|
# |       2|   5|   11|
# |       2|   6|   12|
# +--------+----+-----+


# Answer
df_exploded = (df.rdd
               .flatMap(lambda row: [(row.customer, b, c) for b, c in zip(row.time, row.value)])
               .toDF(['customer', 'time', 'value']))
df_exploded.show()

# COMMAND ----------

# In this dataframe, select a column "manita" being a reformated to have 5 digits after the comma:
df = spark.createDataFrame([(5,)], ['goals'])
# Expected:
# +------+
# |manita|
# +------+
# |5.0000|
# +------+



# Answer
df.select(F.format_number('goals', 4).alias('manita')).show()

# COMMAND ----------

# In this dataframe, select a column "manita" being a formatted version of a and b:
df = spark.createDataFrame([(5, "goals")], ['a', 'b'])
# Expected:
# +-------+
# | manita|
# +-------+
# |5 goals|
# +-------+



# Answer
df.select(F.format_string('%d %s', df.a, df.b).alias('manita')).show()

# COMMAND ----------

# You have this dataframe. The second column is a string containing a json object. You need to select the first column key, as well as:
# - a column "Belgium" made of the values of the key f1
# - a column "Argentina" made of the values of the key f2
data = [("1", '''{"f1": "Hazard", "f2": "Messi"}'''), ("2", '''{"f1": "De Bruyne"}''')]
df = spark.createDataFrame(data, ("key", "jstring"))
# Expected:
# +---+---------+---------+
# |key|  Belgium|Argentina|
# +---+---------+---------+
# |  1|   Hazard|    Messi|
# |  2|De Bruyne|     null|
# +---+---------+---------+



# Answer
df.select("key", F.get_json_object("jstring", '$.f1').alias("Belgium"), \
                 F.get_json_object("jstring", '$.f2').alias("Argentina") ).show()

# COMMAND ----------

# You have the following dataframe. Produce the hash of column 'Player'. Display both columns
df = spark.createDataFrame([('Neymar',), ('Hazard',)], ['Players'])
# Expected:
# +-------+-----------+
# |Players|       hash|
# +-------+-----------+
# | Neymar|-1320182336|
# | Hazard|  163122813|
# +-------+-----------+



# Answer
df.select('Players', F.hash('Players').alias('hash')).show()

# COMMAND ----------

# Create a column "PlayerIsNull" which tests if column a is null or not. Display all columns
df = spark.createDataFrame([("Bale", None), (None, 2)], ("Player", "Goals"))
# Expected:
# +------+-----+------------+
# |Player|Goals|PlayerIsNull|
# +------+-----+------------+
# |  Bale| null|       false|
# |  null|    2|        true|
# +------+-----+------------+



# Answer
df.select("Player", "Goals", F.isnull("Player").alias("PlayerIsNull")).show()

# COMMAND ----------

# Using the following data, create a dataframe with a column jstring having the json string
data = [("1", '''{"Brazil": "Romario", "Italy": "Baggio"}'''), ("2", '''{"Brazil": "Bebeto"}''')]
# Then split the json string per json key, in 2 columns, "Brazil" and "Italy". Print all columns, without truncating
# Expected:
# +---+----------------------------------------+-------+------+
# |key|jstring                                 |Brazil |Italy |
# +---+----------------------------------------+-------+------+
# |1  |{"Brazil": "Romario", "Italy": "Baggio"}|Romario|Baggio|
# |2  |{"Brazil": "Bebeto"}                    |Bebeto |null  |
# +---+----------------------------------------+-------+------+



# Answer
df = spark.createDataFrame(data, ("key", "jstring"))
df.select("key", "jstring", F.json_tuple("jstring", 'Brazil', 'Italy').alias("Brazil", "Italy")).show(truncate=False)

# COMMAND ----------

# In this dataframe, transform the string into a CSV string
df = spark.createDataFrame([("1,2,3",)], ("value",))
# Expected:
# +---------+
# |      csv|
# +---------+
# |[1, 2, 3]|
# +---------+



# Answer
df.select(F.from_csv(df.value, "a INT, b INT, c INT").alias("csv")).show()
df.printSchema()

# COMMAND ----------

# you have a df with 3 columns. select for each row the greatest element
df = spark.createDataFrame([(1, 4, 3), (7, 4, 3)], ['Hazard', 'De Bruyne', 'Kompany'])
# Expected:
# +--------+
# |greatest|
# +--------+
# |       4|
# |       7|
# +--------+



# Answer
df.select(F.greatest(F.col("Hazard"), F.col("De Bruyne") ,F.col("Kompany")).alias("greatest")).show()

# COMMAND ----------

# In following dataframe, get the length of the strings in column a
df = spark.createDataFrame([('Messi vs Ronaldo ',)], ['GOAT'])
# Expected:
# +------+
# |length|
# +------+
# |    17|
# +------+



# Answer
df.select(F.length('GOAT').alias('length')).show()

# COMMAND ----------

# Pad the following column s by character $, for a total of 6 characters
df = spark.createDataFrame([('Bale',)], ['s',])
# Expected:
# +------+
# |     s|
# +------+
# |$$Bale|
# +------+



# Answer
df.select(F.lpad("s", 6, '$').alias('s')).show()

# COMMAND ----------

# find the first occurance of the substring "van" in the following column (index is one-based, not zero-based)
df = spark.createDataFrame([('Daniel van Buyten',)], ['Defensor',])
# Expected:
# +---+
# |  s|
# +---+
# |  8|
# +---+



# Answer
df.select(F.locate('van', "Defensor", 1).alias('s')).show()

# COMMAND ----------

# Left-trim the following column s
df = spark.createDataFrame([('   Lukaku',)], ['Best',])
# Expected:
# +------+
# |  Best|
# +------+
# |Lukaku|
# +------+



# Answer
df.select(F.ltrim("Best").alias('Best')).show()

# COMMAND ----------

# You have the following dataframe containing 2 columns, each being a map:
df = spark.sql("SELECT map(1, 'Lukaku', 2, 'Zlatan') as map1, map(3, 'Eriksen') as map2")
# Concatenate both maps into one. Show without truncating
# Expected:
# +----------------------------------------+
# |map3                                    |
# +----------------------------------------+
# |[1 -> Lukaku, 2 -> Zlatan, 3 -> Eriksen]|
# +----------------------------------------+



# Answer
df.select(F.map_concat("map1", "map2").alias("map3")).show(truncate=False)

# COMMAND ----------

# You have the following dataframe with 2 array columns. Create a column map, where the keys are column k, and values are column v. Show it without truncate.
df = spark.createDataFrame([(['Henry', 'Zidane'], [2, 5])], ['k', 'v'])
# Expected:
# +-------------------------+
# |map                      |
# +-------------------------+
# |[Henry -> 2, Zidane -> 5]|
# +-------------------------+



# Answer
df.select(F.map_from_arrays("k", "v").alias("map")).show(truncate=False)

# COMMAND ----------

# You have the following dataframe containing an array of arrays. Convert it into a map
df = spark.sql("SELECT array(struct(1, 'Henry'), struct(2, 'Zidane')) as data")
# Given:
# +-------------------------+
# |data                     |
# +-------------------------+
# |[[1, Henry], [2, Zidane]]|
# +-------------------------+
# Expected:
# +-------------------------+
# |map                      |
# +-------------------------+
# |[1 -> Henry, 2 -> Zidane]|
# +-------------------------+



# Answer
df.select(F.map_from_entries("data").alias("map")).show(truncate=False)

# COMMAND ----------

# you have this dataframe. Get the keys into one column, and the values into another one. Display the full df
df = spark.sql("SELECT map(1, 'Henry', 2, 'Zidane') as data")
# Expected:
# +-------------------------+------+---------------+
# |data                     |keys  |values         |
# +-------------------------+------+---------------+
# |[1 -> Henry, 2 -> Zidane]|[1, 2]|[Henry, Zidane]|
# +-------------------------+------+---------------+



# Answer
df.withColumn("keys",F.map_keys("data")).withColumn("values",F.map_values("data")).show(truncate=False)

# COMMAND ----------

# You have this dataframe. Create a monotonically increasing id column. Select it and the date column
df = spark.createDataFrame(["2017-01-01", "2018-02-08", "2019-01-03"], "string").withColumnRenamed("value", "date")
# Expected like:
# +-----------+----------+
# |         id|      date|
# +-----------+----------+
# |17179869184|2017-01-01|
# |42949672960|2018-02-08|
# |60129542144|2019-01-03|
# +-----------+----------+



# Answer
df.select(F.monotonically_increasing_id().alias('id'), "date").show()

# COMMAND ----------

# You have this df. produce a column arr from a and b
df = spark.createDataFrame(
    [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
    ("a", "b")) 
# Expected:
# +---+----+-----------+
# |  a|   b|        arr|
# +---+----+-----------+
# |  1| 1.0| [1.0, 1.0]|
# |  1| 2.0| [1.0, 2.0]|
# |  2| 3.0| [2.0, 3.0]|
# |  2| 5.0| [2.0, 5.0]|
# |  2|10.0|[2.0, 10.0]|
# +---+----+-----------+



# Answer
df.withColumn("arr", F.array('a', 'b')).show()

# COMMAND ----------

# in these arrays, keep only the distinct values:
df = spark.createDataFrame([([1, 2, 3, 2],), ([4, 5, 5, 4],)], ['data'])
# Expected:
# +---------+
# |      arr|
# +---------+
# |[1, 2, 3]|
# |   [4, 5]|
# +---------+



# Answer
df.select(F.array_distinct(df.data).alias('arr')).show()

# COMMAND ----------

# check if arrays in column "data" contain the string item "Al", and call the new column "contains_Al". Print all columns
df = spark.createDataFrame([(["Ron", "Al", "Do"],), ([],)], ['data'])
# Expected:
# +-------------+-----------+
# |         data|contains_Al|
# +-------------+-----------+
# |[Ron, Al, Do]|       true|
# |           []|      false|
# +-------------+-----------+



# Answer
df.select('data', F.array_contains(df.data, "Al").alias('contains_Al')).show()
#or
#df.select('data', F.array_contains(df.data, F.lit("Al")).alias('contains_Al')).show()

# COMMAND ----------

# Return an array of the elements in team1 but not in team2, without duplicates, call that column "except" and show it.
from pyspark.sql import Row
df = spark.createDataFrame([Row(team1=["Vazquez", "Kroos", "Hazard", "Ramos"], team2=["Ramos", "Benzema", "Hazard", "Courtois"])])
# Expected:
# +----------------+
# |          except|
# +----------------+
# |[Vazquez, Kroos]|
# +----------------+



# Answer
df.select(F.array_except(df.team1, df.team2).alias("except")).show()

# COMMAND ----------

# Get the intersection of both arrays:
from pyspark.sql import Row
df = spark.createDataFrame([Row(team1=["Vazquez", "Kroos", "Hazard", "Ramos"], team2=["Ramos", "Benzema", "Hazard", "Courtois"])])
# Expected:
# +-----------------------------+
# |array_intersect(team1, team2)|
# +-----------------------------+
# |              [Hazard, Ramos]|
# +-----------------------------+



# Answer
df.select(F.array_intersect(df.team1, df.team2)).show()

# COMMAND ----------

# Concatenate the elements of the array (for each row) into a string. Do this twice:
df = spark.createDataFrame([(["Ribery", "Gallas", "Henry"],), (["Ribery", None],)], ['players'])
# - ignore the Nulls
# Expected:
# +-------------------+
# |             joined|
# +-------------------+
# |Ribery,Gallas,Henry|
# |             Ribery|
# +-------------------+
# - include the Nulls
# +-------------------+
# |             joined|
# +-------------------+
# |Ribery,Gallas,Henry|
# |        Ribery,NULL|
# +-------------------+



# Answer
df.select(F.array_join(df.players, ",").alias("joined")).show()
df.select(F.array_join(df.players, ",", "NULL").alias("joined")).show()

# COMMAND ----------

# Get the max of the array
df = spark.createDataFrame([([2, 1, 3],), ([None, 10, -1],)], ['data'])
# Expected:
# +---+
# |max|
# +---+
# |  3|
# | 10|
# +---+



# Answer
df.select(F.array_max(df.data).alias('max')).show()

# COMMAND ----------

# Get the position of element "Messi" in arrays of column 'Barcelona':
df = spark.createDataFrame([(["Puig", "Ter Stegen", "Messi"],), ([],)], ['Barcelona'])
# Expected:
# +---+
# |pos|
# +---+
# |  3|
# |  0|
# +---+



# Answer
df.select(F.array_position(df.Barcelona, "Messi").alias('pos')).show()

# COMMAND ----------

# remove the value 1 from following array:
df = spark.createDataFrame([([1, 2, 3, 1, 1],), ([],)], ['data'])
# Expected:
# +------+
# |   arr|
# +------+
# |[2, 3]|
# |    []|
# +------+



# Answer
df.select(F.array_remove(df.data, 1).alias('arr')).show()

# COMMAND ----------

# repeat the following array 3 times, call new column "WhoScored"...and do not truncate the show!
df = spark.createDataFrame([('Messiiiiiiii!!! ',)], ['data'])
# Expected:
# +------------------------------------------------------+
# |WhoScored                                             |
# +------------------------------------------------------+
# |[Messiiiiiiii!!! , Messiiiiiiii!!! , Messiiiiiiii!!! ]|
# +------------------------------------------------------+



# Answer
df.select(F.array_repeat(df.data, 3).alias('WhoScored')).show(truncate=False)

# COMMAND ----------

# Sort the array:
df = spark.createDataFrame([([2, 1, None, 3],),([1],),([],)], ['data'])
# Expected:
# +----------+
# |         r|
# +----------+
# |[1, 2, 3,]|
# |       [1]|
# |        []|
# +----------+



# Answer
df.select(F.array_sort(df.data).alias('r')).show()

# COMMAND ----------

# Union both array columns, call result column "real":
from pyspark.sql import Row
df = spark.createDataFrame([Row(team1=["Vazquez", "Kroos", "Hazard", "Ramos"], team2=["Ramos", "Benzema", "Hazard", "Courtois"])])
# Expected:
# +--------------------------------------------------+
# |real                                              |
# +--------------------------------------------------+
# |[Vazquez, Kroos, Hazard, Ramos, Benzema, Courtois]|
# +--------------------------------------------------+



# Answer
df.select(F.array_union(df.team1, df.team2).alias('real')).show(truncate=False)

# COMMAND ----------

# Create a column "overlap" that returns true if the arrays contain any common non-null element; if not, returns null if both the arrays are non-empty and any of them contains a null element; returns false otherwise. Show all columns
df = spark.createDataFrame([(["a", "b"], ["b", "c"]), (["a"], ["b", "c"])], ['x', 'y'])
# Expected:
# +------+------+-------+
# |     x|     y|overlap|
# +------+------+-------+
# |[a, b]|[b, c]|   true|
# |   [a]|[b, c]|  false|
# +------+------+-------+



# Answer
df.select('x', 'y', F.arrays_overlap(df.x, df.y).alias("overlap")).show()

# COMMAND ----------

# From this dataframe, create a column "zipped" that returns a column which contains the zipped arrays 'vals1' and 'vals2' as shown in result:
df = spark.createDataFrame([(([1, 2, 3], [2, 3, 4]))], ['vals1', 'vals2'])
# Expected
# +------------------------+
# |zipped                  |
# +------------------------+
# |[[1, 2], [2, 3], [3, 4]]|
# +------------------------+



# Answer
df.select(F.arrays_zip(df.vals1, df.vals2).alias('zipped')).show(truncate=False)

# COMMAND ----------

# You have 2 columns. Return a column that gives the first element of the 2 columns which is not NUll. Show all columns.
df = spark.createDataFrame([(None, None), (1, None), (None, 2)], ("a", "b"))
# Expected:
# +----+----+--------------+
# |   a|   b|coalesce(a, b)|
# +----+----+--------------+
# |null|null|          null|
# |   1|null|             1|
# |null|   2|             2|
# +----+----+--------------+



# Answer
df.select('*',F.coalesce(df["a"], df["b"])).show()

# COMMAND ----------

# Collect all the element of the column into an array:
df = spark.createDataFrame([(2,), (5,), (5,)], ('age',))
# Expected:
# +---------+
# |        a|
# +---------+
# |[2, 5, 5]|
# +---------+



# Answer
df.agg(F.collect_list('age').alias("a")).show()

# COMMAND ----------

# Now do the same, but extracting only the distinct elements:
df = spark.createDataFrame([(2,), (5,), (5,)], ('age',))
# Expected:


# Answer
df.agg(F.collect_set('age').alias("a")).show()

# COMMAND ----------

# Concatenate the following 2 columns into one, "player":
df = spark.createDataFrame([('Leo ','33')], ['name', 'age'])
# Expected:
# +------+
# |player|
# +------+
# |Leo 33|
# +------+



# Answer
df.select(F.concat(df.name, df.age).alias('player')).show()

# COMMAND ----------

# Again, concatenate the 3 columns into one, "arr":
df = spark.createDataFrame([([1, 2], [3, 4], [5]), ([1, 2], None, [3])], ['a', 'b', 'c'])
# Expected:
# +---------------+
# |            arr|
# +---------------+
# |[1, 2, 3, 4, 5]|
# |           null|
# +---------------+


# Answer
df.select(F.concat(df.a, df.b, df.c).alias("arr")).show()

# COMMAND ----------

# Now concatenate 2 columns with a symbol - between them, into the column "player":
df = spark.createDataFrame([('Zinedine','Zidane')], ['name', 'surname'])
# Expected:
# +---------------+
# |         player|
# +---------------+
# |Zinedine-Zidane|
# +---------------+



# Answer
df.select(F.concat_ws('-', df.name, df.surname).alias('player')).show()

# COMMAND ----------

# Convert col of binary number (base 2) to base 16 number, into column "hex":
df = spark.createDataFrame([("010101",)], ['n'])
# Expected:
# +---+
# |hex|
# +---+
# | 15|
# +---+



# Answer
df.select(F.conv(df.n, 2, 16).alias('hex')).show()

# COMMAND ----------

# Get the distinct number of rows, i.e. the distinct count based on both columns simultaneously, call result 'countDistinct':
df = spark.createDataFrame([('Hazard','26'), ('Messi','123'), ('Messi','123'), ('Ronaldo','135'), ('Trump','0')], ['player', 'goals'])
# Expected:
# +-------------+
# |countDistinct|
# +-------------+
# |            4|
# +-------------+



# Answer
df.select(F.countDistinct('player', 'goals').alias('countDistinct')).show()

# COMMAND ----------

# in the following df, explode the map_a into key and value. Do it a second time, keeping all the rows, even when only Null:
df = spark.createDataFrame(
    [(1, ["foo", "bar"], {"x": 1.0}), (2, [], {}), (3, None, None)],
    ("id", "an_array", "a_map")
)
# Expected:
# +---+----------+---------+-----------+
# | id|  an_array|key_a_map|value_map_a|
# +---+----------+---------+-----------+
# |  1|[foo, bar]|        x|        1.0|
# +---+----------+---------+-----------+

# +---+----------+---------+-----------+
# | id|  an_array|key_a_map|value_map_a|
# +---+----------+---------+-----------+
# |  1|[foo, bar]|        x|        1.0|
# |  2|        []|     null|       null|
# |  3|      null|     null|       null|
# +---+----------+---------+-----------+



# Answer
df.select("id", "an_array", F.explode("a_map").alias("key_a_map", "value_map_a")).show()
df.select("id", "an_array", F.explode_outer("a_map").alias("key_a_map", "value_map_a")).show()

# COMMAND ----------

# Do the same here for the array:
# Expected:
# +---+----------+----------+--------------+
# | id|  an_array|     a_map|exploded_array|
# +---+----------+----------+--------------+
# |  1|[foo, bar]|[x -> 1.0]|           foo|
# |  1|[foo, bar]|[x -> 1.0]|           bar|
# +---+----------+----------+--------------+

# +---+----------+----------+--------------+
# | id|  an_array|     a_map|exploded_array|
# +---+----------+----------+--------------+
# |  1|[foo, bar]|[x -> 1.0]|           foo|
# |  1|[foo, bar]|[x -> 1.0]|           bar|
# |  2|        []|        []|          null|
# |  3|      null|      null|          null|
# +---+----------+----------+--------------+



# Answer
df.select("id", "an_array", "a_map", F.explode("an_array").alias("exploded_array")).show()
df.select("id", "an_array", "a_map", F.explode_outer("an_array").alias("exploded_array")).show()

# COMMAND ----------

# In next df, overlay the first string column by the 2nd one, starting at the 13th character of the first string. Call result "best" and show without truncating
df = spark.createDataFrame([("the best is Berkamp", "Batistuta")], ("x", "y"))
# Expected:
# +---------------------+
# |best                 |
# +---------------------+
# |the best is Batistuta|
# +---------------------+



# Answer
df.select(F.overlay("x", "y", 13).alias("best")).show(truncate=False)

# COMMAND ----------

# Using posexplode on array intlist, get the position of each element, and the element itself
from pyspark.sql import Row
df = spark.createDataFrame([Row(a=1, intlist=[1,2,3], mapfield={"a": "b"})])
# Expected:
# +---+---+
# |pos|col|
# +---+---+
# |  0|  1|
# |  1|  2|
# |  2|  3|
# +---+---+



# Answer
df.select(F.posexplode(df.intlist)).show()

# COMMAND ----------

# Now explode the the map. Call the resulting column "myPos","myKey","myValue".
# Expected:
# +-----+-----+-------+
# |myPos|myKey|myValue|
# +-----+-----+-------+
# |    0|    a|      b|
# +-----+-----+-------+



# Answer
df.select(F.posexplode(df.mapfield).alias("myPos","myKey","myValue")).show()

# COMMAND ----------

# In this dataframe, explode the array, keeping the Nulls rows. Then do the same for the map
df = spark.createDataFrame(
    [(1, ["foo", "bar"], {"x": 1.0}), (2, [], {}), (3, None, None)],
    ("id", "an_array", "a_map")
)
# Expected:
# +---+----------+----+----+-----+
# | id|  an_array| pos| key|value|
# +---+----------+----+----+-----+
# |  1|[foo, bar]|   0|   x|  1.0|
# |  2|        []|null|null| null|
# |  3|      null|null|null| null|
# +---+----------+----+----+-----+

# +---+----------+----+----+
# | id|     a_map| pos| col|
# +---+----------+----+----+
# |  1|[x -> 1.0]|   0| foo|
# |  1|[x -> 1.0]|   1| bar|
# |  2|        []|null|null|
# |  3|      null|null|null|
# +---+----------+----+----+



# Answer
df.select("id", "an_array", F.posexplode_outer("a_map")).show()
df.select("id", "a_map", F.posexplode_outer("an_array")).show()

# COMMAND ----------

# Repeat 3 times the string in col s
df = spark.createDataFrame([('he scored! ',)], ['s',])
# Expected:
# +---------------------------------+
# |s                                |
# +---------------------------------+
# |he scored! he scored! he scored! |
# +---------------------------------+



# Answer
df.select(F.repeat(df.s, 3).alias('s')).show(truncate=False)

# COMMAND ----------

# extract the value 100 using regexp:
df = spark.createDataFrame([('100-200',)], ['str'])
# Expected:
# +---+
# |  d|
# +---+
# |100|
# +---+



# Answer
df.select(F.regexp_extract('str', r'(\d+)-(\d+)', 1).alias('d')).show()

# COMMAND ----------

# replace the value matching the regexp r'(\d+)' by ##
df = spark.createDataFrame([('100-200',)], ['str'])
# Expected:
# +-----+
# |    d|
# +-----+
# |##-##|
# +-----+



# Answer
df.select(F.regexp_replace('str', r'(\d+)', '##').alias('d')).show()

# COMMAND ----------

# return a column s which is the reverse string of the column data
df = spark.createDataFrame([('Who is the GOAT?',)], ['data'])
# Expected:
# +----------------+
# |               s|
# +----------------+
# |?TAOG eht si ohW|
# +----------------+



# Answer
df.select(F.reverse(df.data).alias('s')).show()

# COMMAND ----------

# right pad the string, with 18 characters at most. Don't truncate the show :)
df = spark.createDataFrame([('Hazard earned a lot of ',)], ['s',])
# Expected:
# +------------------------------+
# |s                             |
# +------------------------------+
# |Hazard earned a lot of $$$$$$$|
# +------------------------------+



# Answer
df.select(F.rpad(df.s, 30, '$').alias('s')).show(truncate=False)

# COMMAND ----------

# parse the string '1|a' and parse its schema
# Expected:
# +--------------------------+
# |csv                       |
# +--------------------------+
# |struct<_c0:int,_c1:string>|
# +--------------------------+



# Answer
df = spark.range(1)
df.select(F.schema_of_csv(F.lit('1|a'), {'sep':'|'}).alias("csv")).show(truncate=False)

# COMMAND ----------

# parse the json string '{"a": 0}' and parse its schema
# Expected:
# +----------------+
# |            json|
# +----------------+
# |struct<a:bigint>|
# +----------------+



# Answer
df = spark.range(1)
df.select(F.schema_of_json(F.lit('{"a": 0}')).alias("json")).show()

# COMMAND ----------

# Using this df, create a column r that contains a sequence between C1 up to C2, by step 1
df = spark.createDataFrame([(-2, 2)], ('C1', 'C2'))
# Expected:
# +-----------------+
# |                r|
# +-----------------+
# |[-2, -1, 0, 1, 2]|
# +-----------------+



# Answer
df.select(F.sequence('C1', 'C2').alias('r')).show()

# COMMAND ----------

# Shuffle the arrays in column "data"
df = spark.createDataFrame([([1, 20, 3, 5],), ([1, 20, None, 3],)], ['data'])
# Expected:
# +-------------+
# |            s|
# +-------------+
# |[1, 20, 3, 5]|
# | [, 3, 20, 1]|
# +-------------+



# Answer
df.select(F.shuffle(df.data).alias('s')).show()

# COMMAND ----------

# Get the size of the array:
df = spark.createDataFrame([([1, 2, 3],),([1],),([],)], ['data'])
# Expected:
# +----------+
# |size(data)|
# +----------+
# |         3|
# |         1|
# |         0|
# +----------+



# Answer
df.select(F.size(df.data)).show()

# COMMAND ----------

# Get a slice of the array, start at position 2, size of slice 2:
df = spark.createDataFrame([([1, 2, 3],), ([4, 5],)], ['x'])
# Expected:
# +------+
# |sliced|
# +------+
# |[2, 3]|
# |   [5]|
# +------+



# Answer
df.select(F.slice(df.x, 2, 2).alias("sliced")).show()

# COMMAND ----------

# Sort the following array in asc and second time then in desc order:
df = spark.createDataFrame([([2, 1, None, 3],),([1],),([],)], ['data'])
# Expected:
# +-----------+
# |          r|
# +-----------+
# |[, 1, 2, 3]|
# |        [1]|
# |         []|
# +-----------+

# +----------+
# |         r|
# +----------+
# |[3, 2, 1,]|
# |       [1]|
# |        []|
# +----------+



# Answer
df.select(F.sort_array(df.data).alias('r')).show()
df.select(F.sort_array(df.data, asc=False).alias('r')).show()

# COMMAND ----------

# Split the array s using either on of the characters in the list [ABC]
# - do it until second occurance of such character
# - do it until end of string
df = spark.createDataFrame([('oneAtwoBthreeC',)], ['s',])
# Expected:
# +-----------------+
# |                s|
# +-----------------+
# |[one, twoBthreeC]|
# +-----------------+

# +-------------------+
# |                  s|
# +-------------------+
# |[one, two, three, ]|
# +-------------------+



# Answer
df.select(F.split(df.s, '[ABC]', 2).alias('s')).show()
df.select(F.split(df.s, '[ABC]', -1).alias('s')).show()

# COMMAND ----------

# Convert the value array to a csv string
from pyspark.sql import Row
data = [(1, Row(name='Messi', age=33))]
df = spark.createDataFrame(data, ("key", "value"))
# Expected
# +--------+
# |     csv|
# +--------+
# |Messi,33|
# +--------+



# Answer
df.select(F.to_csv(df.value).alias("csv")).show()

# COMMAND ----------

# Same, convert to json:
data = [(1, Row(name='Zlatan', age=85))]
df = spark.createDataFrame(data, ("key", "value"))
# Expected:
# +--------------------------+
# |json                      |
# +--------------------------+
# |{"name":"Zlatan","age":85}|
# +--------------------------+



# Answer
df.select(F.to_json(df.value).alias("json")).show(truncate=False)

# COMMAND ----------

# In Databricks Community edition, read /mnt/training/dataframes/people-with-dups.txt using delimiter :, header and infer the schema
# Then 
# - create a new column "firstName", as a lowercase version of "lcFirstName"
# - create a new column "ssn", as "ssnNums" where "-" is replaced by ""
# Select and show 3 rows of the columns mentioned
df = (spark.read
      .option("delimiter",":")   
      .option("header", "true")   
      .option("inferSchema", "true")            
      .csv("/mnt/training/dataframes/people-with-dups.txt"))
# Expected:
# +-----------+---------+---------+-----------+
# |lcFirstName|firstName|  ssnNums|        ssn|
# +-----------+---------+---------+-----------+
# |    emanuel|  Emanuel|935907627|935-90-7627|
# |     eloisa|   Eloisa|935899009|935-89-9009|
# |      cathi|    Cathi|959307957|959-30-7957|
# +-----------+---------+---------+-----------+



# Answer
df = (df.withColumn("lcFirstName", F.lower(F.col("firstName")))
      .withColumn("ssnNums",F.translate(F.col("ssn"),"-",""))
                  )
df.select("lcFirstName","firstName","ssnNums","ssn").show(3)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC #### Exercises on dataframe functions

# COMMAND ----------

# You have the following dataset, task is to sample by value 0 and 1, using 10% for first value, 20% for the second. Count the 2 groups
df = spark.range(0, 100).select((F.col("id") % 3).alias("key"))
# Expected:
# +---+-----+
# |key|count|
# +---+-----+
# |  0|    7|
# |  1|    6|
# +---+-----+



# Answer
sampled = df.sampleBy("key", fractions={0: 0.1, 1: 0.2}, seed=0)
sampled.groupBy("key").count().orderBy("key").show()

# COMMAND ----------

# In this df, replace in the column country any occurence of It by Italy
df = spark.createDataFrame([
                    ['Klinsmann',"Germany",8000],
                    ['Romario',"Brazil",10000],
                    ['Baggio','It',11000],
                    ['Ronaldo','Portugal',10000000]],('name','country','salary'))
# Expected:
# +---------+--------+--------+
# |     name| country|  salary|
# +---------+--------+--------+
# |Klinsmann| Germany|    8000|
# |  Romario|  Brazil|   10000|
# |   Baggio|   Italy|   11000|
# |  Ronaldo|Portugal|10000000|
# +---------+--------+--------+



# Answer
df.na.replace("It","Italy",subset=['country']).show()

# COMMAND ----------

# You have this df. Use these functions with transform on the dataframe, first one to cast to int, second one to sort the columns (i.e. by column name):
def cast_all_to_int(input_df):
    return input_df.select([F.col(col_name).cast("int") for col_name in input_df.columns])
def sort_columns_asc(input_df):
    return input_df.select(*sorted(input_df.columns))

df = spark.createDataFrame([(1, 1.0), (2, 2.0)], ["int", "float"])
# Expected:
# +-----+---+
# |float|int|
# +-----+---+
# |    1|  1|
# |    2|  2|
# +-----+---+



# Answer
df.transform(cast_all_to_int).transform(sort_columns_asc).show()

# COMMAND ----------

# You have the following dataframes. Return a new DataFrame containing rows in both this DataFrame and another DataFrame while preserving duplicates. Order the result and show
df1 = spark.createDataFrame([("Ronaldo", 1), ("Ramos", 2), ("Ramos", 2), ("Marcelo", 3)], ["player", "level"])
df2 = spark.createDataFrame([("Ronaldo", 1), ("Ramos", 2), ("Ramos", 2)], ["player", "level"])
# Expected:
# +-------+-----+
# | player|level|
# +-------+-----+
# |Ronaldo|    1|
# |  Ramos|    2|
# |  Ramos|    2|
# +-------+-----+



df1.intersectAll(df2).sort("player", "level").orderBy("level",asc=True).show()

# COMMAND ----------

# Same, without preserving duplicates
# Expected:
# +-------+-----+
# | player|level|
# +-------+-----+
# |Ronaldo|    1|
# |  Ramos|    2|
# +-------+-----+



# Answer
df1.intersect(df2).sort("player", "level").orderBy("level",asc=True).show()

# COMMAND ----------

# You have these dataframes. Union them, paying attention that the name order is not the same.
df1 = spark.createDataFrame([[1, 2, 3]], ["col0", "col1", "col2"])
df2 = spark.createDataFrame([[4, 5, 6]], ["col1", "col2", "col0"])
# Expected:
# +----+----+----+
# |col0|col1|col2|
# +----+----+----+
# |   1|   2|   3|
# |   6|   4|   5|
# +----+----+----+



# Answer
df1.unionByName(df2).show()

# COMMAND ----------

# Take a look at the following DataFrame, which has missing values.
df = spark.createDataFrame([
  (1, 4, 7),
  (5, 8, 3),
  (3, None, 77),
  (None, 6, 28)], 
  ["length", "width", "height"]
)
# Drop any records that have null values.
# Expected:
# +------+-----+------+
# |length|width|height|
# +------+-----+------+
# |     1|    4|     7|
# |     5|    8|     3|
# +------+-----+------+



# Answer
#df.dropna("any").show()
df.na.drop("any").show()

# COMMAND ----------

# Using previous dataframe, impute values with the mean
# Expected:
# +------+-----+------+
# |length|width|height|
# +------+-----+------+
# |     1|    4|     7|
# |     5|    8|     3|
# |     3|    6|    77|
# |     3|    6|    28|
# +------+-----+------+



# Answer
df.na.fill({"length": 3, "width": 6}).show()

# COMMAND ----------

# Take a look at the following DataFrame that has duplicate values.
df = spark.createDataFrame([
  (23, "Jordan", "67"),
  (23, "Jordan", "67"),
  (13, "Smith", "13"),
  (45, "Wilson", "17")], 
  ["id", "player", "points_scored"]
)
# Drop duplicates on id and points_scored
# Expected:
# +---+------+-------------+
# | id|player|points_scored|
# +---+------+-------------+
# | 13| Smith|           13|
# | 23|Jordan|           67|
# | 45|Wilson|           17|
# +---+------+-------------+



# Answer
df.dropDuplicates(["id", "points_scored"]).show()

# COMMAND ----------

# From previous dataframe, drop id and point_scored
# Expected:
# +------+-------------+
# |player|points_scored|
# +------+-------------+
# |Jordan|           67|
# |Jordan|           67|
# | Smith|           13|
# |Wilson|           17|
# +------+-------------+



# Answer
df.drop("id", "points_scored").show()

# COMMAND ----------

# # In Databricks Community edition, you have these 2 df:
df_a = spark.read.parquet("/mnt/training/countries/ISOCountryCodes/ISOCountryLookup.parquet")
df_b = spark.read.parquet("/mnt/training/EDGAR-Log-20170329/enhanced/logDFwithIP.parquet")

# Create a new DataFrame df_joined
# Get the full country name by performing a broadcast join that broadcasts the lookup table to the server log
# Drop all columns other than EnglishShortName
# Expected:
# +---------------+----------+--------+----+---------+--------------------+--------------------+-----+------+---+-------+-------+----+-------+-------+------------+--------------------+
# |             ip|      date|    time|zone|      cik|           accession|           extention| code|  size|idx|norefer|noagent|find|crawler|browser|IPLookupISO2|    EnglishShortName|
# +---------------+----------+--------+----+---------+--------------------+--------------------+-----+------+---+-------+-------+----+-------+-------+------------+--------------------+
# |  101.71.41.ihh|2017-03-29|00:00:00| 0.0|1437491.0|0001245105-17-000052|xslF345X03/primar...|301.0| 687.0|0.0|    0.0|    0.0|10.0|    0.0|   null|          CN|               China|
# |104.196.240.dda|2017-03-29|00:00:00| 0.0|1270985.0|0001188112-04-001037|                .txt|200.0|7619.0|0.0|    0.0|    0.0|10.0|    0.0|   null|          US|United States of ...|
# |  107.23.85.jfd|2017-03-29|00:00:00| 0.0|1059376.0|0000905148-07-006108|          -index.htm|200.0|2727.0|1.0|    0.0|    0.0|10.0|    0.0|   null|          US|United States of ...|
# +---------------+----------+--------+----+---------+--------------------+--------------------+-----+------+---+-------+-------+----+-------+-------+------------+--------------------+



# Answer:
df_joined = (df_b
  .join(F.broadcast(df_a), df_b.IPLookupISO2 == df_a.alpha2Code)
  .drop("alpha2Code", "alpha3Code", "numericCode", "ISO31662SubdivisionCode", "independentTerritory")
)
df_joined.show(3)

# COMMAND ----------

# Create a databricks table from a parquet file /mnt/training/ecommerce/events/events.parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS events USING parquet OPTIONS (path "/mnt/training/ecommerce/events/events.parquet");

# COMMAND ----------

# Load the table, show 3 rows and print the schema
# Expected:
# +-------+---------+----------+------------------------+----------------+-----------------+--------------------+--------------+--------------------------+-----------------+
# | device|ecommerce|event_name|event_previous_timestamp| event_timestamp|              geo|               items|traffic_source|user_first_touch_timestamp|          user_id|
# +-------+---------+----------+------------------------+----------------+-----------------+--------------------+--------------+--------------------------+-----------------+
# |  macOS|     [,,]|  warranty|        1593878899217692|1593878946592107|   [Montrose, MI]|                  []|        google|          1593878899217692|UA000000107379500|
# |Windows|     [,,]|     press|        1593876662175340|1593877011756535|[Northampton, MA]|                  []|        google|          1593876662175340|UA000000107359357|
# |  macOS|     [,,]|  add_item|        1593878792892652|1593878815459100|    [Salinas, CA]|[[, M_STAN_T, Sta...|       youtube|          1593878455472030|UA000000107375547|
# +-------+---------+----------+------------------------+----------------+-----------------+--------------------+--------------+--------------------------+-----------------+



# Answer
df = spark.table("events")
df.show(3)

df.printSchema()
#df.schema

# COMMAND ----------

# Filter for rows where device is macOS and then Sort rows by event_timestamp
# Expected:
# +------+---------+----------+------------------------+----------------+------------------+-----+--------------+--------------------------+-----------------+
# |device|ecommerce|event_name|event_previous_timestamp| event_timestamp|               geo|items|traffic_source|user_first_touch_timestamp|          user_id|
# +------+---------+----------+------------------------+----------------+------------------+-----+--------------+--------------------------+-----------------+
# | macOS|     [,,]|mattresses|                    null|1592539216262230|   [Waterbury, CT]|   []|        direct|          1592539216262230|UA000000103314644|
# | macOS|     [,,]|mattresses|        1592322041626307|1592539218667144|[Cedar Rapids, IA]|   []|         email|          1592321551374513|UA000000102690062|
# | macOS|     [,,]|   reviews|        1592538997090958|1592539218939615|      [Toledo, OH]|   []|        google|          1592538965425275|UA000000103314591|
# +------+---------+----------+------------------------+----------------+------------------+-----+--------------+--------------------------+-----------------+



# Answer
macDF = (df
  .where("device == 'macOS'")
  .sort("event_timestamp")
)
macDF.show(3)

# or equivalently:
macDF = (df
  .filter(F.col("device")=="macOS")
  .orderBy("event_timestamp")
)
macDF.show(3)

# COMMAND ----------

# Create the same DataFrame using SQL query
#- Use SparkSession to run a sql query on the `events` table
#- Use SQL commands above to write the same filter and sort query used earlier
# Expected:
# +------+---------+----------+------------------------+----------------+------------------+-----+--------------+--------------------------+-----------------+
# |device|ecommerce|event_name|event_previous_timestamp| event_timestamp|               geo|items|traffic_source|user_first_touch_timestamp|          user_id|
# +------+---------+----------+------------------------+----------------+------------------+-----+--------------+--------------------------+-----------------+
# | macOS|     [,,]|mattresses|                    null|1592539216262230|   [Waterbury, CT]|   []|        direct|          1592539216262230|UA000000103314644|
# | macOS|     [,,]|mattresses|        1592322041626307|1592539218667144|[Cedar Rapids, IA]|   []|         email|          1592321551374513|UA000000102690062|
# | macOS|     [,,]|   reviews|        1592538997090958|1592539218939615|      [Toledo, OH]|   []|        google|          1592538965425275|UA000000103314591|
# +------+---------+----------+------------------------+----------------+------------------+-----+--------------+--------------------------+-----------------+



# Expected:
macSQLDF = spark.sql("SELECT * FROM events WHERE device == 'macOS' ORDER BY event_timestamp")
macSQLDF.show(3)

# COMMAND ----------

# Count results and take first 5 rows



# Answer
macDF.count()
macDF.take(5)

# COMMAND ----------

# in next dataframe, drop duplicate rows
df = spark.createDataFrame([(1, 4, 3), (2, 8, 1), (2, 8, 1), (2, 8, 3), (3, 2, 1)], ["A", "B", "C"])  
# Expected:
# +---+---+---+
# |  A|  B|  C|
# +---+---+---+
# |  1|  4|  3|
# |  2|  8|  1|
# |  2|  8|  3|
# |  3|  2|  1|
# +---+---+---+



# Answer
df.dropDuplicates().show()

# COMMAND ----------

# then drop only the identical rows for columns A and B
# Expected:
# +---+---+---+
# |  A|  B|  C|
# +---+---+---+
# |  1|  4|  3|
# |  2|  8|  1|
# |  3|  2|  1|
# +---+---+---+



# Answer
df.dropDuplicates(['A','B']).show()

# COMMAND ----------

# drop columns A and B
# Expected:
# +---+
# |  C|
# +---+
# |  3|
# |  1|
# |  1|
# |  3|
# |  1|
# +---+



# Answer
df.drop('A','B').show()

# COMMAND ----------

# You have such dataframe. Create column C, indicating if the sum of A and B is higher than 7, as a boolean.
df = spark.createDataFrame([(1, 4), (2, 8), (2, 6)], ["A", "B"]) 
# Expected:
# +---+---+-----+
# |  A|  B| high|
# +---+---+-----+
# |  1|  4|false|
# |  2|  8| true|
# |  2|  6| true|
# +---+---+-----+



# Answer
df.selectExpr("A", "B", "A+B > 7 as high").show()

# COMMAND ----------

# Sort the dataframe on column B, descending order
# Expected:
# +---+---+
# |  A|  B|
# +---+---+
# |  2|  8|
# |  2|  6|
# |  1|  4|
# +---+---+



# Answer
df.sort(F.col("B").desc()).show()

# COMMAND ----------

# now sort the df along col A in desc, and then along col B in ascending order
# Expected:
# +---+---+
# |  A|  B|
# +---+---+
# |  2|  6|
# |  2|  8|
# |  1|  4|
# +---+---+



# Answer
df.sort(F.col("A").desc(), F.col("B").asc()).show()

# COMMAND ----------

# You have such dataframe. Sort along B column in desc order, keeping NULL at the end
df = spark.createDataFrame([(1, 4), (2, 8), (3, None), (2, 6)], ["A", "B"]) 
# Expected:
# +---+----+
# |  A|   B|
# +---+----+
# |  2|   8|
# |  2|   6|
# |  1|   4|
# |  3|null|
# +---+----+



# Answer
df.sort(F.col("B").desc_nulls_last()).show()

# COMMAND ----------

# filter the previous dataframe so that to keep column B without NULL
# Expected:
# +---+---+
# |  A|  B|
# +---+---+
# |  1|  4|
# |  2|  8|
# |  2|  6|
# +---+---+



# Answer
df.filter(F.col("B").isNotNull()).show()

# COMMAND ----------

# find the unique values in column A
# Expected:
# +---+
# |  A|
# +---+
# |  1|
# |  2|
# |  3|
# +---+



# Answer
df.select("A").distinct().show()

# COMMAND ----------

# Join these dataframes on A, exlude Null from col B
df1 = spark.createDataFrame([(1, 4), (2, 5), (2, 8), (2, 8), (2, 9), (3, 6), (3, 2), (3, None)], ["A", "B"])
df2 = spark.createDataFrame([(1, 3), (1, 5), (2, 4), (2, 3), (4, 1)], ["A", "C"])
# Expected:
# +---+---+----+
# |  A|  B|   C|
# +---+---+----+
# |  1|  4|   3|
# |  1|  4|   5|
# |  2|  5|   4|
# |  2|  5|   3|
# |  2|  8|   4|
# |  2|  8|   3|
# |  2|  8|   4|
# |  2|  8|   3|
# |  2|  9|   4|
# |  2|  9|   3|
# |  3|  6|null|
# |  3|  2|null|
# +---+---+----+



# Answer
df = (df1
      .join(df2, "A", "outer")
      .filter(F.col("B").isNotNull())                                 
)
df.show()

# COMMAND ----------

# you have 2 dataframes, df1 and df2. Find the common columns between them
# Expected:
# ['A']



# Answer
list(set(df1.columns).intersection(set(df2.columns)))

# COMMAND ----------

# you have following dataframe. filter by speed, should be stricly between 30 and 200. Modify column speed to get it 1.8 times its current value. Then sort everything by speed, highest at the top.
df = spark.createDataFrame([
                    ['Denton',95],
                    ['Jensen',93],
                    [None,28],
                    ['Manderley',53]],('name','speed'))
# Expected:
# +---------+-----+
# |     name|speed|
# +---------+-----+
# |   Denton|171.0|
# |   Jensen|167.4|
# |Manderley| 95.4|
# +---------+-----+



# Answer
df = df.filter(F.col("speed") > 30).filter(F.col("speed") < 200)
df = df.withColumn("speed",F.col("speed") * 1.8)
df = df.select("*").orderBy(F.col("speed").desc())
df.show()

# COMMAND ----------

