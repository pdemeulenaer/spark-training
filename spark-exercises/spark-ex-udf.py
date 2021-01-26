# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Classical User Defined Functions (UDF)

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# You have the python function
def firstLetterFunction(email):
  # ex: firstLetterFunction("annagray@kaufman.com") would give you "a"
  return email[0]

# You have the data df
df = spark.read.parquet("/mnt/training/ecommerce/sales/sales.parquet")

# Give back the original dataframe for columns order_id, email, and a new column "firstLetter" which is the UDF firstLetterUDF applied on "email" column.
# Expected:
# +--------+--------------------+-----------+
# |order_id|               email|firstLetter|
# +--------+--------------------+-----------+
# |  257437|kmunoz@powell-dur...|          k|
# |  282611|bmurillo@hotmail.com|          b|
# |  257448| bradley74@gmail.com|          b|
# +--------+--------------------+-----------+



# Answer
firstLetterUDF = F.udf(firstLetterFunction)
df.select("order_id", "email", firstLetterUDF(F.col("email")).alias("firstLetter")).show(3)

# COMMAND ----------

# Now register the UDF to create a UDF in the SQL namespace.



# Answer
df.createOrReplaceTempView("sales")
spark.udf.register("sql_udf", firstLetterFunction)

# COMMAND ----------

# Apply on a sales view (that you should create), derived from df dataframe. Produce the same output as above



# Answer

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, email, sql_udf(email) AS firstLetter FROM sales limit 3

# COMMAND ----------

# Now define the same email function than above, but using decorator. Call it decoratorUDF. Apply it on the dataframe df
# As previously, give back the original dataframe for columns order_id, email, and a new column "firstLetter" which is the UDF decoratorUDF applied on "email" column.
# Expected:
# +--------+--------------------+-----------+
# |order_id|               email|firstLetter|
# +--------+--------------------+-----------+
# |  257437|kmunoz@powell-dur...|          k|
# |  282611|bmurillo@hotmail.com|          b|
# |  257448| bradley74@gmail.com|          b|
# +--------+--------------------+-----------+



# Answer
# Our input/output is a string
@F.udf("string")
def decoratorUDF(email: str) -> str:
  return email[0]

df.select("order_id", "email", decoratorUDF(F.col("email")).alias("firstLetter")).show(3)

# COMMAND ----------

# Use Vectorized UDF to help speed up the computation using Apache Arrow.
import pandas as pd

# We have a string input/output
@F.pandas_udf("string")
def vectorizedUDF(email: pd.Series) -> pd.Series:
  return email.str[0]

# Expected:
# +--------+--------------------+-----------+
# |order_id|               email|firstLetter|
# +--------+--------------------+-----------+
# |  257437|kmunoz@powell-dur...|          k|
# |  282611|bmurillo@hotmail.com|          b|
# |  257448| bradley74@gmail.com|          b|
# +--------+--------------------+-----------+



# Answer
df.select("order_id", "email", vectorizedUDF(F.col("email")).alias("firstLetter")).show(3)

# COMMAND ----------

# You can do the same as above, using a pandas UDF, but using a one-liner using lambda function. Define it and apply it
# Expected: same as above


# Answer
vectorizedUDF2 = F.pandas_udf(lambda s: s.str[0], "string")  
df.select("order_id", "email", vectorizedUDF2(F.col("email")).alias("firstLetter")).show(3)

# COMMAND ----------

# Also register the Vectorized UDF vectorizedUDF to the SQL namespace.



# Answer
spark.udf.register("sql_vectorized_udf", vectorizedUDF)

# COMMAND ----------

# You have such a dataframe. Create a python function update_salary that modifies the salary so that new salary = 1.05*old salary.
# use this function as a udf, select the name and that function on salary (result called "new_salary"). Also register the udf so that it could also be used in SQL

df = spark.createDataFrame([
                    ['Will',"HR",3000],
                    ['Smith','Admin',2000]],('name','dept','salary'))
# Expected
# +-----+----------+
# | name|new_salary|
# +-----+----------+
# | Will|    3150.0|
# |Smith|    2100.0|
# +-----+----------+



# Answer
def update_salary(x):
  return x * 1.05

spark.udf.register("updateSalary", update_salary) # only for SQL functions !!!

update_salary_udf = udf(update_salary) # only for dataframe operations !!!

df.select(F.col("name"),update_salary_udf(F.col("salary")).alias("new_salary")).show()

# COMMAND ----------

# Define a python udf using as a lambda function, that outputs the length of a string. Register the udf, and get the name (strlen) of the registered udf as a spark function.

# - apply the SQL udf function onto the value "test" using spark.sql(). Extract the length as textLength
# - apply the non-SQL udf function strlen on the dataframe: spark.sql("SELECT 'foo' AS text"). Extract the length as textLength

# Expected:
# +----------+
# |textLength|
# +----------+
# |         4|
# +----------+

# +----------+
# |textLength|
# +----------+
# |         3|
# +----------+



# Answer
strlen = spark.udf.register("stringLengthString", lambda x: len(x))

spark.sql("SELECT stringLengthString('test') AS textLength").show()
spark.sql("SELECT 'foo' AS text").select(strlen("text").alias("textLength")).show()

# COMMAND ----------

# Create a udf random_udf as a lambda function, returning a random integer random.randint(0, 100). label the udf as Non deterministic(). Register the udf, apply in spark.sql(). 
# Expected like:
# +------------+
# |random_udf()|
# +------------+
# |          28|
# +------------+



# Answer
import random
from pyspark.sql.types import IntegerType
random_udf = udf(lambda: random.randint(0, 100), IntegerType()).asNondeterministic()
new_random_udf = spark.udf.register("random_udf", random_udf)
spark.sql("SELECT random_udf()").show()  

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Pandas UDFs

# COMMAND ----------

# Create the pandas_udf add_one which gets s (as a pd.Series) and return s+1 (as a pd.Series) as integer. 
# Register it and use it as spark.sql(SELECT add_one(id) FROM range(3))
# Expected
# +-----------+
# |add_one(id)|
# +-----------+
# |          1|
# |          2|
# |          3|
# +-----------+



# Answer
import pandas as pd  
@F.pandas_udf("integer")  
def add_one(s: pd.Series) -> pd.Series:
    return s + 1

_ = spark.udf.register("add_one", add_one)  
spark.sql("SELECT add_one(id) FROM range(3)").show()  

# COMMAND ----------

# You have such a dataframe:
q = "SELECT * FROM VALUES (3, 0), (2, 0), (1, 1) tbl(v1, v2)"
spark.sql(q).show()
# Create a pandas_udf that sums a series and returns an integer. Register the udf. Then apply that on a "group by" v2 version of the table above, within spark.sql().
# Expected:
# +------+
# |sum_v1|
# +------+
# |     5|
# |     1|
# +------+



# Answer
@F.pandas_udf("integer")  
def sum_udf(v: pd.Series) -> int:
    return v.sum()

_ = spark.udf.register("sum_udf", sum_udf)  
q = "SELECT sum_udf(v1) AS sum_v1 FROM VALUES (3, 0), (2, 0), (1, 1) tbl(v1, v2) GROUP BY v2"
spark.sql(q).show() 

# COMMAND ----------

# You have this df. Build a pandas_udf "normalize" function that takes as input a pandas pdf and assigns a column v = (v-v.mean()) / v.std()
df = spark.createDataFrame(
    [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
    ("id", "v"))  
# apply the function on the different id groups of df, using "apply". Here you will need to declare the schema in the pandas_udf decorator of the python function
# Expected
# +---+-------------------+
# | id|                  v|
# +---+-------------------+
# |  1|-0.7071067811865475|
# |  1| 0.7071067811865475|
# |  2|-0.8320502943378437|
# |  2|-0.2773500981126146|
# |  2| 1.1094003924504583|
# +---+-------------------+



# Answer
import pandas as pd  

@F.pandas_udf("id long, v double", F.PandasUDFType.GROUPED_MAP)  
def normalize(pdf):
    v = pdf.v
    return pdf.assign(v=(v - v.mean()) / v.std())

df.groupby("id").apply(normalize).show()  

# COMMAND ----------

# As previous exercise, but now you have only the python function (not pandas_udf):

def normalize(pdf):
    v = pdf.v
    return pdf.assign(v=(v - v.mean()) / v.std())

# Use the function with applyInPandas on the grouped dataframe (by id), defining the schema.
# Expected
# +---+-------------------+
# | id|                  v|
# +---+-------------------+
# |  1|-0.7071067811865475|
# |  1| 0.7071067811865475|
# |  2|-0.8320502943378437|
# |  2|-0.2773500981126146|
# |  2| 1.1094003924504583|
# +---+-------------------+

  
  
# Answer
df.groupby("id").applyInPandas(normalize, schema="id long, v double").show()

# COMMAND ----------

# You have the following dataframe df and function filter_func. Apply the function on the dataframe
df = spark.createDataFrame([(1, 21), (2, 30)], ("id", "age"))

def filter_func(iterator):
    for pdf in iterator:
        yield pdf[pdf.id == 1]        
# Expected:
# +---+---+
# | id|age|
# +---+---+
# |  1| 21|
# +---+---+
        
        

# Answer    
df.mapInPandas(filter_func, df.schema).show()  

# COMMAND ----------

# You have this dataframe and this pandas_udf that builds the mean. Apply this mean function to group of id

df = spark.createDataFrame(
    [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)], ("id", "v"))

@F.pandas_udf("double")
def mean_udf(v: pd.Series) -> float:
    return v.mean()
  
# Expected:
# +---+--------------+
# | id|mean_of_groups|
# +---+--------------+
# |  1|           1.5|
# |  2|           6.0|
# +---+--------------+



# Answer
df.groupby("id").agg(mean_udf(df['v']).alias("mean_of_groups")).show()

# COMMAND ----------

# Now use this function on a window function on groups of id, ordered by v, between previous and current row. Called the new column mean_v and show the df
df = spark.createDataFrame(
    [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)], ("id", "v"))

# Expected:
# +---+----+------+
# | id|   v|mean_v|
# +---+----+------+
# |  1| 1.0|   1.0|
# |  1| 2.0|   1.5|
# |  2| 3.0|   3.0|
# |  2| 5.0|   4.0|
# |  2|10.0|   7.5|
# +---+----+------+



# Answer
from pyspark.sql import Window
w = Window.partitionBy('id').orderBy('v').rowsBetween(-1, 0)
df.withColumn('mean_v', mean_udf("v").over(w)).show()

# COMMAND ----------

# Build the pandas_udf taking a string and outputing that string in capital letters. Apply on this dataframe:
df = spark.createDataFrame([("John Doe",)], ("name",))
# Expected:
# +--------+
# |    name|
# +--------+
# |JOHN DOE|
# +--------+



# Answer
@F.pandas_udf("string")
def to_upper(s: pd.Series) -> pd.Series:
    return s.str.upper()

df.select(to_upper("name").alias("name")).show()

# COMMAND ----------

# Create a pandas_udf taking a string as input and returning the length of it (as integer). Do this twice:
# - first using the pre-spark 3 way of declaring pandas_udf
# - second using the way since spark 3
# Apply on this df:
df = spark.createDataFrame([("John Doe",)], ("name",))
# Expected:
# +----+
# |name|
# +----+
# |   8|
# +----+



# Answer
import pandas as pd
from pyspark.sql.types import IntegerType
# pre-spark 3
@F.pandas_udf(IntegerType(), F.PandasUDFType.SCALAR)
def slen(s):
    return s.str.len()
  
df.select(slen("name").alias("name")).show()  


# starting from spark 3
@F.pandas_udf(IntegerType())
def slen(s: pd.Series) -> pd.Series:
    return s.str.len()

df.select(slen("name").alias("name")).show()

# COMMAND ----------

# You have this dataframe, and you are asked to round the arrays in the first column to 2 decimals
df = spark.createDataFrame([([1.4343,2.3434,3.4545],'val1'),([4.5656,5.1215,6.5656],'val2')],['col1','col2'])
# For this, use this function:
# def round_func(v):
#     return v.apply(lambda x:np.around(x,decimals=2))
# Declare the pandas_udf decorator in pre-spark 3, as well as a second time using spark 3 convention

# Expected:
# +--------------------+----+------------------+
# |                col1|col2|              col3|
# +--------------------+----+------------------+
# |[1.4343, 2.3434, ...|val1|[1.43, 2.34, 3.45]|
# |[4.5656, 5.1215, ...|val2|[4.57, 5.12, 6.57]|
# +--------------------+----+------------------+



# Answer
import numpy as np
import pandas as pd
from pyspark.sql.types import *

# pre-spark 3
@F.pandas_udf(ArrayType(FloatType()),F.PandasUDFType.SCALAR)
def round_func(v):
    return v.apply(lambda x:np.around(x,decimals=2))

df.withColumn('col3',round_func(df.col1)).show()

# starting from spark 3
@F.pandas_udf(ArrayType(FloatType()))
def round_func(v: pd.Series) -> pd.Series:
    return v.apply(lambda x:np.around(x,decimals=2))

df.withColumn('col3',round_func(df.col1)).show()