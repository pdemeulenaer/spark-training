# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC ### Dates

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# For the following dataframe, select "dt" as well as a new column "next_date", that adds one day to the date:
df = spark.createDataFrame([('2015-04-08',)], ['dt'])
# Expected:
# +----------+----------+
# |        dt| next_date|
# +----------+----------+
# |2015-04-08|2015-04-09|
# +----------+----------+


# Answer
df.select("dt", F.date_add(df.dt, 1).alias('next_date')).show()

# COMMAND ----------

# For the following dataframe, convert format to MM/dd/yyyy and alias it as "date"
df = spark.createDataFrame([('2015-04-08',)], ['dt'])
# Expected
# +----------+
# |      date|
# +----------+
# |04/08/2015|
# +----------+

# Answer
df.select(F.date_format('dt', 'MM/dd/yyyy').alias('date')).show()

# COMMAND ----------

# you have the following dataframe. Truncate to get the first day of the year (alias "year"), as well as a second column which gets the first day of the month (alias "month"):
df = spark.createDataFrame([('1997-02-28 05:02:11',)], ['t'])
# Expected:
# +-------------------+-------------------+
# |               year|              month|
# +-------------------+-------------------+
# |1997-01-01 00:00:00|1997-02-01 00:00:00|
# +-------------------+-------------------+



# Answer
df.select(F.date_trunc('year', df.t).alias('year'), F.date_trunc('mon', df.t).alias('month')).show()

# COMMAND ----------

# You have this Pandas dataframe with the column "time" containing the dates between start_date and end_date. 
# 1. Convert to Pyspark
# 2. Create a colunm "transactiondate" by casting the column "time" to date

import pandas as pd
time = pd.date_range('2018-01-01', '2018-01-03', freq='D')
dfp = pd.DataFrame(columns=['time'])
dfp['time'] = time
# Expected:
# +-------------------+---------------+
# |               time|transactiondate|
# +-------------------+---------------+
# |2018-01-01 00:00:00|     2018-01-01|
# |2018-01-02 00:00:00|     2018-01-02|
# |2018-01-03 00:00:00|     2018-01-03|
# +-------------------+---------------+



# Answer
df = spark.createDataFrame(dfp)
df = df.withColumn('transactiondate', F.to_date(df.time))
df.show()

# COMMAND ----------

# Using spark.sql and sequence function, create a dataframe containing the column date, made of an array of dates: the dates are the first day of each month between 2018-01 to 2018-03. Show without truncating.
# Expected:
# +------------------------------------+
# |date                                |
# +------------------------------------+
# |[2018-01-01, 2018-02-01, 2018-03-01]|
# +------------------------------------+



# Answer
df = spark.sql("SELECT sequence(to_date('2018-01-01'), to_date('2018-03-01'), interval 1 month) as date")
df.show(truncate=False)

# COMMAND ----------

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

# Using this dataframe, convert the column to date. Use 3 methods: selectExpr+SQL CAST, select+cast, to_date

df = spark.createDataFrame([
  "2017-01-01", "2018-02-08", "2019-01-03"], "string"
)



# Answer
df.selectExpr("CAST(value AS date) AS date").show()

df.select(F.col("value").cast("date").alias("date")).show()

df.select(F.to_date("value").alias("date")).show()

# COMMAND ----------

# Extract the year and the hour of the following column ts

df = spark.createDataFrame([('2015-04-08 13:08:15',)], ['ts'])
# Expected:
# +----+----+
# |year|hour|
# +----+----+
# |2015|  13|
# +----+----+


# Answer
df.select(F.year('ts').alias('year'), F.hour('ts').alias('hour')).show()

# COMMAND ----------

# Get the last day of the month in column d, as "date"

df = spark.createDataFrame([('1997-02-10',)], ['d'])
# Expected:
# +----------+
# |      date|
# +----------+
# |1997-02-28|
# +----------+



# Answer
df.select(F.last_day("d").alias('date')).show()

# COMMAND ----------

# Get the number of different months (alias month) between the 2 following date columns. Round the column "month". Show all 3 columns
df = spark.createDataFrame([('1997-02-28 10:30:00', '1996-10-30')], ['date1', 'date2'])
# Expected:
# +-------------------+----------+------+
# |              date1|     date2|months|
# +-------------------+----------+------+
# |1997-02-28 10:30:00|1996-10-30|   4.0|
# +-------------------+----------+------+



# Answer
df.select('date1', 'date2', F.round(F.months_between('date1', 'date2'),0).alias('months')).show()

# COMMAND ----------

# Using spark.sql and function make_date, from this df, build the associated date
spark.createDataFrame([(2020, 6, 26), (1000, 2, 29), (-44, 1, 1)], ['Y', 'M', 'D']).createOrReplaceTempView('YMD')
# Expected:
# +-----------+
# |       date|
# +-----------+
# | 2020-06-26|
# |       null|
# |-0044-01-01|
# +-----------+



# Answer
df = spark.sql('select make_date(Y, M, D) as date from YMD')
df.show()

# COMMAND ----------

# Using selectExpr and make_timestamp function, from this df, build the associated timestamp 

df = spark.createDataFrame([(2020, 6, 28, 10, 31, 30.123456),
                            (1582, 10, 10, 0, 1, 2.0001), (2019, 2, 29, 9, 29, 1.0)],
                           ['YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND'])
# Expected:
# +--------------------------+
# |THE_TIMESTAMP             |
# +--------------------------+
# |2020-06-28 10:31:30.123456|
# |1582-10-10 00:01:02.0001  |
# |null                      |
# +--------------------------+



# Answer
ts = df.selectExpr("make_timestamp(YEAR, MONTH, DAY, HOUR, MINUTE, SECOND) as THE_TIMESTAMP")
ts.show(truncate=False)

# COMMAND ----------

# Add 1 month to the next dt column
df = spark.createDataFrame([('2015-04-08',)], ['dt'])
# Expected:
# +----------+
# |next_month|
# +----------+
# |2015-05-08|
# +----------+



# Answer
df.select(F.add_months(df.dt, 1).alias('next_month')).show()

# COMMAND ----------

# Get the current date and current timestamp:
# Expected like this:
# +--------------+-----------------------+
# |current_date()|current_timestamp()    |
# +--------------+-----------------------+
# |2021-01-24    |2021-01-24 22:27:35.045|
# +--------------+-----------------------+


# Answer
df.select(F.current_date(), F.current_timestamp()).show(truncate=False)

# COMMAND ----------

# Using following dataframe, cast the column as date, alias it as "date", and find the minimum and maximum values. Print them
df = (spark.createDataFrame([
  "2017-01-01", "2018-02-08", "2019-01-03"], "string")
)
# Expected
# (datetime.date(2017, 1, 1), datetime.date(2019, 1, 3)) 



# Answer
df = df.selectExpr("CAST(value AS date) AS date")
min_date, max_date = df.select(F.min("date"), F.max("date")).first()
min_date, max_date  

# COMMAND ----------

# You have such a dataframe in input
df = spark.createDataFrame([('UA000000107379500', 1593878946592107), 
                            ('UA000000107359357', 1593877011756535), 
                            ('UA000000107375547', 1593878815459100)], ["id", "ts"])
# Your task is to cast column to a different data type, specified using string representation or DataType.
# Convert timestamp from microseconds to seconds by dividing by 1 million and cast to timestamp
# Expected:
# +-----------------+--------------------------+
# |id               |ts                        |
# +-----------------+--------------------------+
# |UA000000107379500|2020-07-04 16:09:06.592107|
# |UA000000107359357|2020-07-04 15:36:51.756535|
# |UA000000107375547|2020-07-04 16:06:55.4591  |
# +-----------------+--------------------------+



# Answer
from pyspark.sql.types import TimestampType
df = df.withColumn("ts", (F.col("ts") / 1e6).cast(TimestampType()))
df.show(truncate=False)

# COMMAND ----------

# Using previous dataframe, format the time and date: convert ts to 
# - a column "date string" with format "MMMM dd, yyyy"
# - a column "time string" with format "HH:mm:ss.SSSSSS"
# - a column "date_new" with format "dd-mm-yyyy"
# Expected:
# +-----------------+--------------------------+-------------+---------------+----------+
# |id               |ts                        |date string  |time string    |date_new  |
# +-----------------+--------------------------+-------------+---------------+----------+
# |UA000000107379500|2020-07-04 16:09:06.592107|July 04, 2020|16:09:06.592107|04-09-2020|
# |UA000000107359357|2020-07-04 15:36:51.756535|July 04, 2020|15:36:51.756535|04-36-2020|
# |UA000000107375547|2020-07-04 16:06:55.4591  |July 04, 2020|16:06:55.459100|04-06-2020|
# +-----------------+--------------------------+-------------+---------------+----------+



# Answer
df = (df.withColumn("date string", F.date_format("ts", "MMMM dd, yyyy"))
      .withColumn("time string", F.date_format("ts", "HH:mm:ss.SSSSSS"))
      .withColumn("date_new", F.date_format("ts", "dd-mm-yyyy"))             
) 
df.show(truncate=False)

# COMMAND ----------

# Using previous dataframe, extracts the year as an integer from a given date/timestamp/string.
# Similar methods: month, dayofweek, minute, second
# Expected:
# +-----------------+--------------------+-------------+---------------+----------+----+-----+---------+------+------+
# |               id|                  ts|  date string|    time string|  date_new|year|month|dayofweek|minute|second|
# +-----------------+--------------------+-------------+---------------+----------+----+-----+---------+------+------+
# |UA000000107379500|2020-07-04 16:09:...|July 04, 2020|16:09:06.592107|04-09-2020|2020|    7|        7|     9|     6|
# |UA000000107359357|2020-07-04 15:36:...|July 04, 2020|15:36:51.756535|04-36-2020|2020|    7|        7|    36|    51|
# |UA000000107375547|2020-07-04 16:06:...|July 04, 2020|16:06:55.459100|04-06-2020|2020|    7|        7|     6|    55|
# +-----------------+--------------------+-------------+---------------+----------+----+-----+---------+------+------+



# Answer
df = (df.withColumn("year", F.year(F.col("ts")))
      .withColumn("month", F.month(F.col("ts")))
      .withColumn("dayofweek", F.dayofweek(F.col("ts")))
      .withColumn("minute", F.minute(F.col("ts")))
      .withColumn("second", F.second(F.col("ts")))              
)
df.show()

# COMMAND ----------

# Converts the column into DateType with name "date" by casting rules to DateType (use function to_date).
# Then create a column plus_two_days that adds 2 days to the date. Select "date" and "plus_two_days"
# Expected:
# +----------+-------------+
# |      date|plus_two_days|
# +----------+-------------+
# |2020-07-04|   2020-07-06|
# |2020-07-04|   2020-07-06|
# |2020-07-04|   2020-07-06|
# +----------+-------------+

# Answer
df = df.withColumn("date", F.to_date(F.col("ts")))
df = df.withColumn("plus_two_days", F.date_add(F.col("ts"), 2))
df.select("date", "plus_two_days").show()

# COMMAND ----------

# Get the day of week, "day", and the day of week name, "day_name". Select these columns
# Expected:
# +---+--------+
# |day|day_name|
# +---+--------+
# |  7|     Sat|
# |  7|     Sat|
# |  7|     Sat|
# +---+--------+



# Answer
df = df.withColumn("day", F.dayofweek(F.col("date"))).withColumn("day_name", F.date_format(F.col("date"), "E"))
df.select("day", "day_name").show()

# COMMAND ----------

# In this dataframe, convert the unix_time column (which is the number of seconds from unix epoch, 1970-01-01 00:00:00 UTC) to a string representing the timestamp of that moment in the current system time zone in the given format. After that operation, unset the spark.sql.session.timeZone. 

spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
time_df = spark.createDataFrame([(1428476400,)], ['unix_time'])
# Expected:
# +-------------------+
# |                 ts|
# +-------------------+
# |2015-04-08 00:00:00|
# +-------------------+



# Answer
time_df.select(F.from_unixtime('unix_time').alias('ts')).show()
spark.conf.unset("spark.sql.session.timeZone")

# COMMAND ----------

# You have this dataframe, with a column for the datetime (given in UTC time), and the second for the time zone. Convert this datetime in local time, once for the PST (Pacific Standard Time) and in the second case use the time zone column tz

df = spark.createDataFrame([('1997-02-28 10:30:00', 'JST')], ['ts', 'tz'])
# Expected:
# +-------------------+-------------------+
# |           PST_time|         local_time|
# +-------------------+-------------------+
# |1997-02-28 02:30:00|1997-02-28 19:30:00|
# +-------------------+-------------------+



# Answer
df.select(F.from_utc_timestamp(df.ts, "PST").alias('PST_time'), F.from_utc_timestamp(df.ts, df.tz).alias('local_time')).show()

# COMMAND ----------

