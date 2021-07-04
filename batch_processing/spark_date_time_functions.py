from pyspark.sql import  SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('Date time function demo').getOrCreate()

emp = [(1, "AAA", "dept1", 1000, "2019-02-01 15:12:13"),
    (2, "BBB", "dept1", 1100, "2018-04-01 5:12:3"),
    (3, "CCC", "dept1", 3000, "2017-06-05 1:2:13"),
    (4, "DDD", "dept1", 1500, "2019-08-10 10:52:53"),
    (5, "EEE", "dept2", 8000, "2016-01-11 5:52:43"),
    (6, "FFF", "dept2", 7200, "2015-04-14 19:32:33"),
    (7, "GGG", "dept3", 7100, "2019-02-21 15:42:43"),
    (8, "HHH", "dept3", 3700, "2016-09-25 15:32:33"),
    (9, "III", "dept3", 4500, "2017-10-15 15:22:23"),
    (10, "JJJ", "dept5", 3400, "2018-12-17 15:14:17")]

emp_df = spark.createDataFrame(emp,['id','name','dept','salary', 'date'])

emp_df.printSchema()

emp_df.show()

df = emp_df\
  .withColumn("current_date", current_date())\
  .select("id", "current_date")

df.show(3, truncate=False)

df = emp_df.select("date","name","salary").withColumn("next_month", add_months("date", 1)).select("name", "next_month", "salary")
df.show(3, truncate=False)

df = emp_df.select("name", "salary", "dept", "date")\
  .withColumn("current_timestamp", current_timestamp())\
  .withColumn('post_15_day', date_add("date", 15))\
  .withColumn('pre_15_day', date_sub("date",15))\
  .withColumn('day_difference', datediff("post_15_day","pre_15_day"))\
  .withColumn('day_of_month', dayofmonth("date"))\
  .withColumn('day_of_week', dayofweek("date"))\
  .withColumn('day_of_year', dayofyear("date"))\
  .withColumn('new_format', date_format("date","yyyy/mm/dd"))
  
df.show(3, truncate=False)

df = emp_df.select("name", "date")\
  .withColumn('Indian_timestamp', from_utc_timestamp(current_timestamp(),'IST'))\
  .withColumn('hour', hour("date"))\
  .withColumn('minute', minute("date"))\
  .withColumn('second', second("date")) \
  .withColumn('month', month("date"))\
  .withColumn('year', year("date")) \
  .withColumn('next_day', next_day("date","sun"))\
  .withColumn('last_day', last_day("date")) \
  .withColumn('quarter', quarter("date"))\
  .withColumn('to_date', to_date("date"))\
  .withColumn('to_timestamp', to_timestamp("date"))
df.show(3, truncate=False)

df.printSchema()

