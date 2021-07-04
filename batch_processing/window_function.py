from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('Window Function').getOrCreate()

data =   [("sales", 1, 5000),
  ("personnel", 2, 3900),
  ("sales", 3, 4800),
  ("sales", 4, 4800),
  ("personnel", 5, 3500),
  ("develop", 7, 4200),
  ("develop", 8, 6000),
  ("develop", 9, 4500),
  ("develop", 10, 5200),
  ("develop", 11, 5200)]

columns = ['dept', 'empno', 'salary']
emp_df = spark.createDataFrame(data= data, schema=columns)

print(emp_df.show())

window_spec = Window.partitionBy("dept").orderBy("salary")
cum_salary_by_dept = emp_df\
  .withColumn("max_salary", sum("salary").over(window_spec)) 
print(max_salary_by_dept.show())

emp_rank_by_dept = emp_df\
  .withColumn("rank", rank().over(Window.partitionBy("dept").orderBy("salary")))\
  .withColumn("dense_rank", dense_rank().over(Window.partitionBy("dept").orderBy("salary")))\
  .withColumn("row_number", row_number().over(Window.partitionBy("dept").orderBy("salary"))) \
  .withColumn("max_salary", max("salary").over(Window.partitionBy("dept")))\
  .withColumn("min_salary", min("salary").over(Window.partitionBy("dept")))
print(emp_rank_by_dept.show())

print(emp_rank_by_dept.select("dept","min_salary","max_salary").dropDuplicates().show())

print(\
      emp_df\
      .withColumn("lag", lag("salary", offset= 1).over(Window.partitionBy("dept").orderBy("salary")))\
      .withColumn("lead", lead("salary", offset= 2).over(Window.partitionBy("dept").orderBy("salary")))\
      .show(5, False))

print(\
      emp_df\
      .withColumn("sum", sum("salary")\
        .over(Window.partitionBy("dept").orderBy("salary")\
        .rangeBetween(-10,10)))\
      .show())

