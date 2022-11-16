from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import min, max, dense_rank, desc, col, row_number

spark = SparkSession.builder.appName('Window Function').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

data = [
    ("Ram", 28, "Sales", 3000),
    ("Meena", 33, "Sales", 4600),
    ("Robin", 40, "Sales", 4100),
    ("Kunal", 25, "Finance", 3000),
    ("Ramesh", 28, "Sales", 3000),
    ("Srishti", 46, "Management", 3300),
    ("Jeny", 26, "Finance", 3900),
    ("Hitesh", 30, "Marketing", 3000),
    ("Kailash", 29, "Marketing", 2000),
    ("Sharad", 39, "Sales", 4100)

]

# create employee dataframe
columns = ['name', 'age', 'dept', 'salary']
emp_df = spark.createDataFrame(data=data, schema=columns)

print(emp_df.show())
print(emp_df.printSchema())

window_spec = Window.partitionBy("dept").orderBy("salary")
emp_df_salary = emp_df\
    .withColumn(
        "min_salary_by_dept",
        min("salary").over(Window.partitionBy("dept")))\
    .withColumn(
        "max_salary_by_dept",
        max("salary").over(Window.partitionBy("dept")))\
    .withColumn(
        "rank_by_salary",
        dense_rank().over(
            Window.partitionBy("dept").orderBy(col("salary").desc())))\
    .withColumn(
        "row_number",
        row_number().over(
            Window.partitionBy("dept").orderBy(col("salary").desc())))\
    .orderBy(col("row_number"))

print(emp_df_salary.show())

# emp_rank_by_dept = emp_df\
#     .withColumn("rank", rank().over(Window.partitionBy("dept").orderBy("salary")))\
#     .withColumn("dense_rank", dense_rank().over(Window.partitionBy("dept").orderBy("salary")))\
#     .withColumn("row_number", row_number().over(Window.partitionBy("dept").orderBy("salary"))) \
#     .withColumn("max_salary", max("salary").over(Window.partitionBy("dept")))\
#     .withColumn("min_salary", min("salary").over(Window.partitionBy("dept")))
# print(emp_rank_by_dept.show())

# print(emp_rank_by_dept.select("dept", "min_salary",
#       "max_salary").dropDuplicates().show())

# print(
#     emp_df
#     .withColumn("lag", lag("salary", offset=1).over(Window.partitionBy("dept").orderBy("salary")))
#     .withColumn("lead", lead("salary", offset=2).over(Window.partitionBy("dept").orderBy("salary")))
#     .show(5, False))

# print(
#     emp_df
#     .withColumn("sum", sum("salary")
#                 .over(Window.partitionBy("dept").orderBy("salary")
#                       .rangeBetween(-10, 10)))
#     .show())
