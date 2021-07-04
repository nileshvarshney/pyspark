from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression

spark = SparkSession.builder.appName('Linear Regression').getOrCreate()
ecomm_cust = spark.read.csv('./Ecommerce_Customers.csv',inferSchema=True, header=True)
print(ecomm_cust.printSchema())

