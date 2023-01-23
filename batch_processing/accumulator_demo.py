from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("accumulator_demo").