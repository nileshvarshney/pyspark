from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os

schema =  StructType([
        StructField('Country', StringType(), False),
        StructField('Year', IntegerType(), False),
        StructField('Status', StringType(), False),
        StructField('LifeExpectancy', FloatType(), False)
    ]) 


data = 'streaming/other_streaming/datasets/lifeExpectancyDataset'
data_location = os.path.join(os.getcwd(), data)

spark = SparkSession.builder.appName('direct_stream').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

spark\
    .readStream\
    .option("header","true")\
    .schema(schema)\
    .option("maxFilesPerTrigger", 1)\
    .csv(data_location).createOrReplaceTempView("lifeExpectancy")


# df.createOrReplaceTempView("lifeExpectancy")
out_df = spark.sql("""
    select 
        Country, round(avg(LifeExpectancy),2) LifeExpectancy 
    from lifeExpectancy 
    group by Country 
    order by LifeExpectancy desc limit 5"""
    )

query = out_df.writeStream.outputMode("complete")\
.format("console").start()

query.awaitTermination()

