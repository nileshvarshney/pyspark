# Broadcast variables in Apache Spark is a mechanism for sharing variables 
# across executors that are meant to be read-only. Without broadcast variables 
# these variables would be shipped to each executor for every transformation 
# and action, and this can cause network overhead.

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, broadcast, current_timestamp, year

def main():
    sparkSession = SparkSession\
        .builder\
        .master('local')\
        .appName('UDF and Broadcast')\
        .getOrCreate()

    sparkSession.sparkContext.setLogLevel('ERROR')

    # define schema
    # Date,Open,High,Low,Close,Adj Close,Volume,Name
    stockSchema = StructType([
        StructField('Date', DateType(), True),
        StructField('Open', DoubleType(), True),
        StructField('High', DoubleType(), True),
        StructField('Low', DoubleType(), True),
        StructField('Close', DoubleType(), True),
        StructField('Adj Close', DoubleType(), True),
        StructField('Volume', LongType(), True),
        StructField('Name', StringType(), True)
    ])

    stocks = (
        sparkSession
        .readStream
        .option('header','true')
        .option("maxFilesPerTrigger", 1) # read one file at a time
        .schema(stockSchema)
        .csv('streaming/stock_price/data/stock_data/*.csv')
    )

    print('Is streaming', stocks.isStreaming)
    # print(stocks.printSchema())

    # User Defined Function
    def daily_price_delta(open_price, close_price):
        return close_price - open_price

    # Registering UDF
    sparkSession.udf.register('calculated_price_delta_udf',daily_price_delta, DoubleType())

    # add ingestion time and calculate year
    stocks = stocks.withColumn("ingetion_time", current_timestamp()).withColumn("year", year("Date"))
    
    stocks.createOrReplaceTempView('stock')

    # Broadcasting
    price_delta_broadcast_df = broadcast(sparkSession.sql(
        """ Select year, ingetion_time, Date, Name, calculated_price_delta_udf(Open, Close) delta_price
            from stock where calculated_price_delta_udf(Open, Close) > 15
        """
    ))

    price_delta_df = price_delta_broadcast_df.select("year", "ingetion_time", "Date", "Name", "delta_price")


    query = price_delta_df\
        .writeStream\
        .outputMode("append")\
        .format("console")\
        .option("truncate",'false')\
        .option('numRows', 30)\
        .start().awaitTermination()

if __name__ == "__main__":
    main()

