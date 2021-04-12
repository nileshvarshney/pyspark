# Window : Aggregations over a sliding event-time window 
# window() operations to express windowed aggregations.
# e.g. groupBy(
#    window(words.timestamp, "10 minutes", "5 minutes"))
# Task:
# Calculate 7 days avg closing price with 3 days overlapped window

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, window
from pyspark.sql.types import *
import sys


DROP_LOCATION = './datasets/stockPricesDataset/dropLocation'
def main():
    if len(sys.argv) != 2:
        print("spark-submit wwindow.py overlap(Yes, No)")
        exit(-1)

    overlap = sys.argv[1]

    sparkSession = SparkSession\
        .builder\
        .appName('window_01')\
        .getOrCreate()

    # Set Log level to Error
    sparkSession.sparkContext.setLogLevel("ERROR")

    # Schema
    # Date,Open,High,Low,Close,Adj Close,Volume,Name
    # 2017-01-03,757.919983,758.760010,747.700012,753.669983,753.669983,3521100,AMZN
    schema = StructType([
        StructField('Date', TimestampType(), True),
        StructField('Open', DoubleType(), True),
        StructField('High', DoubleType(), True),
        StructField('Low', DoubleType(), True),
        StructField('Close', DoubleType(), True),
        StructField('Adj Close', DoubleType(), True),
        StructField('Volume', LongType(), True),
        StructField('Name', StringType(), True)
    ])

     # read data stream 
    stocks_df = sparkSession\
        .readStream\
        .option("header", "true")\
        .schema(schema)\
        .csv(DROP_LOCATION)


    print('Is streaming >> ',stocks_df.isStreaming)
    print(stocks_df.printSchema())

    if overlap == "Yes":
    # Calculate 7 days avg closing price with 3 days overlapped window
        output_df = stocks_df\
            .groupBy(window(stocks_df.Date, '7 days','3 days'),stocks_df.Name)\
            .agg({'Close': 'avg'})\
            .withColumnRenamed("avg(Close)", "Average Close")\
            .orderBy("window.start","Name", ascending=False )
    else:
        output_df = stocks_df\
            .groupBy(window(stocks_df.Date, '7 days'),stocks_df.Name)\
            .agg({'Close': 'avg'})\
            .withColumnRenamed("avg(Close)", "Average Close")\
            .orderBy("window.start","Name", ascending=False )

    # output to Console
    query = output_df\
        .writeStream\
        .outputMode("complete")\
        .format("console")\
        .option("truncate", "false")\
        .start().awaitTermination()

if __name__ == "__main__":
    main()