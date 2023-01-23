import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


def main():

    if len(sys.argv) != 1:
        print('Usage : spark-submit update_stock_price.py')
        exit(-1)

    # create spark session
    sparkSession = SparkSession\
        .builder\
        .master('local')\
        .appName('update_stock_price')\
        .getOrCreate()

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

    sparkSession.sparkContext.setLogLevel('ERROR')

    stock_df = sparkSession\
        .readStream\
        .option('header','true')\
        .option('maxFilesPerTrigger',4)\
        .schema(stockSchema)\
        .csv('streaming/stock_price/data/stock_data/*.csv')

    # aggregate data min open price and max close price
    min_max_by_stock = stock_df\
        .withColumn('year', year("Date"))\
        .groupBy(['Name','year'])\
        .agg({'Open' : 'min', 'Close' : 'max'})\
        .withColumnRenamed('min(Open)', 'min_open')\
        .withColumnRenamed('max(Close)','max_close')


    query = min_max_by_stock\
        .writeStream\
        .outputMode('update')\
        .format('console')\
        .option('truncate' ,'false')\
        .option('numRows', 10)\
        .start()\
        .awaitTermination()

if __name__ == "__main__":
    main()