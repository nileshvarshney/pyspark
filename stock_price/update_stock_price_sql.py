import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


def main():

    if len(sys.argv) != 1:
        print('Usage : spark-submit update_stock_price_sql.py')
        exit(-1)

    # create spark session
    sparkSession = SparkSession\
        .builder\
        .appName('update_stock_price_sql')\
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
        .option('maxFilesPerTrigger',10)\
        .schema(stockSchema)\
        .csv('./data')

    stock_df.createOrReplaceTempView('stocks_update')

    stock_sql = sparkSession.sql("""
        SELECT 
            Name,
            min(Open) min_open,
            max(Open) max_open,
            avg(Open) avg_open,
            min(Close) min_close,
            max(Close) max_close,
            avg(Close) avg_close,
            std(Open) std_open,
            std(Close) std_close
        FROM stocks_update 
        GROUP BY Name"""
    )


    query = stock_sql\
        .writeStream\
        .outputMode('update')\
        .format('console')\
        .option('truncate' ,'false')\
        .option('numRows', 10)\
        .start()\
        .awaitTermination()

if __name__ == "__main__":
    main()