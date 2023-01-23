import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


def main():

    if len(sys.argv) != 1:
        print('Usage : spark-submit update_stock_price_sql.py')
        exit(-1)

    # create spark session
    spark = (
            SparkSession
            .builder
            .master('local')
            .appName('update_stock_price_sql')
            .getOrCreate()
    )
    # define schema
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

    spark.sparkContext.setLogLevel('ERROR')

    stock_df = (
        spark\
        .readStream\
        .option('header','true')\
        .option('maxFilesPerTrigger',10)\
        .schema(stockSchema)\
        .csv('streaming/stock_price/data/stock_data/*.csv')
    )


    # create temp view
    stock_df.createOrReplaceTempView('stocks_update')

    stock_sql = spark.sql("""
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


    query = (
        stock_sql
        .writeStream
        .outputMode('update')
        # Data source csv does not support Update output mode
        .format('console')
        .option('truncate' ,'false')
        .option('numRows', 10)
        .start()
        .awaitTermination()
    )
if __name__ == "__main__":
    main()