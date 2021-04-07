import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def main():

    if len(sys.argv) != 1:
        print('Usage : spark-submit adhoc_infer_schema.py')
        exit(-1)

    # create spark session
    sparkSession = SparkSession\
        .builder\
        .appName('adhoc infer Schema')\
        .getOrCreate()


    sparkSession.sparkContext.setLogLevel('ERROR')

    stock_df = sparkSession\
        .read\
        .option('header','true')\
        .option('maxFilesPerTrigger',10)\
        .option('inferSchema', 'true')\
        .csv('./data')

    print(stock_df.printSchema())

    stock_df.createOrReplaceTempView('stocks_infer')

    stock_sql = sparkSession.sql("""
        SELECT *
        FROM stocks_infer 
    """)

    print(stock_sql.show(20, truncate=False))


if __name__ == "__main__":
    main()