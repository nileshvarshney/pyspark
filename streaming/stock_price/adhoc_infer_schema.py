import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# batch Processing
# issue: unable to archive after processing the specified file.

def main():

    if len(sys.argv) != 1:
        print('Usage : spark-submit adhoc_infer_schema.py')
        exit(-1)

    # create spark session
    sparkSession = SparkSession\
        .builder\
        .appName('adhoc infer Schema')\
        .master("local[*]")\
        .config("spark.sql.shuffle.partitions", 3)\
        .config("spark.streaming.stopGracefullyOnShutdown","true")\
        .config("spark.sql.streaming.schemaInference", "true")\
        .getOrCreate()

    sparkSession.sparkContext.setLogLevel('ERROR')

    stock_df = sparkSession\
        .read\
        .option('header','true')\
        .option('inferSchema', 'true')\
        .csv('streaming/stock_price/data/stock_data/FB*.csv')

    print(stock_df.printSchema())

    stock_df.createOrReplaceTempView('stocks_infer')

    stock_sql = sparkSession.sql("""
        SELECT 
            Date,
            round(Open, 2) as open,
            round(High, 2) as high,
            round(Low, 2) as low,
            round(Close, 2) as close,
            round(`Adj Close`, 2) as adj_close,
            Volume as volume,
            Name as name
        FROM stocks_infer 
    """)

    print(stock_sql.show(20, truncate=False))


if __name__ == "__main__":
    main()