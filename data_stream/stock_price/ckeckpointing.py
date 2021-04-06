import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

def main():
    if len(sys.argv) != 1:
        print('Usage : spark-submit checkpointing.py')
        exit(-1)


    # create spark session
    sparkSession = SparkSession\
        .builder\
        .master('local')\
        .appName('checkpointing')\
        .getOrCreate()

    sparkSession.sparkContext.setLogLevel('ERROR')

    # define schema
    stock_schema = StructType([
        StructField('Date',StringType(),True),
        StructField('Open',StringType(),True),
        StructField('High',StringType(),True),
        StructField('Low',StringType(),True),
        StructField('Close',StringType(),True),
        StructField('Adj Close',StringType(),True),
        StructField('Volume',StringType(),True),
        StructField('Name',StringType(),True)
    ])

    # read stream data
    stock_price_df = sparkSession\
        .readStream\
        .option('header','true')\
        .schema(stock_schema)\
        .csv('./data')

    stock_price_df.createOrReplaceTempView('stocks')

    stock_df = sparkSession.sql("""
        SELECT
            Name,
            avg(Open) avg_open,
            avg(Close) avg_close
        FROM stocks 
        GROUP BY Name
    """)

    query = stock_df\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .option('truncate','false')\
        .option('numRows', 10)\
        .option('checkpointLocation', 'checkpoint')\
        .start()\
        .awaitTermination()

if __name__ == "__main__":
    main()











