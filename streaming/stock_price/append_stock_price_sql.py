import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.functions import input_file_name

# example with cleanSource archive strategy
# carefully execute it

def main():
    if len(sys.argv) != 1:
        print('Usage :  spark-submit append_stock_price_sql.py', file=sys.stderr)


    # create spark session
    sparkSession = SparkSession\
        .builder\
        .master('local')\
        .appName('append_stock_price_sql')\
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
        .option("maxFilesPerTrigger", 1)\
        .option("cleanSource", "archive")\
        .option("sourceArchiveDir","streaming/stock_price/data/archive")\
        .schema(stock_schema)\
        .csv('streaming/stock_price/data/stock_data/FB*.csv')

    # include filename
    stock_price_df = stock_price_df.withColumn("filename", input_file_name())

    stock_price_df.createOrReplaceTempView('stocks')

    stock_df = sparkSession.sql("""
        SELECT
            Name,
            Date,
            (Open - Close) as day_diff,
            filename
        FROM stocks 
        WHERE Open - Close > 5
    """)

    query = stock_df\
        .writeStream\
        .outputMode('append')\
        .format('console')\
        .option('truncate','false')\
        .option('numRows', 10)\
        .start()\
        .awaitTermination()

if __name__ == "__main__":
    main()











