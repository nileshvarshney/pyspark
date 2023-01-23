"""
This spark streaming program will read csv
landed in data directory, process it in append mode and
store processed data in csv format at specified location partitioned by name and year
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.functions import year

def main():
    if len(sys.argv) != 1:
        print('Usage : spark-submit stock_price.py')
        exit(-1)

    # schema
    # Date,Open,High,Low,Close,Adj Close,Volume,Name
    schema = StructType([
        StructField('Date',StringType(),True),
        StructField('Open',StringType(),True),
        StructField('High',StringType(),True),
        StructField('Low',StringType(),True),
        StructField('Close',StringType(),True),
        StructField('Adj Close',StringType(),True),
        StructField('Volume',StringType(),True),
        StructField('Name',StringType(),True)
    ])

    # open spark session
    spark = SparkSession\
        .builder\
        .master('local')\
        .appName('Stock Price')\
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    stockPriceDF = spark\
        .readStream\
        .option("maxFilesPerTrigger", 1)\
        .option('header','true')\
        .schema(schema)\
        .csv('streaming/stock_price/data/stock_data/*.csv')

    print(stockPriceDF.printSchema())

    # get selected columns where open > close
    days_df = stockPriceDF\
        .withColumn("year", year("Date"))\
        .select("Name","Date","Open","close", "year")\
        .where("open > close")

    query = (
        days_df
        .writeStream
        .partitionBy('name','year')
        .outputMode('append')
        .format("csv")
        .option("path", "streaming/stock_price/data/output")
        .option("header", "true")
        .option("checkpointLocation", "streaming/stock_price/data/checkpointLocation")
        .start()
        .awaitTermination()
    )

if __name__ == "__main__":
    main()

# .format('console')\
# .option('truncate','false')\
# .option('numRows',5)\