"""
This spark streaming program will read csv
landed in ./data directory, process it in append mode and
display 5 records when close price is higer than open price
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

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
        .option('header','true')\
        .schema(schema)\
        .csv('./data')

    print(stockPriceDF.printSchema())

    # get selected columns where open > close
    days_df = stockPriceDF\
        .select("Name","Date","Open","close")\
        .where("open > close")

    query = days_df\
        .writeStream\
        .outputMode('append')\
        .format('console')\
        .option('truncate','false')\
        .option('numRows',5)\
        .start()\
        .awaitTermination()

if __name__ == "__main__":
    main()