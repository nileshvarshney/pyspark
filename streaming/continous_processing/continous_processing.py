import sys
from pyspark.sql import SparkSession

def main():
    if len(sys.argv)  != 1:
        print('Usage : spark-submit continous_processing.py')
        exit(-1)


    # create spark session
    sparkSession = SparkSession\
        .builder\
        .appName('continous_processing')\
        .master("local[*]")\
        .config("spark.sql.shuffle.partitions", 3)\
        .config("spark.streaming.stopGracefullyOnShutdown","true")\
        .config("spark.sql.streaming.schemaInference", "true")\
        .getOrCreate()

    # set log level to error
    sparkSession.sparkContext.setLogLevel('ERROR')

    # Rate source (for testing) - Generates data at the specified number of rows per second, 
    # each output row contains a timestamp and value.
    stream_df = sparkSession.readStream.format('rate')\
        .option('rowPerSecond', 100)\
        .option('rampUpTime', 1)\
        .option("numPartitions",2)\
        .load()


    print('Is streaming..', stream_df.isStreaming)

    print(stream_df.printSchema())

    select_df = stream_df.selectExpr("*")

    query = select_df.writeStream\
        .outputMode('append')\
        .format('console')\
        .trigger(continuous = '3 second')\
        .option("truncate","false")\
        .start()\
        .awaitTermination()


if __name__ == "__main__":
    main()