import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *

def main():
    if len(sys.argv)  != 1:
        print('Usage : spark-submit continous_processing.py')
        exit(-1)


    # create spark session
    sparkSession = SparkSession.builder.appName('continous_processing').getOrCreate()

    sparkSession.sparkContext.setLogLevel('ERROR')

    stream_df = sparkSession.readStream.format('rate')\
        .option('rowPerSecond', 1).option('rampUpTime', 1).load()


    print('Is streaming..', stream_df.isStreaming)


    print(stream_df.printSchema())

    select_df = stream_df.selectExpr("*")

    query = select_df.writeStream.outputMode('append').format('console').trigger(continuous = '3 second').start()


    query.awaitTermination()


if __name__ == "__main__":
    main()