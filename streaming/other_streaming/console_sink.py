from pyspark.sql import SparkSession
from pyspark.sql.types import *

def main():
    sparkSession = SparkSession\
        .builder\
        .appName('console_sink')\
        .getOrCreate()

    # set log level
    sparkSession.sparkContext.setLogLevel('ERROR')

    #Date,Article_ID,Country_Code,Sold_Units
    schema = StructType([
        StructField('Date', StringType(), False),
        StructField('Article_ID', StringType(), False),
        StructField('Country_Code', StringType(), False),
        StructField('Sold_Units', IntegerType(), False)       
    ])

    # read data files
    read_stream_df = sparkSession\
        .readStream\
        .option('header', 'true')\
        .schema(schema)\
        .csv('./datasets/historicalDataset/dropLocation')
    
    print('Is streaming ready ?:', read_stream_df.isStreaming)
    print(read_stream_df.printSchema())

    select_df = read_stream_df.selectExpr("*")

    # console sink
    query = select_df\
        .writeStream\
        .format("console")\
        .outputMode('append')\
        .start()\
        .awaitTermination()

if __name__ == "__main__":
    main()

