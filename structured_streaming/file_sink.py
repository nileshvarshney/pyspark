from pyspark.sql import SparkSession
from pyspark.sql.types import *

def main():

    OUTPUT_DIR = './output_dir'
    CHECKPOINT_DIR = './checkpoint_dir'
    
    sparkSession = SparkSession\
        .builder\
        .appName('file_sink')\
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
    final_df = select_df.select(
        "Date","Article_ID","Country_Code","Sold_Units")\
            .withColumnRenamed("Date", 'date')\
            .withColumnRenamed("Article_ID",'articleId')\
            .withColumnRenamed("Country_Code", 'countryCode')\
            .withColumnRenamed("Sold_Units",'soldUnits')

    # file sink - json format
    query =   final_df\
        .writeStream\
        .outputMode('append')\
        .format('json')\
        .option('path', OUTPUT_DIR)\
        .option('checkpointLocation', CHECKPOINT_DIR)\
        .start()\
        .awaitTermination()      

if __name__ == "__main__":
    main()

