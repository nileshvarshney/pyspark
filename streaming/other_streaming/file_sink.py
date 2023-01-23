from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os

def main():

    OUTPUT_DIR = 'streaming/other streaming/datasets/historicalDataset/output_dir'
    CHECKPOINT_DIR = 'streaming/other streaming/datasets/historicalDataset/checkpoint_dir'
    
    OUTPUT_DIR = os.path.join(os.getcwd(), OUTPUT_DIR)
    CHECKPOINT_DIR = os.path.join(os.getcwd(), CHECKPOINT_DIR)
    DATA_DIR = os.path.join(os.getcwd(), 'streaming/other streaming/datasets/historicalDataset/data')

    sparkSession = SparkSession\
        .builder\
        .master('local')\
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
        .option("maxFilesPerTrigger", 1)\
        .option('header', 'true')\
        .schema(schema)\
        .csv(DATA_DIR)
    
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
        .partitionBy("countryCode")\
        .outputMode('append')\
        .format('json')\
        .option('path', OUTPUT_DIR)\
        .option('checkpointLocation', CHECKPOINT_DIR)\
        .start()\
        .awaitTermination()      

if __name__ == "__main__":
    main()

