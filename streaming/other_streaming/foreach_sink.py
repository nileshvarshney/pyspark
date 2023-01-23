# This program reads files from drop location and create files for 
# each country code and store no of overservations
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *

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
        .option("maxFilesPerTrigger", 5)\
        .option('header', 'true')\
        .schema(schema)\
        .csv(DATA_DIR)
    
    print('Is streaming ready ?:', read_stream_df.isStreaming)
    print(read_stream_df.printSchema())

    # count records by country Code
    count_df = read_stream_df.groupBy('Country_Code').count()

    def process_row(row):
        file_path = os.path.join(OUTPUT_DIR, row["Country_Code"])
        with open(file_path, 'a+') as f:
            f.write("%s, %s\n" % (row["Country_Code"], row["count"]))
            f.close()

    query = count_df\
        .writeStream\
        .foreach(process_row)\
        .outputMode('complete')\
        .option('checkpointLocation',CHECKPOINT_DIR)\
        .start()\
        .awaitTermination()

if __name__ == "__main__":
    main()