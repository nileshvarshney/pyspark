"""
foreachBatch(...) allows you to specify a function that is executed on the output data of every micro-batch of a streaming query.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp


def main():
    OUTPUT_DIR = 'streaming/other_streaming/datasets/historicalDataset/output_dir'
    CHECKPOINT_DIR = 'streaming/other_streaming/datasets/historicalDataset/checkpoint_dir'
    
    OUTPUT_DIR = os.path.join(os.getcwd(), OUTPUT_DIR)
    CHECKPOINT_DIR = os.path.join(os.getcwd(), CHECKPOINT_DIR)
    DATA_DIR = os.path.join(os.getcwd(), 'streaming/other_streaming/datasets/historicalDataset/data')
  
    sparkSession = SparkSession\
        .builder\
        .master('local')\
        .appName('for_each_batch')\
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
    count_df = read_stream_df.groupBy('Country_Code').count().withColumn("process_time",current_timestamp())

    def process_batch(df, epochId):
        print(epochId)
        file_path = os.path.join(OUTPUT_DIR, str(epochId))
        print(file_path)
        # merge all output to a single file using repartition.
        df.repartition(1).write.mode("errorifexists").csv(file_path)

    query = count_df\
        .writeStream\
        .foreachBatch(process_batch)\
        .option('checkpointLocation', CHECKPOINT_DIR)\
        .outputMode('update')\
        .start()\
        .awaitTermination()

if __name__ == "__main__":
    main()