"""
foreachBatch(...) allows you to specify a function that is executed on the output data of every micro-batch of a streaming query.
"""

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
        .appName('for_each_batch_2')\
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
        .option("maxFilesPerTrigger", 5)\
        .schema(schema)\
        .csv(DATA_DIR)
    
    print('Is streaming ready ?:', read_stream_df.isStreaming)
    print(read_stream_df.printSchema())

    df_select = read_stream_df.selectExpr("*")

    def process_batch(df, epochId):
        print('Processing batch : '.format(epochId))

        df_FI = df.select("Date","Article_ID","Sold_Units")\
            .where("Country_Code == 'FI'")

        df_AI = df.select("Date","Article_ID","Sold_Units")\
            .where("Country_Code == 'AI'")

        df_FR = df.select("Date","Article_ID","Sold_Units")\
            .where("Country_Code == 'FR'")

        df_SE = df.select("Date","Article_ID","Sold_Units")\
            .where("Country_Code == 'SE'")

        file_path = os.path.join(OUTPUT_DIR, str(epochId))


        if (df_FI.count() > 0):
            df_FI.repartition(1).write.mode('append').csv(os.path.join(file_path,'FI'))
        if (df_AI.count() > 0):
            df_AI.repartition(1).write.mode('append').csv(os.path.join(file_path,'AI'))
        if (df_FR.count() > 0):
            df_FR.repartition(1).write.mode('append').csv(os.path.join(file_path,'FR'))
        if (df_SE.count() > 0):
            df_SE.repartition(1).write.mode('append').csv(os.path.join(file_path,'SE'))
        

    query = df_select\
        .writeStream\
        .foreachBatch(process_batch)\
        .option('checkpointLocation', CHECKPOINT_DIR)\
        .outputMode('append')\
        .start()\
        .awaitTermination()

if __name__ == "__main__":
    main()