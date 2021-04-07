import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *

def main():

    OUTPUT_DIR = './output_dir'
    CHECKPOINT_DIR = './checkpoint_dir'
    
    sparkSession = SparkSession\
        .builder\
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
        .option('header', 'true')\
        .schema(schema)\
        .csv('./datasets/historicalDataset/dropLocation')
    
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
        df_FI.repartition(1).write.mode('append').csv(os.path.join(file_path,'FI'))
        df_AI.repartition(1).write.mode('append').csv(os.path.join(file_path,'AI'))
        df_FR.repartition(1).write.mode('append').csv(os.path.join(file_path,'FR'))
        df_SE.repartition(1).write.mode('append').csv(os.path.join(file_path,'SE'))
        

    query = df_select\
        .writeStream\
        .foreachBatch(process_batch)\
        .outputMode('append')\
        .start()\
        .awaitTermination()

if __name__ == "__main__":
    main()