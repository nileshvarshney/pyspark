# read car ads data, max 1 file per trigger and 
# display following informtion on comsole in append mode
# 1. car
# 2. model
# 3. price
# 4. mileage
# 5. year

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os

def main():

    OUTPUT_DIR = 'streaming/other streaming/datasets/carAdsDataset/output_dir'
    CHECKPOINT_DIR = 'streaming/other streaming/datasets/carAdsDataset/checkpoint_dir'
    
    OUTPUT_DIR = os.path.join(os.getcwd(), OUTPUT_DIR)
    CHECKPOINT_DIR = os.path.join(os.getcwd(), CHECKPOINT_DIR)
    DATA_DIR = os.path.join(os.getcwd(), 'streaming/other streaming/datasets/carAdsDataset')

    # create spark session
    spark = SparkSession\
        .builder\
        .master('local')\
        .appName('select_column_rename')\
        .getOrCreate()

    # set log level to Error
    spark.sparkContext.setLogLevel('ERROR')

    # define schema
    # car,price,body,mileage,engV,engType,registration,year,model,drive
    # Ford,15500,crossover,68,2.5,Gas,yes,2010,Kuga,full
    schema = StructType([
        StructField('car', StringType(), False),
        StructField('price', IntegerType(), False),
        StructField('body', StringType(), False),
        StructField('mileage', IntegerType(), False),
        StructField('engV', FloatType(), False),
        StructField('engType', StringType(), False),
        StructField('registration', StringType(), False),
        StructField('year', IntegerType(), False),
        StructField('model', StringType(), False),
        StructField('drive', StringType(), False)
    ])

    # read stream data from drop location, 1 file at a time
    read = spark\
        .readStream\
        .option('header','true')\
        .schema(schema)\
        .option('maxFilesPerTrigger', 1)\
        .csv(DATA_DIR)

    print('Is streaming ?:',read.isStreaming)

    select_df = read.select("car","model","price","mileage","year")\
        .where("year > 2015")\
        .withColumnRenamed("car","make")\
        .withColumnRenamed("mileage", "miles")

    query = select_df\
        .writeStream\
        .format("console")\
        .outputMode('append')\
        .start()\
        .awaitTermination()

if __name__ == "__main__":
    main()
