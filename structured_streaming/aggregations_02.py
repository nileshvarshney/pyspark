# read car ads data, max 1 file per trigger and 
# display following informtion on comsole in append mode
# 1. car
# 2. model
# 3. price
# 4. mileage
# 5. year

from pyspark.sql import SparkSession
from pyspark.sql.types import *

def main():

    DROP_LOCATION = './datasets/carAdsDataset/dropLocation'
    # create spark session
    spark = SparkSession\
        .builder\
        .appName('aggregations_02')\
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
        .csv(DROP_LOCATION)

    print('Is streaming ?:',read.isStreaming)

    select_df = read\
        .groupBy("year")\
        .count()\
        .withColumnRenamed('count', 'year count')\
        .orderBy("year", ascending=False)

    query = select_df\
        .writeStream\
        .format("console")\
        .outputMode('complete')\
        .start()\
        .awaitTermination()

if __name__ == "__main__":
    main()
