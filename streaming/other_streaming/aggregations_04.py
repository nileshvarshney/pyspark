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

    DROP_LOCATION = 'streaming/other streaming/datasets/carAdsDataset'
    # create spark session
    spark = SparkSession\
        .builder\
        .master('local')\
        .appName('aggregations_03')\
        .getOrCreate()

    # set log level to Error
    spark.sparkContext.setLogLevel('ERROR')

    # define schema
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

    clean_df = read.selectExpr("*").where("price != 0")

    clean_df.createOrReplaceTempView("car_ads")
 
    select_df = spark.sql("""
        select car as make, round(min(price)) as min_price, round(max(price)) as max_price,
        round(avg(price)) as avg_price from car_ads
        where year > 2014
        group by car
        order by 3 desc
        limit 5
    """)

    query = select_df\
        .writeStream\
        .format("console")\
        .outputMode('complete')\
        .start()\
        .awaitTermination()

if __name__ == "__main__":
    main()
