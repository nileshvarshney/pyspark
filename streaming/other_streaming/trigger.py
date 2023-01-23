#################################################################
# Triggers :                                                    #
# The trigger settings of a streaming query define the timing   #
# of streaming data processing, whether the query is going to   #
# be executed as micro-batch query with a fixed batch interval  #
# or as a continuous processing query.                          #
# 1. If no trigger setting is explicitly specified, then by     #
# default, the query will be executed in micro-batch mode       #   
# 2. Fixed interval micro-batches
# 3. One-time micro-batch
# 4. Continuous with fixed checkpoint interval              
# --------------------------------------------------------------#
# Task :                                                        #
#   1. Read CarAdsDataset - 1 File Per trigger                  #
#   2. Add current timestamp                                    #
#   3. Calulated max timestamp and avg_price by body            #
#   4. Display result on console by avg price descening         #
#################################################################
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import *
import sys

DROP_LOCATION = './datasets/carAdsDataset/dropLocation'
def main():
    if len(sys.argv) != 2:
        print('Usage : spark-submit trigger_01.py <trigger-mode(1,2 or 3)>')
        exit(-1)
    trigger_mode = sys.argv[1]
    # Open/ Get Spark Session 
    sparkSession = SparkSession\
        .builder\
        .appName('trigger')\
        .getOrCreate()

    # Set Log level to Error
    sparkSession.sparkContext.setLogLevel("ERROR")

    # Schema
    # car,price,body,mileage,engV,engType,registration,year,model,drive
    # Ford,15500,crossover,68,2.5,Gas,yes,2010,Kuga,full
    schema = StructType([
        StructField('car', StringType(), True),
        StructField('price', IntegerType(), True),
        StructField('body', StringType(), True),
        StructField('mileage', IntegerType(), True),
        StructField('engV', FloatType(), True),
        StructField('engType', StringType(), True),
        StructField('registration', StringType(), True),
        StructField('year', IntegerType(), True),
        StructField('model', StringType(), True),
        StructField('drive', StringType(), True)
    ])


    # read data stream - One File per Trigger
    car_df = sparkSession\
        .readStream\
        .option("header", "true")\
        .option("maxFilesPerFrigger", 1)\
        .schema(schema)\
        .csv(DROP_LOCATION)
    print('Is streaming >> ',car_df.isStreaming)
    print(car_df.printSchema())

    car_df = car_df.withColumn("ts" , current_timestamp())

    # aggregate data 
    car_output_df = car_df.groupBy("body")\
        .agg({'price' : 'avg', 'ts' : 'max'})\
        .withColumnRenamed('avg(price)', 'avg_price')\
        .withColumnRenamed('max(ts)', 'max_ts')\
        .sort('avg_price', ascending=False)

    # output data to console
    query = car_output_df\
        .writeStream\
        .outputMode("complete")\
        .format('console')\
        .option('numRows', 15)\
        .option('truncate','false')
        
    if int(trigger_mode) == 1:
        # Default trigger (runs micro-batch as soon as it can)
        pass
    elif int(trigger_mode) == 2:
        # Fixed interval micro-batches
        query.trigger(processingTime='2 seconds')
    elif int(trigger_mode) == 3:
        # One-time micro-batch
        query.trigger(once=True)
    else:
        pass
 
 
    query.start()\
        .awaitTermination()

if __name__ == "__main__":
    main()