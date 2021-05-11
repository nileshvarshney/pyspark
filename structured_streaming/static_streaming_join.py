from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import *

STATIC_LOCATION = "./datasets/lifeExpectancyDataset/static_files/*.csv"
DROP_LOCATION_2 = './datasets/lifeExpectancyDataset/dropLocation'

def main():
    # get or open Spark Session
    spark = SparkSession\
        .builder.master("local")\
        .appName('Static-Streaming-Join')\
        .getOrCreate()

    # Schema 1
    # Country,Year,GDP,Population
    # Afghanistan,2015,584.3,33736494.0 
    schema_1 = StructType([
        StructField('Country', StringType(), False),
        StructField('Year', IntegerType(), False),
        StructField('GDP', DoubleType(), False),
        StructField('Population', DoubleType(), False)
    ]) 

    # Country,Year,Status,LifeExpectancy
    # Afghanistan,2015,Developing,65.0
    schema_2 =  StructType([
        StructField('Country', StringType(), False),
        StructField('Year', IntegerType(), False),
        StructField('Status', StringType(), False),
        StructField('LifeExpectancy', FloatType(), False)
    ]) 

    # set Log level to Error
    spark.sparkContext.setLogLevel('ERROR')

    # Static data
    static_gdp_df = spark\
        .read\
        .format("csv")\
        .option("header","true")\
        .schema(schema_1)\
        .load(STATIC_LOCATION)

    print("********* Static Data *****************")
    print("")
    print(static_gdp_df.printSchema())

    # steaing LifeExpectancy
    le_df = spark\
        .readStream\
        .option("header","true")\
        .schema(schema_2)\
        .csv(DROP_LOCATION_2)

    print("Is LifeExpectancy streaming >> ", le_df.isStreaming)
    print("")
    print(le_df.printSchema())


    # Join on year and country
    join_df = static_gdp_df.join(le_df, on=["Country","Year"])

    output_df = join_df.select("Country", "Year","Status","Population","LifeExpectancy")
        

    query = output_df\
        .writeStream\
        .outputMode("append")\
        .format("console")\
        .option("truncate","false")\
        .start()\
        .awaitTermination()

if __name__ == "__main__":
    main()