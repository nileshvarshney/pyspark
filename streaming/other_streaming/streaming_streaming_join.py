from pyspark.sql import SparkSession
from pyspark.sql.types import *

DROP_LOCATION_1 = './datasets/lifeExpectancyDataset/dropLocation_1'
DROP_LOCATION_2 = './datasets/lifeExpectancyDataset/dropLocation_2'

def main():
    # get or open Spark Session
    spark = SparkSession\
        .builder\
        .appName('Streaming-Streaming-Join')\
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

    # steaing GDP
    gdp_df = spark\
        .readStream\
        .option("header","true")\
        .schema(schema_1)\
        .csv(DROP_LOCATION_1)

    print("Is GDP streaming >> ", gdp_df.isStreaming)
    print("")
    print(gdp_df.printSchema())

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
    join_df = gdp_df.join(le_df, on=["Country","Year"])

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