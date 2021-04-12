from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, window, current_timestamp
from pyspark.sql.types import *

DROP_LOCATION = './datasets/supermarketSalesDataset/dropLocation'

def calcualte_rating(rating):
    if rating >= 9:
        return 'Amazing'
    elif rating >= 8:
        return 'Good'
    elif rating >= 5:
        return 'Average'
    else:
        return 'Bad'

def main():
    
    sparkSession = SparkSession\
        .builder\
        .appName('Late Data')\
        .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    # Invoice ID,Branch,City,Customer type,Gender,Product line,Unit price,
    # Quantity,Tax 5%,Total,Date,Time,Payment,cogs,gross margin percentage,
    # gross income,Rating
    # 750-67-8428,A,Yangon,Member,Female,Health and beauty,74.69,
    # 7,26.1415,548.9715,1/5/2019,13:08,Ewallet,522.83,4.761904762,
    # 26.1415,9.1
    schema = StructType([
        StructField('Invoice ID', StringType(), False),
        StructField('Branch', StringType(), False),
        StructField('City', StringType(), False),
        StructField('Customer type', StringType(), False),
        StructField('Gender', StringType(), False),
        StructField('Product line', StringType(), False),
        StructField('Unit price', DoubleType(), False),
        StructField('Quantity', IntegerType(), False),
        StructField('Tax 5%', DoubleType(), False),
        StructField('Total', DoubleType(), False),
        StructField('Date', StringType(), False),
        StructField('Time', StringType(), False),
        StructField('Payment', StringType(), False),
        StructField('cogs', DoubleType(), False),
        StructField('gross margin percentage', DoubleType(), False),
        StructField('gross income', DoubleType(), False),
        StructField('Rating', DoubleType(), False)
    ])
    
    sm_sales_df = sparkSession\
        .readStream.option("header","true")\
            .schema(schema).csv(DROP_LOCATION)
    
    print(sm_sales_df.printSchema())
    print("")
    print("Is Streaming >> ",sm_sales_df.isStreaming)

    # register UDF
    add_rating_udf = udf(calcualte_rating, StringType())

    sm_sales_df = sm_sales_df.withColumn("ingestion_timestamp" ,current_timestamp())

    output_df = sm_sales_df.withWatermark("ingestion_timestamp","1 minutes")\
        .groupBy(window(sm_sales_df.ingestion_timestamp, "1 minutes","30 seconds"),sm_sales_df.City , sm_sales_df.Branch)\
        .agg({'Rating': 'avg'})\
        .withColumnRenamed('avg(Rating)',"Average Rating")\
        .withColumn("comments", add_rating_udf("Average Rating"))\
        .orderBy("Average Rating","City",ascending=False)

    query = output_df\
        .writeStream\
        .outputMode("complete")\
        .format("console")\
        .option("truncate","false")\
        .start()\
        .awaitTermination()
    
if __name__ == "__main__":
    main()

