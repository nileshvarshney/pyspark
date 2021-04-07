import sys
# for docker execution - nc -lkp 9999
# for mac execution: nc -lk 9999 
from pyspark.sql import SparkSession


def main():
    if len(sys.argv) != 3:
        print("Usage: spark-submit happiness.py <host> <port>", file=sys.stderr)
        exit(-1)

    host = sys.argv[1]
    port = sys.argv[2]

    # open spark session
    spark = SparkSession\
        .builder\
        .appName("World Happiness")\
        .getOrCreate()


    readStream = spark\
        .readStream\
        .format('socket')\
        .option('host', host)\
        .option('port', port)\
        .load()


    readStream_df = readStream.selectExpr("split(value, ',')[0] as Country",\
                                 "split(value, ',')[1] as Region",\
                                 "split(value, ',')[2] as HappinessScore"\
        )

    readStream_df.createOrReplaceTempView("happiness")

    averageScore = spark.sql("""SELECT 
                                    Region, 
                                    AVG(HappinessScore) as Avg_Happiness_Score
                                FROM happiness
                                GROUP BY Region"""
                             )    

    
    query = averageScore\
        .writeStream\
        .format('console')\
        .outputMode('complete')\
        .start()\
        .awaitTermination()

if __name__ == "__main__":
    main()