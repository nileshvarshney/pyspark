from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, approx_count_distinct
import os


file_path = os.path.join(os.getcwd(), 'streaming/stock_price/data/stock_data')
output_dir =os.path.join(os.getcwd(), 'dataframes/output_data/csv_read_write')

spark = SparkSession.builder.master('local[1]')\
    .appName('read_write_csv_2').getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

ddl_schema = "Date date, Open double, High double, Low double, Close double, AdjClose double, Volume long, Name String"

df = spark.read.format("csv")\
    .options(header = True, delimiter = ",", date_format = 'YYYY-MM-DD')\
    .schema(ddl_schema)\
    .load(file_path)

df.printSchema()
df.show(5, truncate= False)

# with column
df_mobile = df.withColumn("mobile", col("name").isin("AAPL","GOOGL"))\
    .filter(col("Date") > "2020-01-01")\
    .filter(col("Volume").isNotNull())\
    .drop_duplicates(["Date","Name"])\
    .orderBy(["Date","Name"], ascending = False)
df_mobile.show(5, truncate=False)

# group by stuff
daily_volumn_sum_df = df_mobile.groupBy("Date").sum("Volume")

agg_example = df_mobile.groupBy("Date").agg(sum("Open").alias("Open"), \
    approx_count_distinct("Name").alias("distinct_name"))

agg_example.show(5, truncate= False)

spark.stop()
