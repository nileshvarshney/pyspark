from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.master('local[1]').appName('read_write_csv').getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

file_path = os.path.join(os.getcwd(), 'streaming/stock_price/data/stock_data')
output_dir =os.path.join(os.getcwd(), 'dataframes/output_data/csv_read_write')

df = spark.read.format("csv")\
    .options(header = True, delimiter = ",", date_format = 'YYYY-MM-DD', inferSchema = True)\
    .load(file_path)

df_final = df.withColumnRenamed("Adj Close", "AdjClose")

df = df_final.coalesce(1)

# parquet format saving
df.write.parquet(output_dir, compression="snappy", mode="overwrite")

# csv format
# df.write.mode("overwrite").option("header", True).csv(output_dir)

# json format
# df.write.mode("overwrite").json(output_dir)

spark.stop()