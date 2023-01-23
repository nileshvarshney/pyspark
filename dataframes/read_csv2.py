from pyspark.sql import SparkSession
import os


warehouse_dir = os.path.join(os.getcwd(),'spark-warehouse')
file_path = os.path.join(os.getcwd(), 'streaming/stock_price/data/stock_data')
output_dir =os.path.join(os.getcwd(), 'dataframes/output_data/csv_read_write')

spark = SparkSession.builder.master('local[1]')\
    .config("spark.sql.warehouse.dir", warehouse_dir)\
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .appName('read_write_csv_2').getOrCreate()

spark.sparkContext.setLogLevel('ERROR')



df = spark.read.format("csv")\
    .options(header = True, delimiter = ",", date_format = 'YYYY-MM-DD', inferSchema = True)\
    .load(file_path)

df_final = df.withColumnRenamed("Adj Close", "AdjClose")

df = df_final.coalesce(1)

df.write.mode("overwrite").saveAsTable("stocks")

read_stock = spark.table("stocks")

read_stock.show(5, truncate=False)

spark.stop()
