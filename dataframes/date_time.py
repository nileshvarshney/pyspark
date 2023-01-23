from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, month, year, hour, minute, date_add,date_trunc

spark = SparkSession.builder.master('local[1]')\
    .appName('date_time').getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

df = spark.createDataFrame( [{'user_id': 'UA000000107379500', 'timestamp': 1593878946592107}])

df_timestamp = df.withColumn("timestamp", (col("timestamp")/1e6).cast("timestamp"))\
    .withColumn("date_string", date_format("timestamp", "dd-MM-yyyy"))\
    .withColumn("time_string", date_format("timestamp", "HH:mm:ss.SSSSSS"))\
    .withColumn("month", month("timestamp"))\
    .withColumn("year", year("timestamp"))\
    .withColumn("hour", hour("timestamp"))\
    .withColumn("minute", minute("timestamp"))\
    .withColumn("plus_2_day", date_add(col("timestamp"),2))\
    .withColumn('DateTrunc', date_trunc('quarter', col("timestamp")))

df_timestamp.show(5, False)

df_timestamp.explain(True)

spark.stop()