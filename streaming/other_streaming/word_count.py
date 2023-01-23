from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode

# open spark session
spark = SparkSession\
    .builder\
    .appName('StructuredNetworkWordCount') \
    .getOrCreate()

# read lines from socket
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# set log level to Error
spark.sparkContext.setLogLevel('ERROR')

# Split the lines into words
words = lines.select(explode(split(lines.value," ")).alias("word"))

# generate running word count
word_counts = words.groupBy("word").count()

# write result to console
query = word_counts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
