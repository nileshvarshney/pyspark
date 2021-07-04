# create sample.json file
data = """{
	"id": "0001",
	"type": "donut",
	"name": "Cake",
	"ppu": 0.55,
	"batters":
		{
			"batter":
				[
					{ "id": "1001", "type": "Regular" },
					{ "id": "1002", "type": "Chocolate" },
					{ "id": "1003", "type": "Blueberry" },
					{ "id": "1004", "type": "Devil's Food" }
				]
		},
	"topping":
		[
			{ "id": "5001", "type": "None" },
			{ "id": "5002", "type": "Glazed" },
			{ "id": "5005", "type": "Sugar" },
			{ "id": "5007", "type": "Powdered Sugar" },
			{ "id": "5006", "type": "Chocolate with Sprinkles" },
			{ "id": "5003", "type": "Chocolate" },
			{ "id": "5004", "type": "Maple" }
		]
}"""

with open('sample.json','w') as f:
  f.write(data)

from pyspark.sql import  SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('Json Playground').getOrCreate()

json_df = spark.read.json("sample.json",multiLine=True)
print(json_df.printSchema())

sample_df = json_df.withColumnRenamed("id","key")

sam_df = sample_df.select("key", "batters.batter")
print(sam_df.printSchema())

print(sam_df.show(1, False))

sam2_df = sam_df.select("key",explode("batter").alias("new_batter"))
print(sam2_df.show())

print(sam2_df.select("key","new_batter.*").show())

sam3_df = sam2_df.select("key","new_batter.*")\
  .withColumnRenamed("id", "batter_id")\
  .withColumnRenamed("type","batter_type")
print(sam3_df.show())

final_df =  sample_df.select("key","name", "ppu", "topping", "batters.batter")\
  .select("key","batter","name", "ppu", explode("topping").alias("new_topping"))\
  .select("key","batter","name", "ppu","new_topping.*")\
  .withColumnRenamed("id","topping_id")\
  .withColumnRenamed("type","topping_type")\
  .select("key",explode("batter").alias("new_batter"),"name", "ppu","topping_id","topping_type") \
  .select("key","new_batter.*","name", "ppu","topping_id","topping_type") \
  .withColumnRenamed("id","batter_id")\
  .withColumnRenamed("type","batter_type")

print(final_df.show())

