from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('Array Function').getOrCreate()

data = [
    ("x", 4, 1),
    ("x", 6, 2),
    ("z", 7, 3),
    ("a", 3, 4),
    ("z", 5, 2),
    ("x", 7, 3),
    ("x", 9, 7),
    ("z", 1, 8),
    ("z", 4, 9),
    ("z", 7, 4),
    ("a", 8, 5),
    ("a", 5, 2),
    ("a", 3, 8),
    ("x", 2, 7),
    ("z", 1, 9)]
columns = ['col1', 'col2', 'col3']
data_df = spark.createDataFrame(data=data, schema=columns)

print(data_df.show(3))

array_df = \
    data_df\
    .groupBy("col1")\
    .agg(collect_list('col2').alias("col2_array"),
         collect_list('col3').alias("col3_array"))
print(array_df.show())

# distinct value of the array
print(
    array_df
    .withColumn("result", array_distinct(array_df.col2_array))
    .withColumn('max_value_col3', array_max(array_df.col3_array))
    .show()
)

print(
    array_df
    .withColumn('col2_contain', array_contains(array_df.col2_array, 7))
    .show()
)

print(
    array_df
    .withColumn('union_col2_col3', array_union(array_df.col2_array, array_df.col3_array))
    .withColumn('intersect_col2_col3', array_intersect(array_df.col2_array, array_df.col3_array))
    .withColumn('expect_col2_col3', array_except(array_df.col2_array, array_df.col3_array))
    .withColumn('col2_join', array_join(array_df.col2_array, ' => '))
    .withColumn('col2_position', array_position(array_df.col2_array, 7))
    .show(3, False)
)

print(
    array_df
    .withColumn('renove_7_col2', array_remove(array_df.col2_array, 7))
    .withColumn('repeat', array_repeat("renove_7_col2", 2))
    .withColumn('sort', array_sort(array_df.col3_array))
    .withColumn('check_overlap', arrays_overlap(array_df.col3_array, array_df.col2_array))
    .withColumn('zipped', arrays_zip(array_df.col3_array, array_df.col2_array))
    .show(4, False)
)

print(
    array_df
    .withColumn('element_at', element_at(array_df.col2_array, 2))
    .withColumn('repeat', array_repeat(array_df.col2_array, 2))
    .withColumn('flattern', flatten("repeat"))
    .show(4, False)
)

print(
    array_df
    .withColumn('reverse', reverse(array_sort("col2_array")))
    .withColumn('shuffle', shuffle(array_sort("col2_array")))
    .withColumn('shuffle', size("col2_array"))
    .withColumn('shuffle', slice("col2_array", 1, 3))
    .withColumn('explode', explode("col2_array"))
    .show(12, False)
)
