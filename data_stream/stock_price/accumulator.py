# What are accumulators?
# Accumulators are variables that are used for aggregating information across the executors.

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('accumulator').getOrCreate()

total_acc = spark.sparkContext.accumulator(0)

def main():
    spark.sparkContext.setLogLevel('ERROR')

    rdd = spark.sparkContext.parallelize([2,4,6,8])

    def sum_fn(x):
        global total_acc
        total_acc += x
        print(x, type(total_acc))


    rdd.foreach(sum_fn)
    print('Rsult is ',total_acc.value)

if __name__ == "__main__":
    main()
