from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print('Usages : network_wordcount.py <hostname> <port>', file=sys.strerr)
        sys.exit(-1)

    # create a local streaming context with 2 working thread
    # and batch interval of 1 second
    sc = SparkContext("local[2]", "NetworkWordCount")
    ssc = StreamingContext(sc, 1)

    sc.setLogLevel("ERROR")

    # create a Dstream that connect to hostname:port
    lines = ssc.socketTextStream("localhost", 9999)

    # split the line into words
    # flatMap is a one-to-many DStream operation
    words = lines.flatMap(lambda line : line.split())

    # count each word in each batch
    pairs = words.map(lambda word : (word, 1))
    wordCounts = pairs.reduceByKey(lambda x, y: x + y)

    # print the first 10 elements
    wordCounts.pprint()

    ssc.start()
    ssc.awaitTermination()