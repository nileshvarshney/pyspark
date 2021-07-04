import sys
import re
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    sc = SparkContext("local[2]", 'Text File Streaming')
    ssc = StreamingContext(sc, 5)
    streaming_dir = './data/textStreamingSource'
    sc.setLogLevel("ERROR")
    streamRDD = ssc.textFileStream(streaming_dir)

    # filter out header record from the file if exist
    stream_without_header = streamRDD.filter(lambda x : None == re.search('^Region',x))

    # count the number of records
    record_count = \
        stream_without_header.count()

    record_count.pprint()

    # group data by year and get count
    grouped_by_year = stream_without_header.filter(
        lambda x: 7 == len(x.split(',')))\
        .map(lambda x : (x.split(',')[5], 1)) \
        .count()
        #.groupByKey().mapValues(len)
    #grouped_by_year.pprint()
    ssc.start()
    ssc.awaitTermination()




