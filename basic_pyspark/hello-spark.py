import sys
from pyspark.sql import SparkSession
from lib.logger import Log4j
from lib.util import get_spark_config, load_survey_data, count_by_country


if __name__ == "__main__":
    conf = get_spark_config()
    conf.set("spark.app.name", "hello-spark")

    spark = SparkSession.builder\
        .config(conf=conf)\
        .getOrCreate()

    logger = Log4j(spark)
    logger.info("Starting hello spark")

    # log spark context info
    conf_out = spark.sparkContext.getConf()
    logger.info(conf_out.toDebugString())
    # codes will go here
    if len(sys.argv) != 2:
        logger.error("Usage: hello-spark <filename>")
        sys.exit(-1)

    data_file = sys.argv[1]

    # read the data file
    survey_df = load_survey_data(spark, data_file)
    partitioned_survey_df = survey_df.repartition(2)

    survey_by_country = count_by_country(partitioned_survey_df)
    logger.info(survey_by_country.collect())

    logger.info("Finish hello spark")