import configparser
from pyspark import SparkConf


def get_spark_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for key, value in config.items("SPARK_APP_CONFIG"):
        spark_conf.set(key, value)

    return spark_conf


def load_survey_data(spark, data_file):
    return spark.read \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .option("mode", "FAILFAST")\
        .csv(data_file)


def count_by_country(survey_df):
    return survey_df\
        .filter("age < 40")\
        .select("Age", "Gender", "Country", "state")\
        .groupBy("Country")\
        .count()