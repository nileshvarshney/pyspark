from pyspark.sql import SparkSession
from pyspark import SparkFiles
from dependencies import logger
from os import environ, listdir, path
import json


def start_spark(app_name='sample_start_app', master='local[*]',
                files=[], jar_packages=[], spark_config={}):
    """ Start Spark Session , Get logger and load configuration files"""
    spark_builder = SparkSession.builder.appName(app_name)

    # create spark JARs Strings
    spark_jars_packages = ','.join(list(jar_packages))
    spark_builder.config('spark.jars.packages', spark_jars_packages)

    # create files string
    spark_files = ','.join(list(files))
    spark_builder.config('spark_files', spark_files)

    # add other configs parameters
    for k, v in spark_config:
        spark_builder.config(k, v)

    # create spark session and retrieve logger object
    spark_sess = spark_builder.getOrCreate()
    spark_logger = logger.Log4j(spark_sess)

    # get config files if sent to cluster with files
    spark_files_dir = SparkFiles.getRootDirectory()
    config_files = [filename for filename in listdir(
        spark_files_dir) if filename.endswith('config.json')]

    if config_files:
        path_to_config_file = path.join(spark_files_dir, config_files[0])
        with open(path_to_config_file, 'r') as config_file:
            config_dict = json.load(config_file)
        spark_logger.warn('Loaded configuration from ' + config_files[0])
    else:
        spark_logger.warn('No configuration file found')
        config_dict = None

    return spark_sess, spark_logger, config_dict
