import argparse
from typing import Dict

from pyspark.sql import SparkSession
from flow import execute


def main(spark: SparkSession, args: Dict):
    print('This is spark')
    execute(spark, args)


if __name__ == '__main__':
    spark_session = SparkSession.builder.appName('testing').getOrCreate()
    parser = argparse.ArgumentParser(description='some spark files')
    parser.add_argument("input_file_location", help="input file location")
    parser.add_argument("output_file_location", help = "output file location")

    args = parser.parse_args()
    main(spark_session, args)