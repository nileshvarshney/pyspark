import os
"""The conftest.py file serves as a means of providing fixtures for an entire directory.
   run >>>  pytest data_load_tests.py --no-header -v
"""
import pyspark
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="module")
def spark():
    # conf=pyspark.SparkConf()
    # conf.setExecutorEnv("PYTHONPATH", "$PYTHONPATH:" + os.path.dirname(os.path.abspath(__file__)))
    # spark = SparkSession.builder.master("local[1]").config(conf=conf).getOrCreate()
    # return spark
    spark = SparkSession.builder.master("local[1]").getOrCreate()
    return spark