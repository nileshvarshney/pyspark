from os import write
from pyspark.sql import SparkSession, Row
from dependencies.spark import start_spark
from pyspark.sql.functions import *


def main():

    spark, log, config = start_spark(app_name='my_spark_app', files=[
        'configs/etl_config.json'])

    log.warn('etl job  is up and running')

    # execute ETL pipeline
    data = extract_data(spark)
    data_transformed = transform_data(data, config['steps_per_floor_'])
    load_data(data_transformed)

    log.warn('test etl job finished')
    spark.stop()
    return None


def extract_data(spark):
    """ Load data from Parquet file"""
    df = (spark.read.parquet('tests/test_data/employees'))
    return df


def transform_data(df, steps_per_floor_):
    df_transformed = (df.select(col('id'),
                                concat_ws(' ', col('first_name'), col(
                                    'second_name')).alias('name'),
                                (col('floor') * lit(steps_per_floor_)).alias('steps_to_desk')))

    return df_transformed


def load_data(df):
    (df.coalesce(1).write.to_csv('loaded_data', mode='overwrite', header=True))
    return None


def create_test_data(spark, config):
    local_records = [
        Row(id=1, first_name='Dan', second_name='Germain', floor=1),
        Row(id=2, first_name='Dan', second_name='Sommerville', floor=1),
        Row(id=3, first_name='Alex', second_name='Ioannides', floor=2),
        Row(id=4, first_name='Ken', second_name='Lai', floor=2),
        Row(id=5, first_name='Stu', second_name='White', floor=3),
        Row(id=6, first_name='Mark', second_name='Sweeting', floor=3),
        Row(id=7, first_name='Phil', second_name='Bird', floor=4),
        Row(id=8, first_name='Kim', second_name='Suter', floor=4)
    ]

    df = spark.createDataFrame(local_records)

    # write to Parquet file format
    (df
     .coalesce(1)
     .write
     .parquet('tests/test_data/employees', mode='overwrite'))

    # create transformed version of data
    df_tf = transform_data(df, config['steps_per_floor'])

    # write transformed version of data to Parquet
    (df_tf
     .coalesce(1)
     .write
     .parquet('tests/test_data/employees_report', mode='overwrite'))

    return None


if __name__ == "__main__":
    main()
