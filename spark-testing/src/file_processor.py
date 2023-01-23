from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *


def convertCase(col_str: str):
    return col_str.upper()


convertCaseUDF = udf(lambda x : convertCase(x), StringType())


def load_data(spark: SparkSession, file_path: str) -> DataFrame:
    spark_df = spark.read.csv(path=file_path, inferSchema=True, header=True)
    return spark_df


def write_data(df: DataFrame, destination_path:str) -> None:
    df.write.csv(path=destination_path, header=True)


def convert_column_case(df: DataFrame) -> DataFrame:
    return df.select(list(map(convertCaseUDF, map(col, df.columns))))


def find_the_late_flights(df: DataFrame) -> DataFrame:
    late_arriving_flights_df = df.where("ArrDelay> 0").groupBy("UniqueCarrier")\
        .count().where("count>0").orderBy(desc("count"))
    return late_arriving_flights_df


def enrich_with_flight_desc(flight_arrival_df: DataFrame, flight_desc_df: DataFrame) -> DataFrame:
    late_flight_df = flight_arrival_df.join(flight_desc_df, flight_arrival_df.UniqueCarrier == flight_desc_df.Code)
    return late_flight_df