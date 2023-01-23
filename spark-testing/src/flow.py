from typing import Dict
from pyspark.sql import SparkSession

from file_processor import load_data, write_data, find_the_late_flights, enrich_with_flight_desc


def execute(spark: SparkSession, args: Dict):
    input_df = load_data(spark, args.input_file_location)

    late_flights_df = find_the_late_flights(input_df)

    carrier_desc_df = load_data(spark, args.carrier_description_file_location)

    late_flights_enriched_df = enrich_with_flight_desc(late_flights_df, carrier_desc_df)
    
    write_data(late_flights_enriched_df, args.output_file_location)

