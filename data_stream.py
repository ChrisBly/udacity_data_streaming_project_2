import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# TODO Create a schema for incoming resources
# https://spark.apache.org/docs/2.1.2/api/python/_modules/pyspark/sql/types.html
schema = StructType([StructField("crime_id", IntegerType(), True),
                     StructField("original_crime_type_name", StringType(), True),
                     StructField("report_date", TimestampType(), True),
                     StructField("call_date", TimestampType(), True),
                     StructField("offense_date", TimestampType(), True),
                     StructField("call_time", TimestampType(), True),
                     StructField("call_date_time", TimestampType(), True),
                     StructField("original_crime_type_name", StringType(), True),
                     StructField("disposition", StringType(), True),
                     StructField("address", StringType(), True),
                     StructField("city", StringType(), True),
                     StructField("state", StringType(), True),
                     StructField("agency_id", IntegerType(), True),
                     StructField("common_location", StringType(), True),
                ])

def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("cast(value as string)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    # TODO select original_crime_type_name and disposition
    # https://knowledge.udacity.com/questions/349349
    distinct_table = service_table\
        .select(
            psf.col("original_crime_type_name"),
            psf.col("call_date_time"),
            psf.col("disposition")
        ).withWatermark("call_date_time","30 minutes")\
        .distinct()

    # count the number of original crime type
    # solution_trigger_variation.py
    agg_df = distinct_table\
        .select(
            "original_crime_type_name",
            "call_date_time",
            "disposition"        
        )\
        .groupBy(psf.col("original_crime_type_name"), psf.col("disposition")
            window(psf.col("call_date_time"), "30 minutes"))
            .count()

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    # solution_trigger_variation.py
    # Remove trigger
    query = agg_df \
        .writeStream \
        .outputMode('Complete') \
        .format('console') \
        .option("truncate", "false") \
        .start()


    # TODO attach a ProgressReporter
    query.awaitTermination()

    # TODO get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # TODO join on disposition column
    # https://knowledge.udacity.com/questions/131619
    join_query = agg_df\
        .join(radio_code_df, col('agg_df.disposition') == col('radio_code_df.disposition'), 'left_outer')


    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
