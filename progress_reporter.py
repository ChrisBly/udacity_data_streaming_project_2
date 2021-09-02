import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf

# https://spark.apache.org/docs/2.1.2/api/python/_modules/pyspark/sql/types.html
schema = StructType([
    StructField("crime_id", StringType(), False),
    StructField("original_crime_type_name", StringType(), False),
    StructField("report_date", DateType(), False),
    StructField("call_date", DateType(), False),
    StructField("offense_date", DateType(), False),
    StructField("call_time", StringType(), False),
    StructField("call_date_time", TimestampType(), False),  # timestamp of event time
    StructField("disposition", StringType(), False),
    StructField("address", StringType(), False),
    StructField("city", StringType(), False),
    StructField("state", StringType(), False),
    StructField("agency_id", StringType(), False),
    StructField("address_type", StringType(), False),
    StructField("common_location", StringType(), False)
])


def run_spark_job(spark):

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "police_department_calls_3") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 120) \
        .option("maxRatePerPartition", 100) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()
    
    kafka_df = df.selectExpr("CAST(value as string)")
    
    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("SERVICE_CALLS"))\
        .select("SERVICE_CALLS.*")
    # Trouble shooting
    # service_table.printSchema()
    # TODO select original_crime_type_name and disposition
    # https://knowledge.udacity.com/questions/349349
    # Correct duplicate original_crime_type_namein schema.
    # remove watermark.
    distinct_table = service_table\
        .select(psf.col("original_crime_type_name"),psf.col("disposition"), psf.col("call_date_time"))\
        .withWatermark('call_date_time', '5 minute')
    
    # agg_df = df.count()
    
        # Trouble shooting
    distinct_table.printSchema()
    # count the number of original crime type
    # solution_trigger_variation.py
    # Error with psf.window, corrected using
    # https://knowledge.udacity.com/questions/457255
    agg_df = distinct_table \
        .groupBy(
            psf.col("original_crime_type_name"),
            psf.window(psf.col("call_date_time"), "10 minutes", "5 minutes")
         )\
         .count()
    # Trouble shooting
    agg_df.printSchema()
    
    # play around with processingTime to see how the progress report changes
    query = agg_df \
        .writeStream \
        .trigger(processingTime="20 seconds") \
        .outputMode('Complete') \
        .format('console') \
        .option("truncate", "false") \
        .start()
        
if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("StructuredStreamingSetup") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
