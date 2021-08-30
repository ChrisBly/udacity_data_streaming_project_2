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
    # solution_trigger_variation.py
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "police_department_calls_3") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 200) \
        .option("maxRatePerPartition", 200) \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    # https://knowledge.udacity.com/questions/457255
    # Batches are empty   
    kafka_df = df.selectExpr("CAST(value as string)")
   
    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("SERVICE_CALLS"))\
        .select("SERVICE_CALLS.*")
    
    # TODO select original_crime_type_name and disposition
    # https://knowledge.udacity.com/questions/349349
    # Correct duplicate original_crime_type_namein schema.
    # remove watermark.
    distinct_table = service_table\
        .select('original_crime_type_name','disposition','call_date_time')\
        .distinct()

    # count the number of original crime type
    # solution_trigger_variation.py
    # Error with psf.window, corrected using
    # https://knowledge.udacity.com/questions/457255
    agg_df = distinct_table \
        .withWatermark("call_date_time", "60 minutes") \
        .groupBy(
            psf.window(distinct_table.call_date_time, "10 minutes", "5 minutes"),
            distinct_table.original_crime_type_name
            ).count()
    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    # solution_trigger_variation.py
    # Remove trigger
    # # play around with processingTime to see how the progress report changes
    # https://dzone.com/articles/spark-trigger-options
    query = agg_df \
        .writeStream \
        .trigger(processingTime="20 seconds") \
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
    # empty output, change to left join
    join_query = agg_df\
        .join(radio_code_df, col('agg_df.disposition') == col('radio_code_df.disposition'), 'left')\
        .writeStream \
        .format("console") \
        .queryName("main_query") \
        .start()


    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    # https://knowledge.udacity.com/questions/246389
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()
    
    # https://knowledge.udacity.com/questions/348819
    spark.sparkContext.setLogLevel('WARN')
    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
