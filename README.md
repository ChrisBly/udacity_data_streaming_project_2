# udacity_data_streaming_project_2
## SF Crime Statistics with Spark Streaming

Workspace Environment:

##### Modify the zookeeper.properties and producer.properties given to suit your topic and port number of your choice

- zookeeper-server-start config/zookeeper.properties
- kafka-server-start  config/server.properties


### Step 1

##### The first step is to build a simple Kafka server

Refer to the compelete code in producer_server.py and kafka_server.py

UDACity Workspace Environment

run: python kafka_server.py


start kafka-consumer-console: kafka-console-consumer --bootstrap-server localhost:9092 --topic police_department_calls_3 --from-beginning


Take a screenshot of your kafka-consumer-console output. You will need to include this screenshot as part of your project submission.

- Refer to Snip20210830_40.png

### Step 2

Implement all the TODO items in data_stream.py:

run: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 --master local[*] data_stream.py

#### Take a screenshot of your progress reporter after executing a Spark job. You will need to include this screenshot as part of your project submission.

- Refer to progress_report_1_Snip20210901_62.png
- Refer to progress_report_2_Snip20210901_63.png

#### Take a screenshot of the Spark Streaming UI as the streaming continues. You will need to include this screenshot as part of your project submission.

- Refer to spark_UI_Snip20210901_52.png
- Refer to spark_UI_Snip20210901_56.png

### Step 3

1: How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

When starting the maxOffsetsPerTrigger at 200 and maxOffsetsPerTrigger  at 200, this warning message appears:  WARN  ProcessingTimeExecutor:66 - Current batch is falling behind. The trigger interval is 20000 milliseconds, but spent 85827 milliseconds.
Looking at the results of the progress report and there limit the number of records to fetch per trigger.  

There is a relationship between maxOffsetsPerTrigger and MaxRatePerPartition,  if the MaxRatePerPartition is set lower than the actual rate per second times batch window you'll be always lagging, which is happening with the current settings. After adjusting the maxOffsetsPerTrigger to 120 and maxRatePerPartition at 220,  the throught increased and became stable. 

2: What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?


Looking at the Progress Report results, through multi changes in the parameters above and the Spark UI.
Using these three keys piece of information from the reports:

- numInputRows
-inputRowsPerSecond
- processedRowsPerSecond


Plus looking batchId,  timestamp which provide very useful information regarding the batch size and processed rows during the changing of the parameters in the data streaming code.

Also researching the apache spark performance Tuning page, the three most useful settings are:
spark.streaming.kafka.maxRatePerPartition.  
This allows the maximize the throughput ( processedRowsPerSecond )of the data streaming.  
Spark.sql.shuffle.partitions: As we are grouping the data, this configuration allows the number of partitions to use when shuffling data for joins or aggregations.

Referencing used:

https://spark.apache.org/docs/latest/sql-performance-tuning.html
https://spark.apache.org/docs/2.1.2/api/python/_modules/pyspark/sql/types.html
https://knowledge.udacity.com/questions/457255
https://knowledge.udacity.com/questions/349349
https://knowledge.udacity.com/questions/457255
https://dzone.com/articles/spark-trigger-options
https://knowledge.udacity.com/questions/63367
https://knowledge.udacity.com/questions/335656
https://stackoverflow.com/questions/35788697/leader-not-available-kafka-in-console-producer
https://knowledge.udacity.com/questions/341561
https://knowledge.udacity.com/questions/681381
https://knowledge.udacity.com/questions/673778
