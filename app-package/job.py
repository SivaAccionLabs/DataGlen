"""
Name:       Job.py
Purpose:    Application entry point to create, configure and start spark streaming job.
Author:     Siva
Created:    07/03/2019
"""

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window

LOG_LEVEL_ERROR = 0
LOG_LEVEL_INFO = 1
LOG_LEVEL_DEBUG = 2
APP_LOG_LEVEL = LOG_LEVEL_INFO


def log_level_name(level):
    if level == LOG_LEVEL_DEBUG:
        return 'DEBUG'
    elif level == LOG_LEVEL_INFO:
        return 'INFO'
    else:
        return 'ERROR'

def log_level_value(level):
    if level == 2:
        return 'DEBUG'
    elif level == 1:
        return 'INFO'
    else:
        return 'ERROR'

def log_out(msg_level, message):
    if APP_LOG_LEVEL >= msg_level:
        print '%s %s %s' % (str(datetime.now()),
                            log_level_name(msg_level), message)

def main():
    global APP_LOG_LEVEL

    #
    # Create the spark context
    #
    log_out(LOG_LEVEL_INFO, 'Creating spark context')
    spark = SparkSession.builder
                        .appName(app_name)
                        .getOrCreate()

    #
    # Define the kafka data input for streaming applicatioin
    #
    events = spark.readStream.format("kafka") \
                             .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
                             .option("subscribe", "dataglen") \
                             .load()

    #
    # Kafka events schema
    #
    schema = StructType(Seq(StructField("key", StringType, true),
                            StructField("value", StringType, true),
                            StructField("timestamp", StringType, true),
                       ))
    
    #
    # Introduce a Watermark in order to handle late arriving data
    #
    data_stream_transformed = data_stream.withWatermark("timestamp", "1 day")

    #
    # Extract events out of "value" column and then parse the JSON content into a table
    #
    data_stream_transformed = data_stream_transformed.selectExpr("CAST(value AS STRING) as json") \
                                                     .select(from_json(col("json"), schema=schema)
                                                             .as("data")) \
                                                     .selectExpr("data.key",
                                                                 "cast (data.value as integer)",
                                                                 "data.timestamp")
    #
    # Compute sum and mean of values received in 2 mins
    #
    data_stream_transformed = data_stream_transformed.groupBy(
        window(data_stream_transformed.timestamp,
               "2 minutes",
               "30 seconds"),
        data_stream_transformed.key).agg(
               sum("value"),
               mean("value"))

    #
    # Prints the output to the console/stdout
    #
    query = data_stream_transformed.writeStream.outputMode('complete') \
                                   .format('console') \
                                   .option('truncate', 'false')

    #
    # Start the streaming execution
    #
    log_out(LOG_LEVEL_INFO, 'Starting spark streaming execution')
    query.start().awaitTermination()

if __name__ == "__main__":
    main()
