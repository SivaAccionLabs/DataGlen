# Steps to run the job #

1. Upload the jar files which are there in libs direcotry into HDFS

	hadoop fs -put *.jars /app_packages
        
	or

	hdfs dfs -put .jars /app-packages

2. Run the job as below

	./bin/spark-submit --master yarn --jars hdfs:///app_packages/kafka-clients-0.8.2.2.jar,hdfs:///app_packages/spark-streaming-kafka_2.10-1.6.0.jar,hdfs:///app_packages/kafka_2.10-0.8.2.2.jar,hdfs:///app_packages/metrics-core-2.2.0.jar
