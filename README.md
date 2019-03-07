# DataGlen
Spark Structured Streaming app to extract JSON messages from kafka broker and aggregate them over a window of 2 mins.

Job does the following things

	1. Define the kafka as a data input for the streaming applicatioin.
	2. Ingest the data by reading latest messages from the "dataglen" topic on the Kafka broker.
	3. Introduce a Watermark in order to handle late arriving data.
	4. Extract events out of "value" column and then parse the JSON content into a table.
	5. Compute sum and mean of values received in 2 mins.
	6. Prints the output to the console/stdout.
