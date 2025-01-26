from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.functions import from_json
import logging
from pyspark import SparkContext

# Initialize Spark
spark = SparkSession.builder.appName("ChangeLoggingLevel").getOrCreate()

# Dynamically change log level
log4j_logger = spark._jvm.org.apache.log4j
logger = log4j_logger.LogManager.getLogger("org.apache.kafka.clients.consumer.internals.SubscriptionState")
logger.setLevel(log4j_logger.Level.WARN)

print("Log level changed to WARN")

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("KafkaToFolderStream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .getOrCreate()


# Define Kafka configurations
kafka_brokers = "172.18.0.4:9092"  # Replace with your Kafka broker(s)
kafka_topic = "kafka_1"  # Replace with your Kafka topic

# Define schema of the JSON data in Kafka
schema = StructType() \
    .add("id", IntegerType()) \
    .add("age", IntegerType())

# Read from Kafka topic
kafka_stream_1 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.18.0.4:9092") \
    .option("subscribe", "spark_kafka_2") \
    .option("startingOffsets", """{"spark_kafka_2":{"0":-2}}""") \
    .load()

# Read from Kafka topic
kafka_stream_2 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.18.0.4:9092") \
    .option("subscribe", "spark_kafka_3") \
    .option("startingOffsets", """{"spark_kafka_3":{"0":-2}}""") \
    .load()

# Read from Kafka topic
kafka_stream_3 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.18.0.4:9092") \
    .option("subscribe", "spark_kafka_4") \
    .option("startingOffsets", """{"spark_kafka_4":{"0":-2}}""") \
    .load()

# Parse Kafka value as JSON
df_parsed_1 = kafka_stream_1.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json("json", schema).alias("data")) \
    .select("data.*")

# Parse Kafka value as JSON
df_parsed_2 = kafka_stream_2.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json("json", schema).alias("data")) \
    .select("data.*")

# Parse Kafka value as JSON
df_parsed_3 = kafka_stream_3.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json("json", schema).alias("data")) \
    .select("data.*")


wirte_stream_df = df_parsed_1.join(df_parsed_2, 'id').join(df_parsed_3, 'id').select(df_parsed_1.id, df_parsed_2.id, df_parsed_3.id, df_parsed_1.age, df_parsed_2.age, df_parsed_3.age)

# Write stream to folder in Parquet format
#query = wirte_stream_df.writeStream.format("csv") \
#    .option("checkpointLocation", "opt/checkpoint") \
#    .option("path", "opt/stream_result") \
#    .start()

query = wirte_stream_df.writeStream.format("console").start()

# Await termination
query.awaitTermination()

#docker exec -it pyspark-local spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 /opt/spark-app/pyspark_test.py