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

# Define schema of the JSON data in Kafka
schema = StructType() \
    .add("id", IntegerType()) \
    .add("age", IntegerType())

# Read from Kafka topic
kafka_stream_1 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.18.0.4:9092") \
    .option("subscribe", "spark_kafka_1") \
    .option("startingOffsets", """{"spark_kafka_1":{"0":-2}}""") \
    .load()

# Read from Kafka topic
kafka_stream_2 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.18.0.4:9092") \
    .option("subscribe", "spark_kafka_2") \
    .option("startingOffsets", """{"spark_kafka_2":{"0":-2}}""") \
    .load()

# Read from Kafka topic
kafka_stream_3 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.18.0.4:9092") \
    .option("subscribe", "spark_kafka_3") \
    .option("startingOffsets", """{"spark_kafka_3":{"0":-2}}""") \
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


write_stream_df = df_parsed_1.join(df_parsed_2, 'id').join(df_parsed_3, 'id').select(df_parsed_1.id.alias("id1"), df_parsed_2.id.alias("id2"), df_parsed_3.id.alias("id3"), df_parsed_1.age.alias("age1"), df_parsed_2.age.alias("age2"), df_parsed_3.age.alias("value"))

# Write stream to folder in Parquet format
#query = wirte_stream_df.writeStream.format("csv") \
#    .option("checkpointLocation", "opt/checkpoint") \
#    .option("path", "opt/stream_result") \
#    .start()

#query = write_stream_df.writeStream.format("console").start()

query = write_stream_df \
    .selectExpr( "CAST(id1 AS STRING)", "CAST(id2 AS STRING)", "CAST(id3 AS STRING)", "CAST(age1 AS STRING)", "CAST(age2 AS STRING)", "CAST(value AS STRING)" ) \
    .writeStream \
    .format("kafka") \
    .option("checkpointLocation", "opt/checkpoint") \
    .option("kafka.bootstrap.servers", "172.18.0.4:9092") \
    .option("topic", "spark_write_stream") \
    .start()
print(query.explain())
# Await termination
query.awaitTermination()

#docker exec -it pyspark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark-app/pyspark_test.py
#docker cp pyspark_test.py pyspark:/opt/spark-app