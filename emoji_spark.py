from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, udf
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from kafka import KafkaProducer
import json


# Define Kafka configurations
KAFKA_BROKER = 'localhost:9092'
INPUT_TOPIC = 'emoji_topic'
OUTPUT_TOPIC = 'emoji_highest_topic'
SCALE_DOWN_FACTOR = 10

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_to_kafka(topic, message):
    producer.send(topic, message)

# Spark session setup
spark = SparkSession.builder \
    .appName("EmojiAggregator") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# Define schema for incoming Kafka data
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("emoji_type", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Read data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", INPUT_TOPIC) \
    .load()

# Deserialize Kafka messages
decoded_df = df.selectExpr("CAST(value AS STRING)") \
    .withColumn("jsonData", from_json(col("value"), schema)) \
    .select("jsonData.*")

# Aggregate emoji counts over 2-second windows
aggregated_df = decoded_df \
    .withWatermark("timestamp", "2 seconds") \
    .groupBy(
        window(col("timestamp"), "2 seconds"),
        col("emoji_type")
    ) \
    .agg(count("emoji_type").alias("emoji_count"))

# Scale down emoji counts
@udf(returnType=IntegerType())
def scale_down_count(emoji_count):
    return max(emoji_count // SCALE_DOWN_FACTOR, 1)  # At least 1 emoji

scaled_df = aggregated_df.withColumn("scaled_emoji_count", scale_down_count(col("emoji_count")))

# Write to Kafka and print to terminal
def write_to_kafka(batch_df, batch_id):
    batch_data = batch_df.collect()
    for row in batch_data:
        emoji_message = {
            "emoji_type": row["emoji_type"],
            "scaled_count": row["scaled_emoji_count"]
        }
        # Print to terminal
        print(f"Aggregated Emoji Data: {emoji_message}")
        # Send to Kafka
        send_to_kafka(OUTPUT_TOPIC, emoji_message)

scaled_df.writeStream \
    .foreachBatch(write_to_kafka) \
    .outputMode("update") \
    .start() \
    .awaitTermination()

