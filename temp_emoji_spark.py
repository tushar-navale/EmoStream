from pyspark.sql import SparkSession
from pyspark.sql.functions import window, count
# Start Spark session
# spark = SparkSession.builder.appName("EmojiAggregation").config("spark.executor.instances", "8").config("spark.executor.cores", "4").config("spark.executor.memory", "4g").config("spark.sql.shuffle.partitions", "200").config("spark.sql.autoBroadcastJoinThreshold", "-1").getOrCreate()
spark = SparkSession.builder.appName("EmojiAggregation").config("spark.executor.instances", "16").config("spark.executor.cores", "8").config("spark.executor.memory", "8g").config("spark.sql.shuffle.partitions", "400").config("spark.sql.autoBroadcastJoinThreshold", "10MB").getOrCreate()
from pyspark.sql.functions import window, count, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import from_json

# spark = SparkSession.builder.appName("EmojiAggregation").config("spark.executor.instances", "8").config("spark.executor.cores", "4").config("spark.executor.memory", "4g").config("spark.sql.shuffle.partitions", "200").config("spark.sql.autoBroadcastJoinThreshold", "-1").config("spark.streaming.kafka.maxRatePerPartition", "1000").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "emoji_topic").load()

kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "emoji_topic"

df = df.selectExpr("CAST(value AS STRING)")

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafka_bootstrap_servers).option("subscribe", kafka_topic).option("maxOffsetsPerTrigger", 1000).load()


from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

df = df.selectExpr("CAST(value AS STRING)")

schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("emoji_type", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])


df = df.withColumn("json_data", from_json(col("value"), schema)).select("json_data.*")

emoji_counts_combined = df.groupBy(window("timestamp", "2 seconds")).count()

query = emoji_counts_combined.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", "50") \
    .start()
query.awaitTermination()

query.awaitTermination()