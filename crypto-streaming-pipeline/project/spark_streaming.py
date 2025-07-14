from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CryptoToCassandra") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.jars.packages", 
           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
           "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
    .getOrCreate()

# Define input schema
schema = StructType([
    StructField("id", StringType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("price_usd", DoubleType(), nullable=False),
    StructField("ts", LongType(), nullable=False)
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto-data") \
    .option("startingOffsets", "latest") \
    .load()

# Process data with strict validation
parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select(
    col("data.id"),
    col("data.name"),
    col("data.price_usd"),
    col("data.ts")
).filter(
    (col("id").isNotNull()) & 
    (col("name").isNotNull()) & 
    (col("price_usd").isNotNull()) & 
    (col("ts").isNotNull())
).withColumn(
    "processing_time", (col("ts") / 1000).cast("timestamp")
)

# Debug output
debug_query = parsed_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

# Write to Cassandra
cassandra_query = parsed_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "crypto") \
    .option("table", "prices") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

cassandra_query.awaitTermination()