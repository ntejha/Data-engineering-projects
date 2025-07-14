from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

spark = SparkSession.builder \
    .appName("CryptoDataQuality") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.jars.packages", 
           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
           "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
    .getOrCreate()


schema = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("price_usd", DoubleType(), True),
    StructField("ts", LongType(), False)
])


df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto-data") \
    .load()


processed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*").withColumn(
    "processing_time", (col("ts")/1000).cast("timestamp")
).withColumn(
    "price_usd", 
    when(col("price_usd").isNull(), lit(0.0))  
    .otherwise(col("price_usd"))
).filter(
    (col("id").isNotNull()) &  
    (col("name").isNotNull()) &
    (col("ts").isNotNull()) &
    (col("price_usd") >= 0)  
)

query = processed_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "crypto") \
    .option("table", "prices") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

query.awaitTermination()