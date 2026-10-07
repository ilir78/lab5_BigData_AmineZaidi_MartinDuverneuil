from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum, count, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

spark = SparkSession.builder \
    .appName("WikiStreamsProcessing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()


raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "wikistreams") \
    .option("startingOffsets", "latest") \
    .load()


length_schema = StructType([
    StructField("old", IntegerType()),
    StructField("new", IntegerType())
])
wiki_schema = StructType([
    StructField("server_name", StringType()),
    StructField("title", StringType()),
    StructField("user", StringType()),
    StructField("timestamp", LongType()),
    StructField("length", length_schema)
])

parsed_stream = raw_stream.select(from_json(col("value").cast("string"), wiki_schema).alias("data")).select("data.*")

enriched_stream = parsed_stream \
    .withColumn("edit_size", col("length.new") - col("length.old")) \
    .withColumn("event_time", from_unixtime(col("timestamp")).cast("timestamp"))


tumbling_windowed = enriched_stream.groupBy(
    window(col("event_time"), "5 minutes")
).agg(sum("edit_size").alias("total_edit_size"))


sliding_windowed = enriched_stream.groupBy(
    window(col("event_time"), "10 minutes", "2 minutes")
).agg(count("*").alias("total_edits"))

query = tumbling_windowed.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()