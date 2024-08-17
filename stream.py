import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType,DoubleType
from pyspark.sql.functions import concat, lit

spark = SparkSession.builder \
    .appName("Stream Player Data") \
    .getOrCreate()

# Define the schema for the Kafka data
payload_after_schema = StructType([
    StructField("index", IntegerType(), True),
    StructField("Section", StringType(), True),
    StructField("Mar 2013", DoubleType(), True),
    StructField("Mar 2014", DoubleType(), True),
    StructField("Mar 2015", DoubleType(), True),
    StructField("Mar 2016", DoubleType(), True),
    StructField("Mar 2017", DoubleType(), True),
    StructField("Mar 2018", DoubleType(), True),
    StructField("Mar 2019", DoubleType(), True),
    StructField("Mar 2020", DoubleType(), True),
    StructField("Mar 2021", DoubleType(), True),
    StructField("Mar 2022", DoubleType(), True),
    StructField("Mar 2023", DoubleType(), True),
    StructField("Mar 2024", DoubleType(), True),
])

payload_schema = StructType([
    StructField("after", payload_after_schema, True)
])


message_schema = StructType([
    StructField("payload", payload_schema, True)
])


df1 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "rel.public.profit_loss_data") \
    .option("includeHeaders", "true") \
    .load()

print("Schema of raw Kafka data:")
df1.printSchema()


raw_df = df1.selectExpr("CAST(value AS STRING) as json_value")


print("Printing raw JSON values from Kafka:")
query1 = raw_df.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .start()


parsed_df = raw_df.select(from_json(col("json_value"), message_schema).alias("data")) \
    .select("data.payload.after.*")


print("Printing parsed data after applying schema:")
query2 = parsed_df.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .start()


transformed_df = parsed_df.withColumn("bouns", concat(lit('rel'), col('Section')))


print("Printing transformed data to verify the transformation:")
query3 = transformed_df.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .start()


query4 = transformed_df.selectExpr( "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "new_topic") \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .start()

spark.streams.awaitAnyTermination()

