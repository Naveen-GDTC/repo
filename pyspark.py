from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType,LongType,IntegerType
import psycopg2

spark = SparkSession.builder \
    .appName("ETL Profit and Loss") \
    .getOrCreate()
 
profit_loss_schema =StructType([    
    StructField("index", LongType(), True),          
    StructField("year", StringType(), True),         
    StructField("sales", LongType(), True),          
    StructField("expenses", LongType(), True),       
    StructField("operating_profit", LongType(), True),  
    StructField("opm_percent", IntegerType(), True),  
    StructField("other_income", LongType(), True),   
    StructField("interest", LongType(), True),       
    StructField("depreciation", LongType(), True),   
    StructField("profit_before_tax", LongType(), True),  
    StructField("tax_percent", IntegerType(), True),  
    StructField("net_profit", LongType(), True),     
    StructField("eps_in_rs", DoubleType(), True),    
    StructField("dividend_payout_percent", IntegerType(), True),  
    StructField("stock", StringType(), True)         
])

before_index = StructType([
    StructField("index", LongType(), True)
])
 
payload_schema = StructType([
    StructField("before", before_index, True),
    StructField("after", profit_loss_schema, True),
    StructField("op", StringType(), True)
])
 

message_schema = StructType([
    StructField("payload", payload_schema, True)
])

 

df1 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "rel.public.profit_loss_data") \
    .option("startingOffsets", "earliest") \
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
   .select(col("data.payload.before.index").alias("before_index"),
       col("data.payload.after.index").alias("index"),
        col("data.payload.after.year").alias("year"),
        col("data.payload.after.sales").alias("sales"),
        col("data.payload.after.expenses").alias("expenses"),
        col("data.payload.after.operating_profit").alias("operating_profit"),
        col("data.payload.after.opm_percent").alias("opm_percent"),
        col("data.payload.after.other_income").alias("other_income"),
        col("data.payload.after.interest").alias("interest"),
        col("data.payload.after.depreciation").alias("depreciation"),
        col("data.payload.after.profit_before_tax").alias("profit_before_tax"),
        col("data.payload.after.tax_percent").alias("tax_percent"),
        col("data.payload.after.net_profit").alias("net_profit"),
        col("data.payload.after.eps_in_rs").alias("eps_in_rs"),
        col("data.payload.after.dividend_payout_percent").alias("dividend_payout_percent"),
        col("data.payload.after.stock").alias("stock"),
        col("data.payload.op").alias("operation"))

#Simple transformation
transformed_df = parsed_df.withColumn("sales_expen_diff",col("sales")-col("expenses"))

 
query2 = parsed_df.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .start()


##############################################################################################################################
jdbc_url = "jdbc:postgresql://host.docker.internal:5432/reliance"
connection_properties = {
    "user": "docker",
    "password": "docker",
    "driver": "org.postgresql.Driver"
}


def write_to_postgres(df, epoch_id):
    inserts = df.filter(df.operation == "c")
    updates = df.filter(df.operation == "u")
    deletes = df.filter(df.operation == "d")

    if not inserts.rdd.isEmpty():
        inserts.write.jdbc(url=jdbc_url, table="sink_profit_loss", mode="append", properties=connection_properties)

    if not updates.rdd.isEmpty():
        conn = psycopg2.connect(
            dbname="reliance",
            user="docker",
            password="docker",
            host="192.168.1.188",
            port="5432"
        )
        cursor = conn.cursor()
        index = updates.select("index")
        index_values = index.collect()
        index_tuple = tuple([row["index"] for row in index_values])
        cursor.execute(f"alter table sink_profit_loss replica identity full;")
        if len(index_tuple) == 1:
            cursor.execute(f"delete from sink_profit_loss where index = {index_tuple[0]}")
        else:    
            cursor.execute(f"DELETE FROM sink_profit_loss WHERE index in {index_tuple}")
        conn.commit()
        cursor.close()
        conn.close()

        updates.write.jdbc(url=jdbc_url, table="sink_profit_loss", mode="append", properties=connection_properties)


    if not deletes.rdd.isEmpty():

        conn = psycopg2.connect(
            dbname="reliance",
            user="docker",
            password="docker",
            host="192.168.1.188",
            port="5432"
        )
        cursor = conn.cursor()
        index = deletes.select("before_index")
        index_values = index.collect()
        index_tuple = tuple([row["before_index"] for row in index_values])
        cursor.execute(f"alter table sink_profit_loss replica identity full;")
        if len(index_tuple) == 1:
            cursor.execute(f"delete from sink_profit_loss where index = {index_tuple[0]};")
        else:    
            cursor.execute(f"DELETE FROM sink_profit_loss WHERE index in {index_tuple};")
        conn.commit()
        cursor.close()
        conn.close()
            




#####################################################################################################################

query = transformed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .start()

spark.streams.awaitAnyTermination()
