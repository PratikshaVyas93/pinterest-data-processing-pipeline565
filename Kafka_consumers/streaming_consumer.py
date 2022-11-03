from kafka import KafkaConsumer
from json import loads
import os
from pyspark.sql import SparkSession
import findspark
findspark.init(os.environ.get('SPARK_HOME'))
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.2,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 streaming_consumer.py pyspark-shell"
import sqlalchemy
from sqlalchemy import create_engine
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic_name = 'KafkaPinterestTopic'
HOST = os.environ.get('HOST')
DBAPI = 'psycopg2'
USER = os.environ.get('USER_POSTGRES')
PASSWORD = os.environ.get('PASSWORD')
PORT = 5432
DATABASE = 'postgres'
DATABASE_TYPE = 'postgresql'
class Stream_Processing_Data():
   def __init__(self):
      self.spark = SparkSession \
            .builder \
            .appName("KafkaStreaming") \
            .getOrCreate()
      # Only display Error messages in the console.
      self.spark.sparkContext.setLogLevel("ERROR")

   def create_stream_consumer(self):
      self.stream_pinterest_consumer = KafkaConsumer(
         bootstrap_servers = "localhost:9092",
         value_deserializer = lambda messagepinterest: loads(messagepinterest),
         #ensures messages are read from the beggining
         auto_offset_reset = "earliest")  

   def subscribe_stream_consumer(self):
      self.stream_pinterest_consumer.subscribe(topics = "KafkaPinterestTopic")
      print("stream_pinterest_consumer", self.stream_pinterest_consumer)
      # for message in self.stream_pinterest_consumer:
      #    print(message)

   def run_stream_create_consumer(self):
      self.create_stream_consumer()
      self.subscribe_stream_consumer()
      self.kafkastreaming_spark_streaming()

   def run_stream_process(self):
      print("in run streaming fun")
      # Construct a streaming DataFrame that reads from topic
      self.stream_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", kafka_topic_name) \
            .option("startingOffsets", "earliest") \
            .load()


      base_df = self.stream_df.selectExpr("CAST(value as STRING)", "timestamp")
      base_df.printSchema()
      sample_schema = (
        StructType()
        .add("category", StringType())
        .add("unique_id", StringType())
        .add("title", StringType())
        .add("description", StringType())
        .add("follower_count", IntegerType())
      )
      info_dataframe = base_df.select(
        from_json(col("value"), sample_schema).alias("sample_data_schema"), "timestamp"
      )
      info_df_fin = info_dataframe.select("sample_data_schema.*", "timestamp")
      info_df_fin=info_df_fin.replace({'User Info Error': None}, subset = ['follower_count']) \
                    .replace({'No description available Story format':None}, subset=['description'])\
                    .replace({'No Title Data Available':None}, subset=['title'])
      info_df_fin= info_df_fin.withColumn('follower_count', when(col('follower_count').like('%k'), regexp_replace('follower_count', 'k', '000'))\
            .when(col('follower_count').like('%M'),regexp_replace('follower_count', 'M', '000000'))\
            .when(col('follower_count').like('%B'),regexp_replace('follower_count', 'B', '000000000'))\
            .cast('int'))
    
      info_df_fin = info_df_fin.select([when(col(cl)=='',None).otherwise(col(cl)).alias(cl) for cl in info_df_fin.columns])
     
      #self.save_data_RDS(info_df_fin) 
     
      # # # outputting the messages to the console 
      query = info_df_fin.writeStream \
         .trigger(processingTime="5 seconds") \
         .outputMode("update") \
         .foreachBatch(writeToCassandra) \
         .start()     
      
      #self.stream_df.show(5)
   def writeToCassandra(writeDF, epochId):
      writeDF.write \
         .format("org.apache.spark.sql.cassandra") \
         .options(table="randintstream", keyspace="kafkaspark") \
         .mode("append") \
         .save()
     
      
   def kafkastreaming_spark_streaming(self):
      self.run_stream_process()

   

if __name__ == "__main__":
    obj_spd = Stream_Processing_Data()
    obj_spd.run_stream_create_consumer()
