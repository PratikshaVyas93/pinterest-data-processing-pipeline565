from itertools import count
from kafka import KafkaConsumer
import json
from json import loads, dumps, dump
import boto3
import os

class Batch_Processing_Data:
    def __init__(self):
        """
            To initialise the constructor
        """
        self.s3 = boto3.resource('s3')
        
    def create_consumer(self):
        """
            This Method create kafka consumer to retrive the message from producer
        """
        # Create Consumer to retrieve the messages from the topic
        self.batch_pinterest_consumer = KafkaConsumer(
            bootstrap_servers = "localhost:9092",
            value_deserializer = lambda messagepinterest: loads(messagepinterest),
            #ensures messages are read from the beggining
            auto_offset_reset = "earliest")

    def subscribe_consumer(self):
        """
            This Method subscribe the message from created topic
        """
        self.batch_pinterest_consumer.subscribe(topics = "KafkaPinterestTopic")
        print("batch_pinterest_consumer",self.batch_pinterest_consumer)
        
    def consumer_to_s3(self):
        """
            This Method upload all the topic message to S3. 
            it will loop till 60 records and then it will break the loop
        """
        count = 0
        for _ , message in enumerate(self.batch_pinterest_consumer):
            message = message.value
            obj = self.s3.Object('pinterest-data-pipeline', f'pinterestdata/message_{_}.json')
            obj.put(Body=(bytes(json.dumps(message).encode('UTF-8'))))
            count += 1
            if count >= 60:
                break 

    def run_create_consumer(self):
        """
            This Method first runs the consumer and then subcribe the consumer message.
        """
        self.create_consumer()
        self.subscribe_consumer()

    def dump_data_to_s3_batch(self):
        """
            This method help us to upload data on S3.
        """
        self.consumer_to_s3()

if __name__ == "__main__":
    obj_bpd = Batch_Processing_Data()
    obj_bpd.run_create_consumer()
    obj_bpd.dump_data_to_s3_batch()