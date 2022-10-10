from itertools import count
from kafka import KafkaConsumer
import json
from json import loads, dumps, dump
import boto3
import os

class Batch_Processing_Data:
    def __init__(self):
        self.s3 = boto3.resource('s3')
        
    def create_consumer(self):
        # Create Consumer to retrieve the messages from the topic
        self.batch_pinterest_consumer = KafkaConsumer(
            bootstrap_servers = "localhost:9092",
            value_deserializer = lambda messagepinterest: loads(messagepinterest),
            #ensures messages are read from the beggining
            auto_offset_reset = "earliest")

    def subscribe_consumer(self):
        self.batch_pinterest_consumer.subscribe(topics = "KafkaPinterestTopic")
        print("batch_pinterest_consumer",self.batch_pinterest_consumer)
        
    def consumer_to_s3(self):
        for _ , message in enumerate(self.batch_pinterest_consumer):
            message = message.value
            obj = self.s3.Object('pinterest-data-pipeline', f'pinterestdata/message_{_}.json')
            obj.put(Body=(bytes(json.dumps(message).encode('UTF-8'))))

    def run_create_consumer(self):
        self.create_consumer()
        self.subscribe_consumer()

    def dump_data_to_s3_batch(self):
        self.consumer_to_s3()

if __name__ == "__main__":
    obj_bpd = Batch_Processing_Data()
    obj_bpd.run_create_consumer()
    obj_bpd.dump_data_to_s3_batch()



# for message in batch_pinterest_consumer:
#     print(message)


# s3_client = boto3.client('s3')
# bucket_name = "pinterest-data-pipeline"

# #Send data to Consumer
# count = 0
# for message in batch_pinterest_consumer:
#     json_data = dumps(message.value)
#     foldername = 'batchdata'
#     S3_filename = str(count) + '.json'
#     s3_client.upload_file(foldername,bucket_name,S3_filename )
#     #pin_bucket.(Key = S3_filename, Body = json_data)
#     count += 1
# print(f"Total {count} record/s pushed on S3 bucket.")





# client = boto3.client('s3')
# bucket_name = 'pinterest-data-pipeline'
# counts= 0
# for message in batch_pinterest_consumer:
#     folder_name = {message.value['unique_id']}
#     with open(f"{folder_name}.json", "w") as f:
#             json.dump(message.value, f)
#     client.upload_file(f'{folder_name}.json', bucket_name, f'{folder_name}/batchdata_{counts}.json')
#     counts += 1
#     os.remove(f"{folder_name}.json")