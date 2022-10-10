from kafka import KafkaConsumer
from json import loads

class Stream_Processing_Data():
   def __init__(self):
      pass
   def create_stream_consumer(self):
      self.stream_pinterest_consumer = KafkaConsumer(
         bootstrap_servers = "localhost:9092",
         value_deserializer = lambda messagepinterest: loads(messagepinterest),
         #ensures messages are read from the beggining
         auto_offset_reset = "earliest")  

   def subscribe_stream_consumer(self):
      self.stream_pinterest_consumer.subscribe(topics = "KafkaPinterestTopic")
      print("stream_pinterest_consumer", self.stream_pinterest_consumer)
      for message in self.stream_pinterest_consumer:
         print(message)

   def run_stream_create_consumer(self):
        self.create_stream_consumer()
        self.subscribe_stream_consumer()

if __name__ == "__main__":
    obj_spd = Stream_Processing_Data()
    obj_spd.run_stream_create_consumer()