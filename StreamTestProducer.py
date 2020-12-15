from kafka import KafkaProducer
import time

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'])
while(True):
    location = 'unknown'
    message = {'date' : '2020-12-15', 'Odense, DK'}
    self.producer.send("tweet.date-location.source", bytearray(json.dumps(message), encoding='utf-8'))
    self.producer.flush()
    time.sleep(1)
