from kafka import KafkaProducer
import time
import json

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'])
while(True):
    location = 'unknown'
    message = {'date' : '2020-12-15', "location": 'Odense, DK'}
    producer.send("tweet.date-location.source", bytearray(json.dumps(message), encoding='utf-8'))
    producer.flush()
    time.sleep(1)
