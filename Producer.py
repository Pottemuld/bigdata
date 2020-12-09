from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'])
while(True):
    str = input("Enter message to be sent to kafka topic: ")
    producer.send("sample", bytearray(str, encoding='utf-8'))
    producer.flush()