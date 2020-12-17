from kafka import KafkaConsumer

consumer = KafkaConsumer(
   'tweet.date-location.source',
    enable_auto_commit=True,
    group_id='my-group-1',
    bootstrap_servers=['localhost:9092'])

for m in consumer:
    print(m.value)