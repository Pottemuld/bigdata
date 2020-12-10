from kafka import KafkaConsumer

consumer = KafkaConsumer(
   'csv_test',
    enable_auto_commit=True,
    group_id='my-group-1',
    bootstrap_servers=['localhost:9092'])

for m in consumer:
    print(m.value)