#!/usr/bin/python3
from kafka.admin import KafkaAdminClient, NewTopic
topics = ["tweet.dataset.source", "tweet.date-location.source", "tweet-lookup.dateStarted.refrence"]

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id='test'
)
topic_list = []
for topic in topics:
    topic_list.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)