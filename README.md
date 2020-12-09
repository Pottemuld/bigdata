1. Start zookeeper: bin/zookeeper-server-start.sh config/zookeeper.properties
2. Start Kafka: bin/kafka-server-start.sh config/server.properties
3. Create topic: bin/kafka-topics.sh --create --topic <topic_name> --bootstrap-server localhost:9092 or run NewTopic.py
4. Run Consumer.py
5. Run Producer.py
