#!/usr/bin/python3
import csv
import json
from kafka import KafkaProducer
class CsvProducer:
    def __init__(self):
        self
        self.producer = KafkaProducer(
        bootstrap_servers=['10.123.252.210:9092'])

    def readFile(self, date):
        #filepath = "hdfs://node-master:9000/data/dailies/" + date + "/" + date + "_clean-dataset.tsv"
        filepath = "/home/hadoop/dailies/" + date + "/" + date + "_clean-dataset.tsv"

        with open(filepath, 'r') as file:
            csv_file = csv.DictReader(file, delimiter = '\t')
            for row in csv_file:
                json_object =json.dumps(dict(row), indent=4)
                print(json_object)
                #self.producer.send('tweet.dataset.source', bytes(json_object, encoding='utf-8'))
                #self.producer.flush()

if __name__ == "__main__":
    producer = CsvProducer()
    producer.readFile('2020-10-12')