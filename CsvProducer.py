#!/usr/bin/python3
import csv
import json
from kafka import KafkaProducer
class CsvProducer:
    def __init__(self, date = "2020-10-12"):
        self
        self.date = date
        self.producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'])

    def readFile(self):
        filepath = "data/dailies/" + self.date + "/" +self.date + "_clean-dataset.tsv"

        with open(filepath, 'r') as file:
            csv_file = csv.DictReader(file, delimiter = '\t')
            for row in csv_file:
                json_object =json.dumps(dict(row), indent=4)
                self.producer.send("csv_test", bytearray(json_object, encoding='utf-8'))
                self.producer.flush()

if __name__ == "__main__":
    producer = CsvProducer()
    producer.readFile()