#!/usr/bin/python3
import datetime as date
import CsvProducer

class CsvMainRunner():
    def __init__(self, start_day = date.datetime(2020, 10, 15), end_day = date.datetime(2020, 10,13)):
        self
        self.start_day = start_day
        self.end_day = end_day 
        self.producer = CsvProducer.CsvProducer()


    def run(self):
        #write first day to topic
        self.producer.readFile(self.start_day.isoformat)

        





        current_day = self.start_day
        while current_day >= self.end_day:
            print(current_day.isoformat())
            current_day = current_day - date.timedelta(days= 1)


if __name__ == "__main__":
    main = CsvMainRunner()
    main.run()