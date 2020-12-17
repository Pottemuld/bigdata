#!/usr/bin/python3
import datetime as date
import CsvProducer
import sys
from kafka import KafkaConsumer

class CsvMainRunner():
    def __init__(self, start_day = date.date(2020, 10, 15), end_day = date.date(2020, 10,13)):
        self
        self.start_day = start_day
        self.end_day = end_day 
        self.producer = CsvProducer.CsvProducer()
        self.dateprcecced = []

        self.consumer = KafkaConsumer(
            'tweet-lookup.dateStarted.refrence',
             enable_auto_commit=True,
             group_id='my-group-1',
             bootstrap_servers=['localhost:9092'])

    def check_date(self, date):
        if date in self.dateprcecced:
            return False
        else:
            print('appending day:'+date)
            self.dateprcecced.append(date)
            return True



    def run(self):
        #write first day to topic
        self.producer.readFile(self.start_day.isoformat())

        for m in self.consumer:
            recived = str(m.value, 'utf-8')
            if self.check_date(recived):
                print('recived this date: '+ recived)
                dateArray_y_m_d = str(recived).split('-')
                current_day = date.date(int(dateArray_y_m_d[0]), int(dateArray_y_m_d[1]), int(dateArray_y_m_d[2]))
                nextDay = current_day - date.timedelta(days= 1)
                if nextDay >= self.end_day:
                    self.producer.readFile(nextDay.isoformat())
                else:
                    sys.exit(0)


if __name__ == "__main__":
    main = CsvMainRunner()
    main.run()