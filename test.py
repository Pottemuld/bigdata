import datetime as date

day = date.date(2020, 10, 15)
nextDay = day - date.timedelta(days= 1)
printday = nextDay.isoformat()
print(nextDay.isoformat())