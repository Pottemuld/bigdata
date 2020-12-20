import psycopg2
import pandas as pd
import matplotlib.pyplot as plt


dates = []
locations = []
count = []
try:
    connection = psycopg2.connect(user="postgres", password="Test", host="localhost", port="5432", database="bigdata")
    cursor = connection.cursor()
    query = "SELECT date, location, count FROM data WHERE location NOT IN ('Unknown', 'unknown') ORDER BY count DESC;"

    cursor.execute(query)

    data = cursor.fetchall()
    for row in data:
        dates.append(row[0])
        locations.append(row[1])
        count.append(row[2])
        count_series={"count": count}

    dict = {'Location': locations[:15], 'Count': count[:15]}
    df = pd.DataFrame(dict)
    df.plot(kind='bar', x='Location', y='Count')
    plt.subplots_adjust(top=0.88, bottom=0.3, left=0.11, right=0.9, hspace=0.2, wspace=0.2)

    plt.show()

    print("Plot")

except(Exception, psycopg2.Error) as error:
    print(error)
finally:
    if(connection):
        cursor.close()
        connection.close()
        print("Connection closed")