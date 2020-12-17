import psycopg2

try:
    connection = psycopg2.connect(user="postgres", password="Test", host="10.123.252.210", port="5432", database="bigdata")
    cursor = connection.cursor()
    query = "select * from data;"

    cursor.execute(query)

    data = cursor.fetchall()
    for row in data:
        print("Date = ", row[0])
        print("Location = ", row[1])
        print("Count = ", row[2])


except(Exception, psycopg2.Error) as error:
    print(error)
finally:
    if(connection):
        cursor.close()
        connection.close()
        print("Connection closed")