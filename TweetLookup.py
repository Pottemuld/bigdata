import requests
import json
from kafka import KafkaConsumer

bearer_token = 'AAAAAAAAAAAAAAAAAAAAANnZKQEAAAAA1dCBWQ6qyYCmG2LBdDTvBZ%2FUHpE%3Dx6SELuPPaUUVgNsF4YGYxEgtXQkwhoUskKTyQMQ3teqXOgOxCo'
headers = {"Authorization": "Bearer " + bearer_token}

consumer = KafkaConsumer(
   'csv_test', #Change topic name
    enable_auto_commit=True,
    group_id='my-group-1',
    bootstrap_servers=['localhost:9092'])


base_request = 'https://api.twitter.com/2/tweets/'
expansion_location = '?expansions=geo.place_id&place.fields=contained_within,country,country_code,full_name,geo,id,name,place_type'

for m in consumer:
    tweet = json.loads(m)
    tweet_id = tweet['tweet_id']
    response = requests.get(base_request + tweet_id + expansion_location + '', headers=headers)

    print(response.json())

