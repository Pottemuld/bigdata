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
expansion_author = '?expansions=author_id'
expansion_location = '?expansions=geo.place_id&place.fields=contained_within,country,country_code,full_name,geo,id,name,place_type'

#for m in consumer:
#tweet = json.loads(m.value)
tweet_id = '1315140365503156225'
response_tweet = requests.get(base_request + tweet_id + expansion_author + '', headers=headers).json()
author_id = response_tweet['data']['author_id']
response_user = requests.get('https://api.twitter.com/2/users/' + author_id + '?user.fields=id,location,name', headers=headers).json()

print(response_user)


