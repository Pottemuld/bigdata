import requests
import json
from kafka import KafkaConsumer, KafkaProducer

bearer_token = 'AAAAAAAAAAAAAAAAAAAAANnZKQEAAAAA1dCBWQ6qyYCmG2LBdDTvBZ%2FUHpE%3Dx6SELuPPaUUVgNsF4YGYxEgtXQkwhoUskKTyQMQ3teqXOgOxCo'
bearer_token2 = 'AAAAAAAAAAAAAAAAAAAAAPLZKQEAAAAAu7d%2F9pJlC2fbDNjDFKDhhjH8rTc%3DoHfa0tcrR5BoQwl9Ml87Jt3DeC6N6nmtG0RfILM8dLuZCyjZTo'
headers = {"Authorization": "Bearer " + bearer_token}

consumer = KafkaConsumer(
   'csv_test', #Change topic name
    enable_auto_commit=True,
    group_id='my-group-1',
    bootstrap_servers=['localhost:9092'])

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'])

base_request = 'https://api.twitter.com/2/tweets/'
expansion_author = '?expansions=author_id'
expansion_location = '?expansions=geo.place_id&place.fields=contained_within,country,country_code,full_name,geo,id,name,place_type'

for m in consumer:
    tweet = json.loads(m.value)
    tweet_id = tweet['tweet_id']
    if tweet['country_code'] != 'NULL':
        #tweet_id = '1315140365503156225'
        response_tweet = requests.get(base_request + tweet_id + expansion_location + '', headers=headers).json()
        
        if not 'errors' in response_tweet:
            location = response_tweet['includes']['places'][0]['country_code']
            #send to topic
            
            message = {'date' : tweet['date'], 'location' : location}
            producer.send("location_stream", bytearray(json.dumps(message), encoding='utf-8'))
            producer.flush()
    
    
#Check for error

#check for geo

#check for user locaion
    else:
        response_tweet = requests.get(base_request + tweet_id + expansion_author + '', headers=headers).json()
        if not 'errors' in response_tweet:        
            author_id = response_tweet['data']['author_id']
            response_user = requests.get('https://api.twitter.com/2/users/' + author_id + '?user.fields=id,location,name', headers=headers).json()
            if 'location' in response_user['data']:
                #print(response_user['data']['location'])
                #send to topic
                location = response_user['data']['location']
                message = {'date' : tweet['date'], 'location' : location}
                producer.send("location_stream", bytearray(json.dumps(message), encoding='utf-8'))
                producer.flush()
            else:
                #Send unknown to topic 
                location = 'unknown'
                message = {'date' : tweet['date'], 'location' : location}
                producer.send("location_stream", bytearray(json.dumps(message), encoding='utf-8'))
                producer.flush()