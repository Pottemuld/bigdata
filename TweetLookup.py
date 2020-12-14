import requests
import json
import time
from kafka import KafkaConsumer, KafkaProducer

class TweetLookup:
    def __init__(self):
        self
        self.bearer_token2 = 'AAAAAAAAAAAAAAAAAAAAANnZKQEAAAAA1dCBWQ6qyYCmG2LBdDTvBZ%2FUHpE%3Dx6SELuPPaUUVgNsF4YGYxEgtXQkwhoUskKTyQMQ3teqXOgOxCo'
        self.bearer_token = 'AAAAAAAAAAAAAAAAAAAAAPLZKQEAAAAAu7d%2F9pJlC2fbDNjDFKDhhjH8rTc%3DoHfa0tcrR5BoQwl9Ml87Jt3DeC6N6nmtG0RfILM8dLuZCyjZTo'

        # for handling the twitter 900 request/15 min restraint
        self.count_token1 = 0
        self.count_token2 = 0
        self.time_token1= 0
        self.time_token2 =0

        self.currently_prosccesing_day = 0

        self.consumer = KafkaConsumer(
           'tweet.dataset.source',
            enable_auto_commit=True,
            group_id='my-group-1',
            bootstrap_servers=['localhost:9092'])

        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'])

        self.base_request = 'https://api.twitter.com/2/tweets/'
        self.expansion_author = '?expansions=author_id'
        self.expansion_location = '?expansions=geo.place_id&place.fields=contained_within,country,country_code,full_name,geo,id,name,place_type'
    
    def getHeader(self):
        if (self.count_token1 < 300):
            print('count1= '+str(self.count_token1))
            if self.time_token1 == 0:
                self.time_token1 = time.time()
            self.count_token1 += 1
            headers = {"Authorization": "Bearer " + self.bearer_token}
            return headers
        elif (self.count_token2 < 300):
            print('count2= '+str(self.count_token2))
            self.time_token2 == 0
            self.time_token2 = time.time()
            self.count_token2 += 1
            headers = {"Authorization": "Bearer " + self.bearer_token2}
            return headers
        elif (int(time.time() - self.time_token1) > 900):
            print('resetting 1')
            self.count_token1 = 0
            self.time_token1 = 0
            return self.getHeader()
        elif (int(time.time() - self.time_token2) > 900):
            print('resetting 1')
            self.count_token2 = 0
            self.time_token2 = 0
            return self.getHeader()
        else:
            print('into wait part')
            if self.time_token1 < self.time_token2:
                print('waiting for 1')
                time.sleep(int (self.time_token1+905)-time.time())
            else:
                print('waitng for 2')
                time.sleep(int (self.time_token2+905)- time.time())
            return self.getHeader()
        



    def run(self):
        for m in self.consumer:
            tweet = json.loads(m.value)
            #update day started kafka topic
            if tweet['date'] != self.currently_prosccesing_day:
                self.currently_prosccesing_day = tweet['date']
                print(self.currently_prosccesing_day)
                self.producer.send("tweet-lookup.dateStarted.refrence", bytearray(self.currently_prosccesing_day, encoding='utf-8'))
                self.producer.flush()

            tweet_id = tweet['tweet_id']

            #lookup location if given in tweet
            if tweet['country_code'] != 'NULL':
                #tweet_id = '1315140365503156225'
                response_tweet = requests.get(self.base_request + tweet_id + self.expansion_location + '', headers=self.getHeader()).json()

                if not 'errors' in response_tweet:
                    location = response_tweet['includes']['places'][0]['country_code']
                    
                    #send to topic
                    message = {'date' : tweet['date'], 'location' : location}
                    self.producer.send("tweet.date-location.source", bytearray(json.dumps(message), encoding='utf-8'))
                    self.producer.flush()

            ## lookup location from user
            else:
                response_tweet = requests.get(self.base_request + tweet_id + self.expansion_author + '', headers=self.getHeader()).json()
                if not 'errors' in response_tweet:        
                    author_id = response_tweet['data']['author_id']
                    response_user = requests.get('https://api.twitter.com/2/users/' + author_id + '?user.fields=id,location,name', headers=self.getHeader()).json()
                    
                    #if user have provided location
                    if 'location' in response_user['data']:
                        #print(response_user['data']['location'])
                        #send to topic
                        location = response_user['data']['location']
                        message = {'date' : tweet['date'], 'location' : location}
                        self.producer.send("tweet.date-location.source", bytearray(json.dumps(message), encoding='utf-8'))
                        self.producer.flush()
                    # if no user provided location then set location to unknown
                    else:
                        #Send unknown to topic 
                        location = 'unknown'
                        message = {'date' : tweet['date'], 'location' : location}
                        self.producer.send("tweet.date-location.source", bytearray(json.dumps(message), encoding='utf-8'))
                        self.producer.flush()

    

if __name__ == "__main__":
    lookup = TweetLookup()
    lookup.run()