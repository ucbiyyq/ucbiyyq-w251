import socket
import time
import datetime
import json
from json import JSONEncoder
import tweepy
from tweepy import StreamListener
from tweepy import OAuthHandler
from tweepy import API
from tweepy import Stream


'''
Connects to twitter stream, and then sends them to the spark master

see
* http://docs.tweepy.org/en/v3.4.0/streaming_how_to.html
* https://www.linkedin.com/pulse/apache-spark-streaming-twitter-python-laurent-weichberger/
* http://www.awesomestats.in/spark-twitter-stream/
'''

consumer_key = "ccwtl2crmKH0p30pHocmcvouh"
consumer_secret = "gMjli4yVo2Bf3kM3ejHez1vDtB5AqK7r2HTqk9pvx9GNEF7F9n"
access_token = "1414214346-SIvjGchjh09A6r3YIZzTNSaj0LOBF3kqJL6syrW"
access_token_secret = "2EHxskNPxczHxsjmrqiA1C1peydSjNxf9kHiwPqwSUqZ0"
my_port = 5555
my_host = "spark-2-1"
my_socket_backlog = 5



class MyTweetsListener(StreamListener):
    
    def __init__(self, csocket):
        self.client_socket = csocket
    
    def on_data(self, data):
        '''
        Upon recieving tweet data, represented as a json string, parses out the useful information and sends it to spark
        '''
        
        def get_topics(tweet):
            simple_hashtags = []
            entities = tweet.get("entities")
            if entities is not None:
                hashtags = entities.get("hashtags")
                if hashtags is not None:
                    for tag in hashtags:
                        simple_hashtags.append(tag.get("text"))
            return simple_hashtags
        
        def get_mentions(tweet):
            simple_mentions = []
            #gets the list of mentions and parses out the screen names for each, then appends to users list
            entities = tweet.get("entities")
            if entities is not None:
                user_mentions = entities.get("user_mentions")
                if user_mentions is not None:
                    for user in user_mentions:
                        simple_mentions.append(user.get("screen_name"))
            return simple_mentions
        
        def get_user(tweet):
            simple_user = None
            author = tweet.get("user")
            if author is not None:
                simple_user = author.get("screen_name")
            return simple_user
        
        #loads the tweet data string into a dict so we can do some parsing
        tweet = json.loads(data)
        
        # parses the tweet for only the useful data
        simple_tweet = {}
        simple_tweet["id"] = tweet.get("id")
        simple_tweet["user"] = get_user(tweet)
        simple_tweet["mentions"] = get_mentions(tweet)
        simple_tweet["topics"] = get_topics(tweet)
        simple_tweet["timestamp"] = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        simple_tweet["text"] = tweet.get("text")
        
        # tries to send it to spark through our socket
        try:
            #self.client_socket.send(data)
            #if tweet has no useful data, i.e. id is null, then discard, otherwise send to spark
            tweet_id = simple_tweet.get("id")
            if tweet_id is not None:
                #temp = str(simple_tweet).encode("utf-8")
                #temp = str(simple_tweet["id"]).encode("utf-8")
                #temp = simple_tweet["user"].encode("utf-8")
                #temp = "{binky:'winky'}".encode("utf-8")
                temp = simple_tweet["text"].encode("utf-8")
                print(temp)
                self.client_socket.send(temp)
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True
    
    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream, for quick-quit on rate limiting
            return False
        else:
            print(status)
            return True
    
    
    
    
def main():
    s = socket.socket()     # Creates a socket object
    s.bind((my_host, my_port))    # Binds to the host & port of our spark master
    print("Listening on port: %s" % str(my_port))
    s.listen(my_socket_backlog) # Now waiting for client connection.
    c_socket, addr = s.accept()        # Establish connection with client.
    print("Received request from: " + str(addr))
    
    # gets authen
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = API(auth)
    
    # instantiates a tweets listener & tweets stream
    stream_listener = MyTweetsListener(c_socket)
    twitter_stream = Stream(auth = api.auth, listener=stream_listener)
    
    # sets the tweets stream to sample from all tweets
    twitter_stream.sample()
    
    
    
if __name__ == "__main__":
    main()