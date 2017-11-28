import tweepy
import time
import datetime

'''
Basic test for Tweepy, to see if we can grab anthing from Twitter.
Prints a filtered and truncated version of a tweet

See 
* http://www.awesomestats.in/spark-twitter-stream/
'''

consumer_key = "ccwtl2crmKH0p30pHocmcvouh"
consumer_secret = "gMjli4yVo2Bf3kM3ejHez1vDtB5AqK7r2HTqk9pvx9GNEF7F9n"
access_token = "1414214346-SIvjGchjh09A6r3YIZzTNSaj0LOBF3kqJL6syrW"
access_token_secret = "2EHxskNPxczHxsjmrqiA1C1peydSjNxf9kHiwPqwSUqZ0"
topics = ['python'] #filters twitter stream keywords

class MyStreamListener(tweepy.StreamListener):
    
    def on_status(self, status):
        
        def get_topics(hastags):
            simple_tags = []
            for tag in hastags:
                simple_tags.append(tag.get("text"))
            return simple_tags
        
        def get_users(user, user_mentions):
            simple_mentions = [user.screen_name]
            for mention in user_mentions:
                simple_mentions.append(mention.get("screen_name"))
            return simple_mentions
        
        simple = {}
        simple["id"] = status._json["id"]
        simple["users"] = get_users(status.user, status._json["entities"]["user_mentions"])
        simple["topics"] = get_topics(status._json["entities"]["hashtags"])
        #simple["timestamp"] = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        #simple["text"] = status.text
        print(status)
    
    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream, for quick-quit on rate limiting
            return False
    
    
def main():
    # gets authen
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    
    # creates a stream
    myStreamListener = MyStreamListener()
    twitter_stream = tweepy.Stream(auth = api.auth, listener=myStreamListener)
    
    # sets the twitter feed filter
    #twitter_stream.filter(track=topics)
    twitter_stream.sample()
    
    
    
if __name__ == "__main__":
    main()