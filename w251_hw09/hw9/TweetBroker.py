from tweepy import StreamListener
from tweepy import OAuthHandler
from tweepy import API
from tweepy import Stream
import socket

'''
Connects to Twitter app and sends tweets to a Spark Streaming app.

Run on terminal 1
# python TweetBroker.py

Is safe to kill with ctrl-c as soon as the Spark Streaming app stops its spark streaming context.
'''

consumer_key = "ccwtl2crmKH0p30pHocmcvouh"
consumer_secret = "gMjli4yVo2Bf3kM3ejHez1vDtB5AqK7r2HTqk9pvx9GNEF7F9n"
access_token = "1414214346-SIvjGchjh09A6r3YIZzTNSaj0LOBF3kqJL6syrW"
access_token_secret = "2EHxskNPxczHxsjmrqiA1C1peydSjNxf9kHiwPqwSUqZ0"
my_port = 5555
my_host = "spark-2-1"
my_socket_backlog = 5

class MyStreamListener(StreamListener):
    
    def __init__(self, csocket):
        self.client_socket = csocket
    
    def on_data(self, data):
        # tries to send the tweet text to spark through our socket
        try:
            print("%s ..." % data[:60])
            self.client_socket.send(data.encode("utf-8"))
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True
    
    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream, for quick-quit on rate limiting
            return False
    
    
def main():

    # creates a socket that we'll be using later
    s = socket.socket()     # Creates a socket object
    s.bind((my_host, my_port))    # Binds to the host & port of our spark master
    
    # connects to spark master and waits for the streaming app to start
    print("Listening on port: %s" % str(my_port))
    s.listen(my_socket_backlog) # Now waiting for client connection (stream app to start)
    c_socket, addr = s.accept()        # Establish connection with client.
    print("Received request from: " + str(addr))
    
    # creates authen for our twitter app
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = API(auth)
    
    # creates a stream listner that is attached to our socket
    myStreamListener = MyStreamListener(c_socket)
    twitter_stream = Stream(auth = api.auth, listener=myStreamListener)
    
    # kicks off the stream to sample from twitter
    twitter_stream.sample()
    
    
    
if __name__ == "__main__":
    main()