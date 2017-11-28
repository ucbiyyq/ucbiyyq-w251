from tweepy import StreamListener
from tweepy import OAuthHandler
from tweepy import API
from tweepy import Stream
import socket

'''
Test for Tweepy to be used with a Spark Streaming app, to see if we can parse the tweet json from the twitter stream (not just the text)
Sends twitter json to the Spark Streaming app.

On Terminal 1 run,
$ python SparkTester9-MoreStreamingSQL.py

then start typing into terminal 1, hit enter to send a line of text

On Terminal 2 run,
$ python SparkTester9-MoreStreamingSQL.py

as the tweet tester streams tweets into the spark streaming app, we should see ...

{"delete":{"status":{"id":935412663085039616,"id_str":"93541 ...
{"delete":{"status":{"id":908366234806255616,"id_str":"90836 ...
{"delete":{"status":{"id":416372949550002176,"id_str":"41637 ...
{"created_at":"Tue Nov 28 07:52:11 +0000 2017","id":93541601 ...
{"delete":{"status":{"id":908371318306873345,"id_str":"90837 ...
{"created_at":"Tue Nov 28 07:52:11 +0000 2017","id":93541601 ...
{"delete":{"status":{"id":833798115702489089,"id_str":"83379 ...
{"delete":{"status":{"id":833972875589586944,"id_str":"83397 ...
{"delete":{"status":{"id":908377068693606400,"id_str":"90837 ...
{"delete":{"status":{"id":908375818786586625,"id_str":"90837 ...
{"delete":{"status":{"id":556101938463260672,"id_str":"55610 ...
{"delete":{"status":{"id":836990022989524992,"id_str":"83699 ...
{"delete":{"status":{"id":838231121745874944,"id_str":"83823 ...
{"delete":{"status":{"id":842406287140876288,"id_str":"84240 ...
{"delete":{"status":{"id":444620433535422464,"id_str":"44462 ...
{"delete":{"status":{"id":908665687140442113,"id_str":"90866 ...
{"delete":{"status":{"id":843654381987942401,"id_str":"84365 ...
{"delete":{"status":{"id":908665271904333824,"id_str":"90866 ...
{"delete":{"status":{"id":445382983160446977,"id_str":"44538 ...
{"delete":{"status":{"id":420383475930103810,"id_str":"42038 ...
{"delete":{"status":{"id":908672767104618502,"id_str":"90867 ...


See 
* https://spark.apache.org/docs/latest/streaming-programming-guide.html
* https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
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