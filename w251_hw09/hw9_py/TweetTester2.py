import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json

'''
See
* http://www.awesomestats.in/spark-twitter-stream/
'''

consumer_key = "ccwtl2crmKH0p30pHocmcvouh"
consumer_secret = "gMjli4yVo2Bf3kM3ejHez1vDtB5AqK7r2HTqk9pvx9GNEF7F9n"
access_token = "1414214346-SIvjGchjh09A6r3YIZzTNSaj0LOBF3kqJL6syrW"
access_secret = "2EHxskNPxczHxsjmrqiA1C1peydSjNxf9kHiwPqwSUqZ0"

class TweetsListener(StreamListener):

  def __init__(self, csocket):
      self.client_socket = csocket

  def on_data(self, data):
      try:
          msg = json.loads( data )
          print( msg['text'][:10].encode('utf-8') )
          self.client_socket.send( msg['text'].encode('utf-8') )
          return True
      except BaseException as e:
          print("Error on_data: %s" % str(e))
      return True

  def on_error(self, status):
      print(status)
      return True

def sendData(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    
    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=['trump'])

if __name__ == "__main__":
    s = socket.socket()         # Create a socket object
    host = "spark-2-1"      # Get local machine name
    port = 5555                 # Reserve a port for your service.
    s.bind((host, port))        # Bind to the port
    
    print("Listening on port: %s" % str(port))
    
    s.listen(5)                 # Now wait for client connection.
    c, addr = s.accept()        # Establish connection with client.
    
    print( "Received request from: " + str( addr ) )
    
    sendData( c )