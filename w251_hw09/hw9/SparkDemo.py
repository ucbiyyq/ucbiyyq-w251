import pyspark
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
import time

'''
Sets up a spark streaming application

See
* https://www.linkedin.com/pulse/apache-spark-streaming-twitter-python-laurent-weichberger/
'''

tweet_interval = 10 # batch interval in seconds
tweet_window = 20 # window in seconds
my_port = 5555
my_host = "spark-2-1"
my_spark_master = "spark://spark-2-1:7077"
my_spark_app_name = "hw09"

def main():

    def get_user(tweet):
        json_tweet = json.loads(tweet)
        simple_user = None
        author = tweet.get("user")
        if author is not None:
            simple_user = author.get("screen_name")
        else:
            simple_user = "unknown"
        return simple_user
    
    # sets up the spark streaming context
    conf = SparkConf()
    conf.setMaster(my_spark_master)
    conf.setAppName(my_spark_app_name)
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, tweet_interval)
    
    # sets up the spark sql context
    sqlContext = SQLContext(sc)
    ssc.checkpoint( "/root/hw9/data/checkpoint") # in case of failure
    
    # connects to the tweet reader to recieve the tweets
    tweets_stream = ssc.socketTextStream(my_host, my_port)
    
    # creates a window; RDD is created every interval=10 seconds, but the data in RDD will be for last 20 seconds
    lines = tweets_stream.window(tweet_window)
    
    # creates a namedtuple to store results
    from collections import namedtuple
    fields = ("tag", "count" )
    Tweet = namedtuple( "Tweet", fields )
    
    # processes the stream
    # ( lines.flatMap( lambda text: text.split( " " ) )
      # .filter( lambda word: word.lower().startswith("#") )
      # .map( lambda word: ( word.lower(), 1 ) )
      # .reduceByKey( lambda a, b: a + b )
      # .map( lambda rec: Tweet( rec[0], rec[1] ) )
      # .foreachRDD( lambda rdd: rdd.toDF().sort( desc("count") )
                  # .limit(10).registerTempTable("tweets") ) )
    
    
    # starts the streaming & calculations
    ssc.start()
    
    
    
    
    #users = tweets.map(get_user)
    #print(users)
    #tweets.foreachRDD( lambda rdd: rdd.coalesce(1).saveAsTextFile("./tweets/%f" % time.time()) )
    #tweets.foreachRDD(lambda rdd: rdd.saveAsTextFile("./data/tweets"))
    #tweets.foreachRDD( lambda rdd: rdd.toDF.write.format("json").mode(SaveMode.Append).saveAsTextFile("./data/tweets.json") )
    
    
    # ends the calculation
    ssc.awaitTermination()
    
    
    
if __name__ == "__main__":
    main()