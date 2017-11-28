from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json

'''
Test for Spark Stream to be used with a Tweet Tester, to see if we can parse the tweet json from the twitter stream (not just the text)
Consumes the tweets sampled by the Tweet Tester app.

On Terminal 1 run,
$ TweetTester5-StreamingTweepy.py

On Terminal 2 run,
$ SparkTester5-StreamingTweepy.py

as the tweet tester streams tweets into the spark streaming app, we should see some counts

-------------------------------------------
Time: 2017-11-25 13:58:50
-------------------------------------------
('??', 1)
('????', 1)
('????', 1)
('MissUniverse', 1)
('THAILAND', 1)
('CODWWII', 1)
('fitgays', 1)
('HappyThanksgiving', 1)
('PojokSatu', 1)
('seleb', 1)
...

See 
* http://www.awesomestats.in/spark-twitter-stream/
* https://spark.apache.org/docs/latest/streaming-programming-guide.html
'''

my_port = 5555
my_host = "spark-2-1"
my_batch_interval = 1 #batch interval of seconds
my_app_name = "NetworkWordCount"
my_spark_master = "local[2]" #local StreamingContext with two working threads


def main():

    def get_users_counts(tweets):
        def get_user(tweet):
            tweet_json = json.loads(tweet)
            author = tweet_json.get("user")
            author_screen_name = ""
            if author is not None:
                author_screen_name = author.get("screen_name")
            return author_screen_name
            
        # gets the users from all the tweets
        users = tweets.map(get_user)        
        # counts each user in each batch
        users_pairs = users.map(lambda user: (user, 1))
        users_counts = users_pairs.reduceByKey(lambda x, y: x + y)
        return users_counts
    
    def get_topics_counts(tweets):
        def get_topics(tweet):
            tweet_json = json.loads(tweet)
            entities = tweet_json.get("entities")
            simple_hashtags = []
            if entities is not None:
                hashtags = entities.get("hashtags")
                if hashtags is not None:
                    for tag in hashtags:
                        simple_hashtags.append(tag.get("text"))
            return simple_hashtags
            
        # transforms the stream of tweets into a stream of topics
        topics = tweets.flatMap(get_topics)
        # counts each topic in each batch
        topics_pairs = topics.map(lambda topic: (topic, 1))
        topics_counts = topics_pairs.reduceByKey(lambda x, y: x + y)
        #return topics
        topics_counts_desc = topics_counts.transform( lambda topics_count: topics_count.sortBy(lambda x: x[1], ascending=False))
        return topics_counts_desc
        
    # Creates a local StreamingContext
    sc = SparkContext(my_spark_master, my_app_name)
    ssc = StreamingContext(sc, my_batch_interval)

    # Creates a DStream that holds a chain of RDDs that hold our tweets
    tweets = ssc.socketTextStream(my_host, my_port)
    
    # Gets the prints some user counts
    #users_counts = get_users_counts(tweets)
    #users_counts.pprint()
    
    # Gets and prints some topic counts
    topics_counts = get_topics_counts(tweets)
    topics_counts.pprint()

    ssc.start()             # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate


if __name__ == "__main__":
    main()