#!/usr/bin/python
# -*- coding: utf-8 -*-
# spark_main.py
# Author: James Nguyen, W251 UC Berkeley MIDS



import time
import json
import os
import sys
from pyspark import SparkContext
from pyspark import SparkConf

os.environ[‘SPARK_HOME’] = ‘/usr/local/spark’
# sys.path.append(“/usr/local/spark/python”)

from pyspark.streaming import StreamingContext

#number of most popular topics mentioned during the program running duration
n = 6
#short windows duration to sample data in seconds
short_windows = 120
#slide interval of the windows/sampling frequency
slide_interval_short = 10
#Long windows duration to sample data in seconds
long_windows = 1800
#slide interval of the windows/sampling frequency
slide_interval_long = 10



def get_hashtags(tweet):

    user, mentions, hashtags =‘na’,‘na’, [‘na’]
    json_tweet = json.loads(tweet)
    if json_tweet.has_key(‘entities’):  # When the lang key was not present it caused issues
        if json_tweet[‘entities’].has_key(‘hashtags’):
            hashtags=[hashtag[‘text’] for hashtag in json_tweet[‘entities’][‘hashtags’]]
    if json_tweet.has_key(‘user’):  # When the lang key was not present it caused issues
        user= [json_tweet[‘user’][‘name’]]
    if json_tweet.has_key(‘entities’):  # When the lang key was not present it caused issues
        if json_tweet[‘entities’].has_key(‘user_mentions’):
            mentions = [mention[‘name’] for mention in json_tweet[‘entities’][‘user_mentions’]]
    return [(hashtag, (user, mentions)) for hashtag in hashtags]
    #     return (hashtags, (“user1", “hello”))


# hashtags_count_sorted.pprint()
def print_result(rdd):
    records = rdd.take(n)
    print(“**********************************************“)
    print u’{0} hostest current topics over the last {1} minutes’.format(n, short_windows/60)
    print(“----------------------------------------------“)
    print(“----------------------------------------------“)

    for i in range(n):
        print i+1, u”.Topic: {0} ,Number of tweets: {1}“.format(records[i][0],records[i][1])
        print “Tweeting people: “,
        for author in records[i][2]:print u”{0},“.format(author),
        print
        print “Mentioned people: “,
        for mention in records[i][3]: print u”{0},“.format(mention),
        print



# hashtags_count_sorted.pprint()
def print_long_result(rdd):
    records = rdd.take(n)
    print(“**********************************************“)
    print u’Top {0} common topics right over the last {1} minutes’.format(n, long_windows/60)
    print(“----------------------------------------------“)
    print(“----------------------------------------------“)

    for j in range(n):

        print j+1,u”. Topic: {0} ,Number of tweets: {1}“.format(records[j][0],records[j][1])
        print “Tweeting people: “,
        for author in records[j][2]:print u”{0},“.format(author),
        print
        print “Mentioned people: “,
        for mention in records[j][3]: print u”{0},“.format(mention),
        print




if __name__ == “__main__“:
    try:
        n = int(sys.argv[1])
        cluster =str(sys.argv[2])
    except:
        print(‘invalid n passed in’)

    conf = SparkConf()
    conf.setMaster(cluster)
    conf.setAppName(‘testSpark’)
    sc = SparkContext(conf=conf)

    ssc = StreamingContext(sc, 10)  # 10 is the batch interval in seconds
    IP = “spark1”
    Port = 5555
    tweets = ssc.socketTextStream(IP, Port)


    # Short tweets

    short_tweets = tweets.window(short_windows, slide_interval_short)
    hashtags = short_tweets.flatMap(get_hashtags).filter(lambda (k,v): k!=‘na’)
    hashtags_pairs = hashtags.map(lambda (k,v): (k, (1, v)))
    hashtags_count = hashtags_pairs.reduceByKey(lambda x, y: (x[0] + y[0], (x[1][0]+y[1][0], x[1][1]+y[1][1])))
    hashtags_count_clean = hashtags_count.map(lambda x: (x[0], x[1][0],set(x[1][1][0]), set(x[1][1][1])))
    hashtags_count_sorted = hashtags_count_clean.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))

    #Long tweet

    long_tweets = tweets.window(long_windows, slide_interval_long)
    long_hashtags = long_tweets.flatMap(get_hashtags).filter(lambda (k,v): k!=‘na’)
    # hashtags.pprint()
    long_hashtags_pairs = long_hashtags.map(lambda (k,v): (k, (1, v)))
    long_hashtags_count = long_hashtags_pairs.reduceByKey(lambda x, y: (x[0] + y[0], (x[1][0]+y[1][0], x[1][1]+y[1][1])))
    long_hashtags_count_clean = long_hashtags_count.map(lambda x: (x[0], x[1][0],set(x[1][1][0]), set(x[1][1][1])))
    # hashtags_join = hashtags_count.join(hashtags)
    long_hashtags_count_sorted = long_hashtags_count_clean.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))


    long_hashtags_count_sorted.foreachRDD(print_long_result)
    hashtags_count_sorted.foreachRDD(print_result)

    ssc.start()
    ssc.awaitTermination()
