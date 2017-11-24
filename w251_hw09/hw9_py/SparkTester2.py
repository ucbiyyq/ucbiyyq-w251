from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
from pyspark import SparkConf

'''
See
* http://www.awesomestats.in/spark-twitter-stream/
'''

conf = SparkConf()
conf.setMaster("spark://spark-2-1:7077")
conf.setAppName("hw09 tester2")
sc = SparkContext(conf=conf)

print(sc)

ssc = StreamingContext(sc, 10 )
sqlContext = SQLContext(sc)
ssc.checkpoint("file:///root/hw9/data/checkpoint")

socket_stream = ssc.socketTextStream("spark-2-1", 5555)

lines = socket_stream.window( 20 )

from collections import namedtuple
fields = ("tag", "count" )
Tweet = namedtuple( 'Tweet', fields )

( lines.flatMap( lambda text: text.split( " " ) )
  .filter( lambda word: word.lower().startswith("#") )
  .map( lambda word: ( word.lower(), 1 ) )
  .reduceByKey( lambda a, b: a + b )
  .map( lambda rec: Tweet( rec[0], rec[1] ) )
  .foreachRDD( lambda rdd: rdd.toDF().sort( desc("count") )
              .limit(10).registerTempTable("tweets") ) )
              
              
print(sqlContext)

ssc.start()             # Start the computation

import time

count = 0
while count < 10:
    time.sleep( 20 )
    top_10_tweets = sqlContext.sql( 'Select tag, count from tweets' )
    top_10_df = top_10_tweets.toPandas()
    print(top_10_df.shape)
    count = count + 1


ssc.stop()