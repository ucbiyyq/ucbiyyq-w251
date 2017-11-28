from pyspark import SparkContext
from pyspark.streaming import StreamingContext

'''
Basic test for Spark Stream to be used with a Tweet Tester, to see if we can parse the twitter stream just like we did with the netcat example.
Consumes the tweets sampled by the Tweet Tester app.

On Terminal 1 run,
$ TweetTester4-BasicStreamingTweepy.py

On Terminal 2 run,
$ SparkTester4-BasicStreamingTweepy.py

as the tweet tester streams tweet text into the spark streaming app, we should see some counts

-------------------------------------------
Time: 2017-11-24 14:55:33
-------------------------------------------
('https://t.co/UsLEjlLRNFRT', 1)
('Drop', 1)
('is', 3)
('K-pop', 1)
('group', 1)
('#1', 1)
('latest', 1)
('#Portsmouth,', 1)
('click', 1)
('SAP', 1)
...

-------------------------------------------
Time: 2017-11-24 15:02:12
-------------------------------------------
('#NotAlınBunuRT', 1)
('@ms_hudgins:', 1)
('Ryan', 1)
('was', 3)
('trying', 1)
('trooper,', 1)
('he', 1)
('this', 4)
('lady’s', 1)
('yard', 1)
...

See 
* http://www.awesomestats.in/spark-twitter-stream/
* https://spark.apache.org/docs/latest/streaming-programming-guide.html
'''

my_port = 5555
my_host = "spark-2-1"

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)

# Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream(my_host, my_port)

# Split each line into words
words = lines.flatMap(lambda line: line.split(" "))

# Count each word in each batch
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
