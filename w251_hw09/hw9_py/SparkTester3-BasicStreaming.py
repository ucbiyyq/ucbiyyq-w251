from pyspark import SparkContext
from pyspark.streaming import StreamingContext

'''
Basic Spark Streaming tutorial.
Counts the number of words in text data received from a data server listening on a TCP socket.
Run SparkTester3-BasicStreaming.py in terminal 2, and in terminal 1, run the nc command.

On Terminal 1 run,
$ nc -lk 9999

then start typing into terminal 1, hit enter to send a line of text

On Terminal 2 run,
$ python SparkTester3-BasicStreaming.py

as we type into terminal 1, terminal 2 should display the word count, e.g.

-------------------------------------------
Time: 2017-11-24 13:51:12
-------------------------------------------
('world', 1)
('this', 1)
('is', 1)
('streaming', 1)
('hello', 1)
('a', 1)
('example', 1)

-------------------------------------------
Time: 2017-11-24 13:52:30
-------------------------------------------
('smell', 2)
('human', 1)
('fi', 4)
('fy', 2)
('I', 2)
('a', 1)



See 
https://spark.apache.org/docs/latest/streaming-programming-guide.html
'''

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)

# Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream("localhost", 9999)

# Split each line into words
words = lines.flatMap(lambda line: line.split(" "))

# Count each word in each batch
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate