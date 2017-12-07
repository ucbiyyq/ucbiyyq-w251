from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import explode
from pyspark.sql.functions import col
from pyspark.sql.functions import dense_rank
from pyspark.sql.functions import collect_set
from pyspark.sql.window import Window
from pyspark.streaming import StreamingContext
import json

'''
Test for Spark Stream to be used with a Tweet Tester, to see if we can persist each rdd of the twitter stream, so that we can compute the final stats as well as the interval stats
Consumes the tweets sampled by the Tweet Tester app.

On Terminal 1 run,
# python TweetTester12-CleanPersist.py

On Terminal 2 run,
# $SPARK_HOME/bin/spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.3 SparkTester12-CleanPersist.py

See 
* https://spark.apache.org/docs/latest/streaming-programming-guide.html
* https://databricks.com/blog/2016/07/28/structured-streaming-in-apache-spark.html
* https://stackoverflow.com/questions/35093336/how-to-stop-spark-streaming-when-the-data-source-has-run-out
'''

my_port = 5555
my_host = "spark-2-1"
my_app_name = "hw9_py_test"
my_spark_master = "local[2]" #local StreamingContext with two working threads
my_spark_singleton_name = "sparkSessionSingletonInstance"
my_batch_interval = 5 #batch sampling interval, seconds
my_top_n_topics = 3 #top n most frequently-occurring hashtags among all tweets during the sampling period, or at each sampling period
my_total_run_time = 15 #script run duration, seconds



def calc_stats(spark):
    '''
    helper function that when given a spark session or a sql context, calculates the following stats:
    1 top topics
    2 top users
    3 top mentions
    
    note, assumes that the Spark SQL views topics, users, and mentions already exist
    '''
    # finds the top topics for this mini-batch, using Spark SQL
    qry = ( "WITH topics_count AS (SELECT topic, count(*) as num_tweets FROM topics GROUP BY topic)"
            ", topics_ranked AS (SELECT topic, num_tweets, dense_rank() over (order by num_tweets desc, topic asc) as num_tweets_rnk FROM topics_count)"
            " SELECT * FROM topics_ranked")
    topics_ranked = spark.sql(qry)
    top_3_topics_ranked = topics_ranked.filter(col("num_tweets_rnk") <= my_top_n_topics)
    top_3_topics_ranked.createOrReplaceTempView("top_3_topics_ranked")
    top_3_topics_ranked.persist()
    
    # finds the authors of popular topics, using SQL
    qry = ( "WITH topics_users AS (SELECT ta.user, tb.topic FROM users ta JOIN topics tb ON tb.id = ta.id)"
            ", top_topics_users AS (SELECT ta.user, ta.topic FROM topics_users ta JOIN top_3_topics_ranked tb ON tb.topic = ta.topic)"
            ", top_topics_users_list AS (SELECT user, collect_set(topic) as top_topics_of_user FROM top_topics_users GROUP BY user)"
            " SELECT * FROM top_topics_users_list")
    top_topics_users_list = spark.sql(qry)
    
    # finds the mentions from tweets with popular topics, using SQL
    qry = ( "WITH top_topics_tweets AS (SELECT ta.id, ta.topic FROM topics ta JOIN top_3_topics_ranked tb ON tb.topic = ta.topic)"
            ", top_topics_mentions AS (SELECT ta.mention, tb.topic FROM mentions ta JOIN top_topics_tweets tb ON tb.id = ta.id)"
            ", top_topics_mentions_list AS (SELECT mention, collect_set(topic) as top_topics_of_mention FROM top_topics_mentions GROUP BY mention)"
            " SELECT * FROM top_topics_mentions_list")
    top_topics_mentions_list = spark.sql(qry)
    
    # show all the result dataframes
    top_3_topics_ranked.show(truncate=False)
    top_topics_users_list.show(truncate=False)
    top_topics_mentions_list.show(truncate=False)




def getSparkSessionInstance(sparkConf):
    '''
    helper function that creates a singleton of the spark session
    '''
    if (my_spark_singleton_name not in globals()):
        globals()[my_spark_singleton_name] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()[my_spark_singleton_name]

    
def main():
    conf = SparkConf()
    conf.setMaster(my_spark_master)
    conf.setAppName(my_app_name)
    conf.set("spark.streaming.stopGracefullyOnShutdown","true")
    conf.set("spark.cassandra.connection.connections_per_executor_max", "2")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("OFF")
    ssc = StreamingContext(sc, my_batch_interval)
    
    def setup(sc):
        '''
        helper function that cleans up past cassandra data. Assumes the following tables exists in Cassandra:
        * streaming.hw9_topics
        * streaming.hw9_users
        * streaming.hw9_mentions
        
        DOESN'T SEEM TO WORK!!! Maybe should have used postgres instead of fancy-pants nosql cassandra
        '''
        # sqlContext = SQLContext(sc)
        # cmd = "TRUNCATE TABLE streaming.hw9_topics"
        # sqlContext.sql(cmd)
        # cmd = "TRUNCATE TABLE streaming.hw9_users"
        # sqlContext.sql(cmd)
        # cmd = "TRUNCATE TABLE streaming.hw9_mentions"
        # sqlContext.sql(cmd)
    
    # cleans up the cassandra data from past runs of this script
    setup(sc)
    
    # gets stream of tweets from the Twitter broker
    tweets = ssc.socketTextStream(my_host, int(my_port))
    
    def process(time, rdd):
        '''
        helper function that procesees the mini-batch for each sample interval (RDD), including:
        * persisting the mini-batch to cassandra
        * calling another helper to calculate the stats for this mini-batch
        '''
        print("========= %s =========" % str(time))
        print("processing %s records in this mini-batch" % str(rdd.count()))
        spark = getSparkSessionInstance(rdd.context.getConf())
        df = spark.read.json(rdd)
        
        # creates a temp view so that we can start getting stats using Spark SQL
        df.createOrReplaceTempView("tweets")
        
        # creates temp views, and persists topics, users, and mentions to Cassandra
        qry = "SELECT id, explode(entities.hashtags.text) as topic FROM tweets WHERE id is not null"
        topics = spark.sql(qry)
        topics.createOrReplaceTempView("topics")
        topics.write.format("org.apache.spark.sql.cassandra").mode('append').options(table="hw9_topics", keyspace="streaming").save()
        
        qry = "SELECT id, user.screen_name as user FROM tweets WHERE id is not null"
        users = spark.sql(qry)
        users.createOrReplaceTempView("users")
        users.write.format("org.apache.spark.sql.cassandra").mode('append').options(table="hw9_users", keyspace="streaming").save()
        
        qry = "SELECT id, explode(entities.user_mentions.screen_name) as mention FROM tweets WHERE id is not null"
        mentions = spark.sql(qry)
        mentions.createOrReplaceTempView("mentions")
        mentions.write.format("org.apache.spark.sql.cassandra").mode('append').options(table="hw9_mentions", keyspace="streaming").save()        
        
        # calls helper function to process the stats for this mini-batch
        calc_stats(spark)
    
    # calculates the stats for each sampling interval
    tweets.foreachRDD(process)
    
    # starts the calculations
    ssc.start()
    
    # waits for the calculations to stop for some time
    ssc.awaitTermination(my_total_run_time)
    
    # stops the streaming gracefully, but not the Spark Session
    ssc.stop(False, True)
    print("========= stopped processing mini-batches =========")
    
    def process_final(sc):
        '''
        helper function that procesees the final stats:
        * connects to cassandra and grabs all the accumulated data
        * calls another helper to calculate the stats based on the accumulated data
        '''
        print("========= final stats =========")
        # uses the spark context to create a sql context so that we can process the accumulated stats from cassandra
        sqlContext = SQLContext(sc)
        topics = sqlContext.read.format("org.apache.spark.sql.cassandra").options(table="hw9_topics", keyspace="streaming").load()
        users = sqlContext.read.format("org.apache.spark.sql.cassandra").options(table="hw9_users", keyspace="streaming").load()
        mentions = sqlContext.read.format("org.apache.spark.sql.cassandra").options(table="hw9_mentions", keyspace="streaming").load()
        topics.createOrReplaceTempView("topics")
        users.createOrReplaceTempView("users")
        mentions.createOrReplaceTempView("mentions")
        
        # calls helper function to process final stats from the sessions
        calc_stats(sqlContext)
        
    process_final(sc)
    
    print("========= finished! =========")
    
if __name__ == "__main__":
    main()