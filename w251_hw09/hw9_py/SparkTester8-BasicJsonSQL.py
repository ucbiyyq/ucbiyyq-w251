'''
Test for basic SQL query on JSON, to see if we can parse the tweet json using Spark
Assumes the json data file exists already /root/data/tweet_samples.txt

On Terminal 1 run,
$ python SparkTester8-BasicJsonSQL.py


See 
* https://databricks.com/blog/2015/02/02/an-introduction-to-json-support-in-spark-sql.html
* https://spark.apache.org/docs/latest/sql-programming-guide.html
* http://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html
* http://spark.apache.org/docs/2.2.0/api/python/pyspark.sql.html
* https://stackoverflow.com/questions/38397796/retrieve-top-n-in-each-group-of-a-dataframe-in-pyspark
* https://stackoverflow.com/questions/33516490/column-alias-after-groupby-in-pyspark
* https://stackoverflow.com/questions/42148583/pyspark-filter-dataframe-by-columns-of-another-dataframe
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import col
from pyspark.sql.functions import rank
from pyspark.sql.functions import dense_rank
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import collect_set
from pyspark.sql.window import Window

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    
# creates a df of type: <class 'pyspark.sql.dataframe.DataFrame'>
df = spark.read.json("/root/data/samples/tweet_samples3.json")
#df.show()

# show the schema
#df.printSchema()

# show the id and created timestamp
#df.select(df["id"], df["created_at"]).show()

# show the id and created timestamp using SQL
#df.createOrReplaceTempView("tweets")
#sqlDF = spark.sql("SELECT id, created_at FROM tweets") # <class 'pyspark.sql.dataframe.DataFrame'>
#sqlDF.show()

# show the id, user, and nested topics & mentions
# df.select( df["id"], df["user.screen_name"].alias("user"), df["entities.hashtags.text"].alias("topic"), df["entities.user_mentions.screen_name"].alias("mention") ).show()
# df.select( df["id"], explode(df["entities.hashtags.text"]).alias("topic") ).show()

def using_df():
    # finds the top topics, using DFs
    topics = df.select(explode(df.entities.hashtags.text).alias("topic")).groupby("topic").count().select("topic", col("count").alias("num_tweets")).orderBy(col("num_tweets").asc())
    window = Window.orderBy(topics.num_tweets.desc(), topics.topic.asc())
    topics_ranked = topics.select("*", dense_rank().over(window).alias("num_tweets_rnk"))
    top_3_topics_ranked = topics_ranked.filter(col("num_tweets_rnk") <= 3)
    top_3_topics_ranked.persist()
    top_3_topics_ranked.show()

    # finds the authors of popular topics, using DFs
    topics_users = df.select(df.user.screen_name.alias("user"), explode(df.entities.hashtags.text).alias("topic"))
    cond = [topics_users.topic == top_3_topics_ranked.topic]
    top_topics_users = topics_users.join(top_3_topics_ranked, cond, "inner").select(topics_users.user, topics_users.topic)
    top_topics_users_list = top_topics_users.groupby(top_topics_users.user).agg(collect_set(top_topics_users.topic).alias("top_topics_of_user"))
    top_topics_users_list.show()
    
    # finds the mentions from tweets with popular topics, using DFs
    topics_tweets = df.select(df.id, explode(df.entities.hashtags.text).alias("topic"))
    cond = [topics_tweets.topic == top_3_topics_ranked.topic]
    top_topics_tweets = topics_tweets.join(top_3_topics_ranked, cond, "inner").select(topics_tweets.id, topics_tweets.topic)
    tweet_mentions = df.select(df.id, explode(df.entities.user_mentions.screen_name).alias("mention"))
    cond = [tweet_mentions.id == top_topics_tweets.id]
    top_topics_mentions = tweet_mentions.join(top_topics_tweets, cond, "inner").select(tweet_mentions.mention, top_topics_tweets.topic)
    top_topics_mentions_list = top_topics_mentions.groupby(top_topics_mentions.mention).agg(collect_set(top_topics_mentions.topic).alias("top_topics_of_mention"))
    top_topics_mentions_list.show()
    
#using_df()


# show the id, user, and nested topics & mentions using SQL & DFs
#df.createOrReplaceTempView("tweets")
#sqlDF1 = spark.sql("SELECT id as id1, user.screen_name as user FROM tweets")
#sqlDF2 = spark.sql("SELECT id as id2, explode(entities.hashtags.text) as topic FROM tweets")
#sqlDF3 = spark.sql("SELECT id as id3, explode(entities.user_mentions.screen_name) as mention FROM tweets")
#sqlDF4 = sqlDF1.join(sqlDF2, sqlDF1.id1 == sqlDF2.id2, "left").join(sqlDF3, sqlDF1.id1 == sqlDF3.id3, "left").select(sqlDF1.id1, sqlDF1.user, sqlDF2.topic, sqlDF3.mention) #this doesn't work
#sqlDF4.show()


def using_sql():
    # finds the top topics, using SQL
    df.createOrReplaceTempView("tweets")
    qry = ( "WITH step1 AS (SELECT explode(entities.hashtags.text) as topic FROM tweets)"
            ", topics AS (SELECT topic, count(*) as num_tweets FROM step1 GROUP BY topic)"
            ", topics_ranked AS (SELECT topic, num_tweets, dense_rank() over (order by num_tweets desc, topic asc) as num_tweets_rnk FROM topics)"
            " SELECT * FROM topics_ranked")
    topics_ranked = spark.sql(qry)
    top_3_topics_ranked = topics_ranked.filter(col("num_tweets_rnk") <= 3)
    top_3_topics_ranked.createOrReplaceTempView("top_3_topics_ranked")
    top_3_topics_ranked.persist()
    top_3_topics_ranked.show()
    
    # finds the authors of popular topics, using SQL
    qry = ( "WITH topics_users AS (SELECT user.screen_name as user, explode(entities.hashtags.text) as topic FROM tweets)"
            ", top_topics_users AS (SELECT ta.user, ta.topic FROM topics_users ta JOIN top_3_topics_ranked tb ON tb.topic = ta.topic)"
            ", top_topics_users_list AS (SELECT user, collect_set(topic) as top_topics_of_user FROM top_topics_users GROUP BY user)"
            " SELECT * FROM top_topics_users_list")
    top_topics_users_list = spark.sql(qry)
    top_topics_users_list.show()
    
    # finds the mentions from tweets with popular topics, using SQL
    qry = ( "WITH topics_tweets AS (SELECT id, explode(entities.hashtags.text) as topic FROM tweets)"
            ", top_topics_tweets AS (SELECT ta.id, ta.topic FROM topics_tweets ta JOIN top_3_topics_ranked tb ON tb.topic = ta.topic)"
            ", tweet_mentions AS (SELECT id, explode(entities.user_mentions.screen_name) as mention FROM tweets)"
            ", top_topics_mentions AS (SELECT ta.mention, tb.topic FROM tweet_mentions ta JOIN top_topics_tweets tb ON tb.id = ta.id)"
            ", top_topics_mentions_list AS (SELECT mention, collect_set(topic) as top_topics_of_mention FROM top_topics_mentions GROUP BY mention)"
            " SELECT * FROM top_topics_mentions_list")
    top_topics_mentions_list = spark.sql(qry)
    top_topics_mentions_list.show()

using_sql()