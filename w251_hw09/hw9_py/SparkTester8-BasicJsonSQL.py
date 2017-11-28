'''
Test for basic SQL query on JSON, to see if we can parse the tweet json using Spark
Assumes the json data file exists already /root/data/tweet_samples.txt

On Terminal 1 run,
$ python SparkTester8-BasicJsonSQL.py


See 
* https://databricks.com/blog/2015/02/02/an-introduction-to-json-support-in-spark-sql.html
* https://spark.apache.org/docs/latest/sql-programming-guide.html
* http://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    
# creates a df of type: <class 'pyspark.sql.dataframe.DataFrame'>
df = spark.read.json("/root/data/tweet_samples3.json")
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

# finds the top topics using DFs
df.select(explode(df["entities.hashtags.text"]).alias("topic") ).groupby("topic").count().sort("count", ascending=False).show()


# show the id, user, and nested topics & mentions using SQL & DFs
#df.createOrReplaceTempView("tweets")
#sqlDF1 = spark.sql("SELECT id as id1, user.screen_name as user FROM tweets")
#sqlDF2 = spark.sql("SELECT id as id2, explode(entities.hashtags.text) as topic FROM tweets")
#sqlDF3 = spark.sql("SELECT id as id3, explode(entities.user_mentions.screen_name) as mention FROM tweets")
#sqlDF4 = sqlDF1.join(sqlDF2, sqlDF1.id1 == sqlDF2.id2, "left").join(sqlDF3, sqlDF1.id1 == sqlDF3.id3, "left").select(sqlDF1.id1, sqlDF1.user, sqlDF2.topic, sqlDF3.mention) #this doesn't work
#sqlDF4.show()


# finds the top topics using SQL
# df.createOrReplaceTempView("tweets")
#sqlDF = spark.sql("SELECT id, topic FROM (SELECT id, explode(entities.hashtags.text) as topic FROM tweets) WHERE limit 3").show()
# qry = ( "WITH step1 AS (SELECT id, explode(entities.hashtags.text) as topic FROM tweets)"
        # ", step2 AS (SELECT id, topic, count(*) over (partition by topic) as num_tweets FROM step1)"
        # ", step3 AS (SELECT id, topic, num_tweets, dense_rank() over (order by num_tweets desc) as num_tweets_rnk FROM step2)"
        # " SELECT * FROM step3 WHERE num_tweets_rnk < 2 ORDER BY num_tweets DESC")
# sqlDF = spark.sql(qry).show()