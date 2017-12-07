'''
To run, use

$SPARK_HOME/bin/spark-submit --master local[4]  --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.3  CassandraTester.py
or
$SPARK_HOME/bin/spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.3  CassandraTester.py



After running python, check

cqlsh> select * from streaming.hw9_topics;

 id     | topic
--------+-------
 123458 | sinky
 123457 | winky
 123456 | binky

(3 rows)

cqlsh> truncate streaming.hw9_topics;
cqlsh> select * from streaming.hw9_topics;

(0 rows)





Dont' use, will be missing library

[root@spark-2-1 hw9_py]# python CassandraTester.py


Don't use, does not work with Spark 2

[root@spark-2-1 hw9_py]# $SPARK_HOME/bin/pyspark --master local[4]  --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.3  CassandraTester.py

See
* https://docs.datastax.com/en/cql/3.3/cql/cql_using/useInsertList.html
* https://github.com/datastax/spark-cassandra-connector
* https://github.com/datastax/spark-cassandra-connector/blob/master/doc/15_python.md
* https://stackoverflow.com/questions/33417341/connecting-integrating-cassandra-with-spark-pyspark
* https://stackoverflow.com/questions/37584077/convert-a-standard-python-key-value-dictionary-list-to-pyspark-data-frame
* https://datastax.github.io/python-driver/getting_started.html
* https://medium.com/@amirziai/running-pyspark-with-cassandra-in-jupyter-2bf5e95c319
'''

def test1():
    # from pyspark.sql import SparkSession
    # spark = SparkSession \
        # .builder \
        # .appName("Python Spark Cassandra example") \
        # .config("spark.cassandra.connection.connections_per_executor_max", "2") \
        # .getOrCreate()
    # spark.read.format("org.apache.spark.sql.cassandra").options(table="tweetdata", keyspace="streaming").load().show()


    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SQLContext

    conf = SparkConf()
    conf.setMaster("local[4]")
    conf.setAppName("Spark Cassandra")
    conf.set("spark.cassandra.connection.connections_per_executor_max", "2")
    conf.set("spark.cassandra.connection.host","10.124.150.206")
    #conf.set("spark.cassandra.connection.host","http://127.0.0.1")

    sc = SparkContext(conf=conf)
    sc.setLogLevel("OFF")
    sqlContext = SQLContext(sc)

    # sqlContext.read\
        # .format("org.apache.spark.sql.cassandra")\
        # .options(table="tweetdata", keyspace="streaming")\
        # .load().show()
    
#test1()
    
    
def old_way():
    test1 = {"id":123456,"topic":"binky"}
    test2 = {"id":123457,"topic":"winky"}
    test3 = {"id":123458,"topic":"sinky"}
    tests = [test1, test2, test3]
    df = sc.parallelize(tests).toDF()
    df.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table="hw9_topics", keyspace="streaming")\
        .save()

# old_way()

def new_way():
    from pyspark.sql import Row
    from collections import OrderedDict

    def convert_to_row(d: dict) -> Row:
        return Row(**OrderedDict(sorted(d.items())))
        
    test1 = {"id":123456,"topic":"binky"}
    test2 = {"id":123457,"topic":"winky"}
    test3 = {"id":123458,"topic":"sinky"}
    tests = [test1, test2, test3]
    df = sc.parallelize(tests) \
        .map(convert_to_row) \
        .toDF()

    df.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table="hw9_topics", keyspace="streaming")\
        .save()
        
# new_way()


def just_read_cluster():
    from cassandra.cluster import Cluster
    cluster = Cluster(["10.124.150.206"]) # tries to find local cluster
    session = cluster.connect("streaming")
    rows = session.execute('select * from tweetdata limit 3;')
    for row in rows:
        print(row.id)


#just_read_cluster()


def just_read():
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SQLContext

    conf = SparkConf()
    conf.setMaster("local[4]")
    conf.setAppName("Spark Cassandra")
    conf.set("spark.cassandra.connection.host","http://127.0.0.1")

    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    sqlContext.read\
        .format("org.apache.spark.sql.cassandra")\
        .options(table="streaming", keyspace="tweetdata")\
        .load().show()

just_read()