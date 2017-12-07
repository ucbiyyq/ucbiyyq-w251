W251 HW9

Dependencies
* 3-node Spark 2.0 Cluster:
** spark-2-1 (master)
** spark-2-2
** spark-2-3
* 1-node Cassandra cluster
** assumed located on spark-2-1
* twitter app whose stream is accessible to the spark-2-1 server

How to run project:
1. in terminal 0, run following cqlsh commands to clean cassandra tables, if we want to reset from previous run
truncate streaming.hw9_users;
truncate streaming.hw9_topics;
truncate streaming.hw9_mentions;

2. in terminal 1, run the TweetBroker.py script as a python script
python TweetBroker.py

... wait for: listening on 5555

3. in terminal 2, run the SparkStreaming.py script using spark-submit & cassandra connector
$SPARK_HOME/bin/spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.3 SparkStreaming.py

... wait for: finished!



to change the batch interval, run time, and top-n of the script
* in the SparkStreaming.py change the following variables:
** my_batch_interval
** my_total_run_time
** my_top_n_topics

error on stopping Spark streaming
* please ignore the following error
* script should complete on its own
* might be thrown by SparkStreaming.py, when the script terminates itself after pre-determined run time

Exception in thread "receiver-supervisor-future-0" java.lang.Error: java.lang.InterruptedException: sleep interrupted
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1155)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
        at java.lang.Thread.run(Thread.java:748)
Caused by: java.lang.InterruptedException: sleep interrupted
        at java.lang.Thread.sleep(Native Method)
        at org.apache.spark.streaming.receiver.ReceiverSupervisor$$anonfun$restartReceiver$1.apply$mcV$sp(ReceiverSupervisor.scala:196)
        at org.apache.spark.streaming.receiver.ReceiverSupervisor$$anonfun$restartReceiver$1.apply(ReceiverSupervisor.scala:189)
        at org.apache.spark.streaming.receiver.ReceiverSupervisor$$anonfun$restartReceiver$1.apply(ReceiverSupervisor.scala:189)
        at scala.concurrent.impl.Future$PromiseCompletingRunnable.liftedTree1$1(Future.scala:24)
        at scala.concurrent.impl.Future$PromiseCompletingRunnable.run(Future.scala:24)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
        ... 2 more

The TweetBroker.py might start showing the follow error message once the Spark Streaming context is closed

    Error on_data: [Errno 32] Broken pipe
    {"created_at":"Sun Dec 03 20:15:12 +0000 2017","id":93741493 ...
    Error on_data: [Errno 32] Broken pipe
    {"delete":{"status":{"id":935573795640930305,"id_str":"93557 ...
    Error on_data: [Errno 32] Broken pipe


		
Note on ranking
* we assumed the top-n ranking to be by dense rank of number of tweets
* to work around ties, we also ordered topic by "alphabetical order", though this has a less-than-useful human meaning for international tweets


Cassandra Connection errors
* apparently there's some error in my cassandra setup that prevents me from connecting to the cluster properly
* the code works just fine if we are using a single spark node and a local cassandra node

example errors:
* pyspark.sql.utils.IllegalArgumentException: 'Cannot build a cluster without contact points'
* py4j.protocol.Py4JJavaError: An error occurred while calling o189.save.: java.io.IOException: Failed to open native connection to Cassandra at {10.124.150.206}:9042
* NoHostAvailable: ('Unable to connect to any servers', {'10.124.150.206': ConnectionRefusedError(111, "Tried connecting to [('10.124.150.206', 9042)]. Last error: Connection refused")})


Appendix


Location of main project files on Spark-2-1

# main project deliverables
/root/github/w251/w251_hw09/hw9
/root/github/w251/w251_hw09/hw9/SparkStreaming.py
/root/github/w251/w251_hw09/hw9/TweetBroker.py

# various output logs
/root/logs

# sample data files
/root/data/samples

# other attempts
/root/github/w251/w251_hw09/hw9_py