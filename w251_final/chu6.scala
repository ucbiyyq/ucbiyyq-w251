/*
* find and download the azure-eventhubs-spark_2.11-2.1.6.jar
* place jar in same folder as this script
*
* sbt clean package
* 
* $SPARK_HOME/bin/spark-submit --jars azure-eventhubs-spark_2.11-2.1.6.jar --class AzureTestChu5 --packages org.apache.spark:spark-streaming_2.11:2.1.0 --master local[2] $(find target -iname "*.jar")
* 
* $SPARK_HOME/bin/spark-shell --jars azure-eventhubs-spark_2.11-2.1.6.jar
*/

import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.eventhubs.EventHubsUtils
import org.apache.spark.sql.SparkSession

import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.SparkConf
import java.io._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode


// import play.api.libs.json._
//import play.api.libs.json.{JsNull,Json,JsString,JsValue}
import org.apache.spark.sql.functions._
import scala.util.parsing.json.JSON

  val progressDir = "temp"
  val policyName = "iothubowner"
  val policykey = "iwzIYZfKbhZr5G6nLy0lTxjhjkLcZLkNxuwV96rJCqU="
  val namespace = "iothub-ns-swm-hub-175554-8925ce81b4"
  val name = "swm-hub"
  val batchDuration = 30

     val eventhubParameters = Map[String, String] (
        "eventhubs.policyname" -> policyName,
        "eventhubs.policykey" -> policykey,
        "eventhubs.namespace" -> namespace,
        "eventhubs.name" -> name,
        "eventhubs.partition.count" -> "4",
        "eventhubs.consumergroup" -> "$Default"
      )

  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.SparkSession

  val spark = SparkSession.builder.appName("AzureTest_3").getOrCreate()

  import spark.implicits._

  val ssc = new StreamingContext(spark.sparkContext, Seconds(batchDuration))

  val inputDirectStream = EventHubsUtils.createDirectStreams( ssc,

       namespace,

       progressDir,

       Map(name -> eventhubParameters))

    val stream = inputDirectStream.transform {rdd => 
        rdd.map(eventData => new String(eventData.getBody))
    }
  
  val windowSize = Duration(90000L)       
  val slidingInterval = Duration(30000L)
  val windowedStream = stream.window(windowSize)
  windowedStream.foreachRDD {rdd => {
    rdd.collect().toList.foreach(println)
    // val spark_local = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
    // import spark.implicits._
    val json = spark.read.json(rdd)
    val metrics = json.select(explode(json("data"))).toDF("metrics")
    // metrics.createOrReplaceTempView("metrics")

    val metrics_data_df = metrics.select("metrics.g","metrics.k","metrics.t","metrics.v")
    val metrics_stat_df1 = metrics_data_df.groupBy("k").agg(avg("v"))
    val metrics_stat_df2 = metrics_stat_df1.withColumnRenamed("avg(v)", "average_value")

    metrics_data_df.write.mode(SaveMode.Append).format("csv").save("hdfs:///user/root/spark_out_data/raw")

    metrics_stat_df2.write.mode(SaveMode.Overwrite).format("csv").save("hdfs:///user/root/spark_out_data/real_time")
  }}
  ssc.start()  