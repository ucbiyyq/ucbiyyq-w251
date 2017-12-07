/*
* sbt clean package
* 
* $SPARK_HOME/bin/spark-submit --jars azure-eventhubs-spark_2.11-2.1.6.jar --class AzureTest --packages org.apache.spark:spark-streaming_2.11:2.1.0 --master local[2] $(find target -iname "*.jar")
* --master spark://w209FinalSpark1:7077 $(find target -iname "*.jar")
*/
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.eventhubs.EventHubsUtils

import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.SparkConf

object AzureTest {
  def main(args: Array[String]) {
   val progressDir = "/project"
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
    val sparkConf = new SparkConf().setAppName("Azure Test")
    val ssc = new StreamingContext(sparkConf, Seconds(batchDuration))
    val inputDirectStream = EventHubsUtils.createDirectStreams( ssc,
         namespace,
         progressDir,
         Map(name -> eventhubParameters))
    inputDirectStream.foreachRDD { rdd =>
         rdd.flatMap(eventData => new String(eventData.getBody).split(" ")).collect().toList.
           foreach(println)
       }
    ssc.start()
    ssc.awaitTermination()
  }
}