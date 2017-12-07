/*
* sbt clean package
* 
* $SPARK_HOME/bin/spark-submit --jars azure-eventhubs-spark_2.11-2.1.6.jar --class AzureTestChu4 --packages org.apache.spark:spark-streaming_2.11:2.1.0 --master local[2] $(find target -iname "*.jar")
*/
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.eventhubs.EventHubsUtils

import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.SparkConf

import scala.util.parsing.json._

import com.microsoft.azure.eventhubs.EventData

object AzureTestChu4 {

	def process(rdd: EventData) : List[JamesData] = {
		val james1: JamesData = JamesData("binky", 1, 2)
		val james2: JamesData = JamesData("winky", 2, 3)
		val james3: JamesData = JamesData("sinky", 3, 4)
		val results: List[JamesData] = List(james1, james2, james3)
		return results
	}
	
	def main(args: Array[String]) {
		val progressDir: String = "/project"
		val policyName: String = "iothubowner"
		val policykey: String = "iwzIYZfKbhZr5G6nLy0lTxjhjkLcZLkNxuwV96rJCqU="
		val namespace: String = "iothub-ns-swm-hub-175554-8925ce81b4"
		val name: String = "swm-hub"
		val batchDuration: Int = 30
		val eventhubParameters: Map[String, String] = Map[String, String] ( "eventhubs.policyname" -> policyName, "eventhubs.policykey" -> policykey, "eventhubs.namespace" -> namespace, "eventhubs.name" -> name, "eventhubs.partition.count" -> "4", "eventhubs.consumergroup" -> "$Default")
		val sparkConf: SparkConf = new SparkConf().setAppName("Azure Test")
		val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(batchDuration))
		val inputDirectStream: DStream[EventData] = EventHubsUtils.createDirectStreams( ssc, namespace, progressDir, Map(name -> eventhubParameters) )
		val windowedInputStream: DStream[EventData] = inputDirectStream.window(org.apache.spark.streaming.Minutes(1))
		val output: DStream[JamesData] = windowedInputStream.flatMap(process)
	
		ssc.start()
		ssc.awaitTermination()
	}
}


case class JamesData(k: String, v: Long, t: Long)

// case class D2OnTime(v: Long, t: Long)

/*
{
      "k": "D2OnTime",
      "v": "372",
      "t": 8501977764621
    },
*/