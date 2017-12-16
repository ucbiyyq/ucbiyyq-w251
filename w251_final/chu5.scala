/*
* find and download the azure-eventhubs-spark_2.11-2.1.6.jar
* place jar in same folder as this script
*
* sbt clean package
* 
* $SPARK_HOME/bin/spark-submit --jars azure-eventhubs-spark_2.11-2.1.6.jar --class AzureTestChu5 --packages org.apache.spark:spark-streaming_2.11:2.1.0 --master local[2] $(find target -iname "*.jar")
*/
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.eventhubs.EventHubsUtils

import com.microsoft.azure.eventhubs.EventData

import scala.util.parsing.json._


object AzureTestChu5 {
	val totalRuntime_s: Integer = 15
	val batchDuration_s: Int = 30

	// def process(rdd: EventData) : List[JamesData] = {
		// val james1: JamesData = JamesData("binky", 1, 2)
		// val james2: JamesData = JamesData("winky", 2, 3)
		// val james3: JamesData = JamesData("sinky", 3, 4)
		// val results: List[JamesData] = List(james1, james2, james3)
		// return results
	// }
    
    // def process(rdd: RDD[EventData]) : RDD[JamesData] = {
        // val spark: SparkSession = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf) // Get the singleton instance of SparkSession
        // import spark.implicits._
        
        // val dataStr = rdd.map(eventData => new String(eventData.getBody))
        // val dataJson = spark.read.json(dataStr)
        // val james1: JamesData = JamesData("binky", 1, 2)
        // return james1
    // }
	
	def main(args: Array[String]) {
		val progressDir: String = "/project"
		val policyName: String = "iothubowner"
		val policykey: String = "iwzIYZfKbhZr5G6nLy0lTxjhjkLcZLkNxuwV96rJCqU="
		val namespace: String = "iothub-ns-swm-hub-175554-8925ce81b4"
		val name: String = "swm-hub"
		val eventhubParameters: Map[String, String] = Map[String, String] ( "eventhubs.policyname" -> policyName, "eventhubs.policykey" -> policykey, "eventhubs.namespace" -> namespace, "eventhubs.name" -> name, "eventhubs.partition.count" -> "4", "eventhubs.consumergroup" -> "$Default")
		val sparkConf: SparkConf = new SparkConf().setAppName("Azure Test")
        val sc: SparkContext = new SparkContext(sparkConf)
		val ssc: StreamingContext = new StreamingContext(sc, Seconds(batchDuration_s))
		val inputDirectStream: DStream[EventData] = EventHubsUtils.createDirectStreams( ssc, namespace, progressDir, Map(name -> eventhubParameters) )
		val windowedInputStream: DStream[EventData] = inputDirectStream.window(Seconds(batchDuration_s * 1))
        
        val metrics_stream = inputDirectStream.map {eventData => 
            val rawData = new String(eventData.getBody)
            val parsedData = JSON.parseFull(rawData)
            
            parsedData match {
                case Some(m: Map[String, List[Map[String, String]]]) => m("Data") match {
                case s: Any => s}
            }
        }

        val metrics_flat = metrics_stream.flatMap(list =>list).map(x => (x("k"),( x("v"), x("t"))))

        metrics_flat.foreachRDD { (rdd,time) =>
            rdd.collect().toList.foreach(println)
        }
        
		ssc.start()
		ssc.awaitTerminationOrTimeout(totalRuntime_s * 1000)
		ssc.stop(true, true)

		println(s"============ Exiting ================")
		System.exit(0)
	}
}


// case class JamesData(k: String, v: Long, t: Long)

/** Lazily instantiated singleton instance of SparkSession */
// object SparkSessionSingleton {

  // @transient  private var instance: SparkSession = _

  // def getInstance(sparkConf: SparkConf): SparkSession = {
    // if (instance == null) {
      // instance = SparkSession.builder.config(sparkConf).getOrCreate()
    // }
    // instance
  // }
// }