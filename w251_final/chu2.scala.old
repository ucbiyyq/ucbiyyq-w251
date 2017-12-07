val windowedInputStream = inputDirectStream.window(org.apache.spark.streaming.Minutes(1))
    val newDS = windowedInputStream.transform { rdd =>
        rdd.map(eventData => new String(eventData.getBody).split("\n"))
    }
    newDS.foreachRDD {rdd =>                          
       val pairs = rdd.flatMap{case (jsonItemArr) => {
            jsonItemArr.toList.foreach{
                case (jsonStr) => JSON.parseFull(jsonStr)
            }
        }}.foreach{case (jsonItem) => {
            (jsonItem.get("k"), jsonItem.get("v"))
        }}
        pairs.reduceByKey(_ + _)
        pairs.print()
    }