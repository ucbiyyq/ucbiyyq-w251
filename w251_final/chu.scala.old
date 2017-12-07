val tweet_stream = stream.map(status => (status.getText.split(" ").filter(_.startsWith("#")), List(status.getUser.getScreenName()) ++ status.getText.split(" ").filter(_.startsWith("@"))))
    val topic_to_commentators = tweet_stream.flatMap{ case (topic, commentators) => topic.map( topic => (topic, commentators))}

   val topNCounts = topic_to_commentators
                      .map{case (topic,commentators) => (topic, (1, commentators))}
                      .reduceByKeyAndWindow((x, y) => (x._1 + y._1, x._2 ++ y._2), Seconds(interval))
                      .map{case(topic, (count, commentators)) => (count, (topic, commentators))}
                      .transform(_.sortByKey(false))
    // Print popular hashtags
    topNCounts.foreachRDD(rdd=> {
        val topList = rdd.take(top_n)
    topList.foreach{case (count, (topic, commentators)) => {
         println("tweet count:"+count+"\t|\t tweet topic:"+topic+ "\t|\t tweet commentators:"+ commentators.toSet + "\n========================")
      }}
    })