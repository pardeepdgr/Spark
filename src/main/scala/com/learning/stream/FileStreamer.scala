package com.learning.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

class FileStreamer() {

  /* Streams ignore already existing files */
  def streamFrom(directory: String) = {
    val sparkConf = new SparkConf().setAppName("FileStreamer").setMaster("local")
    val context = new StreamingContext(sparkConf, Seconds(5))
    context.checkpoint("file:///tmp/spark")

    val stream: DStream[String] = context.textFileStream(directory)

    stream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        println("No. of Records: " + rdd.count())
      }
    })

    context.start()
    context.awaitTermination()
  }

}
