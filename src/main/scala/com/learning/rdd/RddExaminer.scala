package com.learning.rdd

import com.learning.spark.SparkInstance.sparkContext

object RddExaminer extends App {

  val rddFromCollection = createRddFromCollection
  rddFromCollection.count()
  rddFromCollection.first()
  rddFromCollection.take(2)
  rddFromCollection.distinct()

  val rddFromCsv = createRddFromCsv
  rddFromCsv.collect().foreach(println)

  private def createRddFromCsv = {
    val path = "src/main/resources/airlines/airlines.csv"
    sparkContext.textFile(path)
  }

  private def createRddFromCollection = {
    val data = Array("1", "Name", "Address", "000-000-0000", 5, 1)
    sparkContext.parallelize(data)
  }
}
