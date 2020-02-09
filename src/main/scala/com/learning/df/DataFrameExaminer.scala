package com.learning.df

import com.learning.spark.SparkInstance.session

object DataFrameExaminer extends App {
  val dfFromRange = session.range(5)
  dfFromRange.show()

  val dfFromCsv = session.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .load("src/main/resources/airlines/flights.csv")
  dfFromCsv.printSchema()
}
