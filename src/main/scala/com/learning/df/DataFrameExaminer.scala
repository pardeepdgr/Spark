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

  dfFromCsv.select("origin", "destination").show()
  import session.implicits._
  val totalTimeDF = dfFromCsv
    .select($"air_time" - $"departure_delay" + $"arrival_delay")
    .withColumnRenamed("((air_time - departure_delay) + arrival_delay)", "total_time")
    .show()
}
