package com.learning.df

import com.learning.spark.SparkInstance.session

object DataFrameExaminer extends App {
  val rangeDf = session.range(5)

  val flightDf = session.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .load("src/main/resources/airlines/flights.csv")
  flightDf.printSchema()
  flightDf.describe()
  flightDf.limit(5).show()
  // OR //
  flightDf.show(5)
  flightDf.show(false)

  flightDf.select("origin", "destination")
  flightDf.drop("air_time", "arrival_delay", "departure_delay")
  flightDf.distinct()

  import session.implicits._
  flightDf.select($"air_time" - $"departure_delay" + $"arrival_delay")
    .withColumnRenamed("((air_time - departure_delay) + arrival_delay)", "total_time")

  flightDf.filter("destination = 'LAX'")
  // OR //
  flightDf.filter(flightDf("destination")==="LAX")
  flightDf.filter(flightDf("destination").isin("LAX","JFK"))

  flightDf.groupBy("flight_number").count()
  flightDf.groupBy("flight_number").sum("air_time")

  import org.apache.spark.sql.functions._
  flightDf.orderBy(desc("air_time"))

  flightDf.stat.crosstab("origin","distance")
}
