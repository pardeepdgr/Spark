package com.learning.df

import com.learning.helper.DataFrameCreator
import com.learning.spark.SparkInstance.session

object AirTrafficController {
  val FLIGHT_DATASET_PATH = "src/main/resources/airlines/flights.csv"

  def main(args: Array[String]): Unit = {

    val flightDf = DataFrameCreator.fromCsv(session, FLIGHT_DATASET_PATH)
    val columns: Array[String] = flightDf.columns
    flightDf.printSchema()
    flightDf.describe()
    flightDf.show(false)
    flightDf.limit(5).show()
    // OR //
    flightDf.show(5)

    flightDf.select("origin", "destination")
    flightDf.drop("air_time", "arrival_delay", "departure_delay")
    flightDf.distinct()

    import session.implicits._
    flightDf.select($"air_time" - $"departure_delay" + $"arrival_delay")
      .withColumnRenamed("((air_time - departure_delay) + arrival_delay)", "total_time")

    flightDf.filter("destination = 'LAX'")
    // OR //
    flightDf.filter(flightDf("destination") === "LAX")
    flightDf.filter(flightDf("destination").isin("LAX", "JFK"))

    flightDf.groupBy("flight_number").count()
    flightDf.groupBy("flight_number").sum("air_time")

    import org.apache.spark.sql.functions._
    flightDf.orderBy(desc("air_time"))

    flightDf.stat.crosstab("origin", "distance")
  }
}
