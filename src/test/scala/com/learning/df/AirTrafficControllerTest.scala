package com.learning.df

import base.TestBootstrap
import base.TestHelper.readCSV
import base.TestSetup.{init, kill, session}
import com.learning.df.AirTrafficController.getActualAirTime
import com.learning.helper.DataFrameAssistant.castColumnTo
import org.apache.spark.sql.DataFrame

class AirTrafficControllerTest extends TestBootstrap {
  private val FLIGHTS = "src/test/resources/airlines/flights.csv"
  private val AIRLINES = "src/test/resources/airlines/airlines.csv"

  private var flights: DataFrame = _
  private var airlines: DataFrame = _

  before {
    init("AirTrafficControllerTest", "local")
    flights = readCSV(session, FLIGHTS)
    airlines = readCSV(session, AIRLINES)
  }

  it should "print schema of flights in tree format" in {
    flights.printSchema()
  }

  it should "describe vital stats(count, max, min, mean, stddev) of flights" in {
    flights.describe().show()
  }

  it should "print 5 element on console without any truncation of long column-values" in {
    airlines.limit(5).show(false)
  }

  it should "fetch all columns name of flights" in {
    val columns = airlines.describe().columns
    assert(columns.sameElements(Array("summary", "Code", "Description")), "column mismatch")
  }

  it should "select all origins and destinations of flights" in {
    flights.select("origin", "destination").show()
  }

  it should "delete delays of flights" in {
    flights.drop("arrival_delay", "departure_delay").show()
  }

  it should "select non-duplicate origin of all flights" in {
    flights.select("origin").distinct().show()
  }

  it should "get actual air time of all flights" in {
    getActualAirTime(flights).show()
  }

  it should "filter all flight which are going to LAX" in {
    flights.filter("destination = 'LAX'").show()
    // OR //
    flights.filter(flights("destination") === "LAX").show()
  }

  it should "filter all flight which are going to LAX and JFK" in {
    flights.filter(flights("destination").isin("LAX", "JFK")).show()
  }

  it should "find number of rounds made by each flight" in {
    flights.groupBy("flight_number").count().show()
  }

  it should "find total air time taken by each flight" in {
    castColumnTo(flights, "air_time", "Decimal(5,2)")
      .groupBy("flight_number").sum("air_time").show()
  }

  it should "print in the descending order of airtime taken by each flight" in {
    import org.apache.spark.sql.functions.desc
    flights.orderBy(desc("air_time")).show()
  }

  it should "create contingency table of origin and distance" in {
    flights.stat.crosstab("origin", "distance").show()
  }

  after {
    kill
  }

}
