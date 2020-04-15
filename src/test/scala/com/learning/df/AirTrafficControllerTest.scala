package com.learning.df

import base.TestBootstrap
import base.TestHelper.readCSV
import base.TestSetup.{init, kill, session}
import com.learning.df.AirTrafficController.getActualAirTime
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

  after {
    kill
  }

}
