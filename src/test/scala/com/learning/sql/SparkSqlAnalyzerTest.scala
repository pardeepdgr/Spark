package com.learning.sql

import base.TestBootstrap
import base.TestSetup.{init, kill, session}
import com.learning.helper.DataFrameAssistant.registerDataFrameAsView
import com.learning.helper.DataFrameCreator.fromCsv
import org.apache.spark.sql.DataFrame

class SparkSqlAnalyzerTest extends TestBootstrap {

  private val FLIGHTS = "src/test/resources/airlines/flights.csv"
  private var flights: DataFrame = _

  before {
    init("AirTrafficControllerTest", "local")
    flights = fromCsv(session, FLIGHTS)
  }

  it should "get all flight which fly more than 2500 using SparkSQL" in {
    val VIEW_NAME = "flights"
    val QUERY = "select flight_number, destination, distance from flights"

    registerDataFrameAsView(flights, VIEW_NAME)
    val df: DataFrame = session.sqlContext.sql(QUERY)
    df.filter(df("distance") > 2500)
  }

  after {
    kill
  }

}
