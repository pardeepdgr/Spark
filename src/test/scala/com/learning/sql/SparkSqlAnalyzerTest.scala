package com.learning.sql

import base.TestBootstrap
import base.TestSetup.{init, kill, session}
import com.learning.helper.DataFrameCreator.fromCsv
import org.apache.spark.sql.DataFrame

class SparkSqlAnalyzerTest extends TestBootstrap {

  private val FLIGHTS = "src/test/resources/airlines/flights.csv"
  private var flights: DataFrame = _

  before {
    init("SparkSqlAnalyzerTest", "local")
    flights = fromCsv(session, FLIGHTS)
  }

  it should "get all flights which fly more than 2500" in {
    val df = SparkSqlAnalyzer(session).registerDataFrameAsViewForCurrentSession(flights)
    assert(df.count() == 9337)
  }

  it should "get all flights which has flown from JFK" in {
    val df = SparkSqlAnalyzer(session).registerDataFrameAsViewForAllSessions(flights)
    assert(df.count() == 8070)
  }

  after {
    kill
  }

}
