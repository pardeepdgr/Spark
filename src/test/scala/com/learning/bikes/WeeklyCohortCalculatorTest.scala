package com.learning.bikes

import java.text.SimpleDateFormat
import java.util.Date

import base.TestBootstrap
import base.TestSetup.{init, kill, session}
import com.learning.helper.DataFrameComparator.compareContent
import com.learning.creator.DataFrameCreator.fromCsv
import org.apache.spark.sql.DataFrame

class WeeklyCohortCalculatorTest extends TestBootstrap {
  private val BIKES = "src/test/resources/bikes/raw/bikes.csv"
  private val COHORTS = "src/test/resources/bikes/transformed/weekly_cohorts.csv"

  private var bikes: DataFrame = _
  private var calculator: WeeklyCohortCalculator = _

  before {
    init("WeeklyCohortCalculatorTest", "local")
    bikes = fromCsv(session, BIKES)
    calculator = new WeeklyCohortCalculator(session, bikes)
  }

  it should "calculate weekly averages of aggregated counts of each customer for given week number and duration" in {
    val cohorts: DataFrame = calculator.calculate(201814, 3)

    val isSameContent = compareContent(fromCsv(session, COHORTS), cohorts)
    assert(isSameContent, "Customer weekly cohorts analysis data mismatches")
  }

  it should "calculate weekly averages of aggregated counts of each customer for given date and duration" in {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date: Date = dateFormat.parse("2018-04-07")
    val cohorts: DataFrame = calculator.calculate(date, 3)

    val isSameContent = compareContent(fromCsv(session, COHORTS), cohorts)
    assert(isSameContent, "Customer weekly cohorts analysis data mismatches")
  }

  after {
    kill
  }

}