package com.learning.bikes

import base.TestBootstrap
import base.TestSetup.{init, kill, session}
import com.learning.helper.DataFrameComparator.compareContent
import com.learning.creator.DataFrameCreator.fromCsv
import org.apache.spark.sql.DataFrame

class CustomerAverageAggregatorTest extends TestBootstrap {

  private val BIKES = "src/test/resources/bikes/raw/bikes.csv"
  private val DAILY_AGG_CUSTOMER = "src/test/resources/bikes/transformed/daily_agg.csv"
  private val HOURLY_AGG_CUSTOMER = "src/test/resources/bikes/transformed/hourly_agg.csv"
  private val MONTHLY_AGG_CUSTOMER = "src/test/resources/bikes/transformed/monthly_agg.csv"
  private val WEEKLY_AGG_CUSTOMER = "src/test/resources/bikes/transformed/weekly_agg.csv"

  private var bikes: DataFrame = _
  private var aggregator: CustomerAverageAggregator = _

  before {
    init("CustomerAverageAggregatorTest", "local")
    bikes = fromCsv(session, BIKES)
    aggregator = new CustomerAverageAggregator
  }

  it should "calculate hourly averages of aggregated counts of each customer" in {
    val hourlyAggregatedCustomers = aggregator.findHourlyAggregatedCustomers(bikes)

    val isSameContent = compareContent(fromCsv(session, HOURLY_AGG_CUSTOMER), hourlyAggregatedCustomers)
    assert(isSameContent, "Daily aggregated customer data mismatch")
  }

  it should "calculate daily averages of aggregated counts of each customer" in {
    val dailyAggregatedCustomers = aggregator.findDailyAggregatedCustomers(bikes)

    val isSameContent = compareContent(fromCsv(session, DAILY_AGG_CUSTOMER), dailyAggregatedCustomers)
    assert(isSameContent, "Daily aggregated customer data mismatch")
  }

  it should "calculate weekly averages of aggregated counts of each customer" in {
    val weeklyAggregatedCustomers = aggregator.findWeeklyAggregatedCustomers(bikes)

    val isSameContent = compareContent(fromCsv(session, WEEKLY_AGG_CUSTOMER), weeklyAggregatedCustomers)
    assert(isSameContent, "Weekly aggregated customer data mismatch")
  }

  it should "calculate monthly averages of aggregated counts of each customer" in {
    val monthlyAggregatedCustomers = aggregator.findMonthlyAggregatedCustomers(bikes)

    val isSameContent = compareContent(fromCsv(session, MONTHLY_AGG_CUSTOMER), monthlyAggregatedCustomers)
    assert(isSameContent, "Monthly aggregated customer data mismatch")
  }

  after {
    kill
  }

}
