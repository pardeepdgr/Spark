package com.learning.bikes

import base.TestBootstrap
import base.TestSetup.{init, kill, session}
import com.learning.helper.DataFrameComparator.compareContent
import com.learning.helper.DataFrameCreator.fromCsv
import org.apache.spark.sql.DataFrame

class CustomerAverageAggregatorTest extends TestBootstrap {

  private val BIKES = "src/test/resources/bikes/raw/bikes.csv"
  private val HOURLY_AGG_CUSTOMER = "src/test/resources/bikes/transformed/hourly_agg.csv"
  private var bikes: DataFrame = _

  before {
    init("CustomerAverageAggregatorTest", "local")
    bikes = fromCsv(session, BIKES)
  }

  it should "calculate hourly averages of aggregated counts of each customer" in {
    val aggregator = new CustomerAverageAggregator
    val hourlyAggregatedCustomers = aggregator.findHourlyAggregatedCustomers(bikes)

    val isSameContent = compareContent(fromCsv(session, HOURLY_AGG_CUSTOMER), hourlyAggregatedCustomers)
    assert(isSameContent, "Content isn't same")
  }

  after {
    kill
  }

}
