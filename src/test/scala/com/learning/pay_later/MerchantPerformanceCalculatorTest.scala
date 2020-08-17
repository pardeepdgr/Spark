package com.learning.pay_later

import base.TestBootstrap
import base.TestSetup.{init, kill, session}
import com.learning.creator.DataFrameCreator.fromCsv
import com.learning.helper.DataFrameComparator.compareContent

class MerchantPerformanceCalculatorTest extends TestBootstrap {

  before {
    init("MerchantPerformanceCalculatorTest", "local")
  }

  behavior of "Merchant performance calculator"

  it should "calculate overall and last week merchant performance" in {
    val transaction = fromCsv(session, "src/test/resources/pay_later/transactions.csv")

    val actualDf = MerchantPerformanceCalculator.overallAndLastWeekPerf(transaction)
    val expectedDf = fromCsv(session, "src/test/resources/pay_later/out/merchant_perf.csv")

    val isSameContent = compareContent(expectedDf, actualDf)
    assert(isSameContent)
  }

  after {
    kill
  }

}
