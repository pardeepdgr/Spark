package com.learning.pay_later

import base.TestBootstrap
import base.TestSetup.{init, kill, session}
import com.learning.creator.DataFrameCreator.fromCsv
import com.learning.helper.DataFrameComparator.compareContent

class DailyReportCreatorTest extends TestBootstrap {

  before {
    init("DailyReportCreatorTest", "local")
  }

  behavior of "Daily report creator"

  it should "create daily report" in {
    val transaction = fromCsv(session, "src/test/resources/pay_later/transactions.csv")

    val actualDf = DailyReportCreator.create(transaction)
    val expectedDf = fromCsv(session, "src/test/resources/pay_later/out/daily_summary.csv")

    val isSameContent = compareContent(expectedDf, actualDf)
    assert(isSameContent)
  }

  after {
    kill
  }

}