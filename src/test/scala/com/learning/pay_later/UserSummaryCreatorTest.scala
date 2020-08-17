package com.learning.pay_later

import base.TestBootstrap
import base.TestSetup.{init, kill, session}
import com.learning.creator.DataFrameCreator.fromCsv
import com.learning.helper.DataFrameComparator.compareContent


class UserSummaryCreatorTest extends TestBootstrap {

  before {
    init("UserSummaryCreatorTest", "local")
  }

  behavior of "User summary creator"

  it should "create user summary" in {
    val userSummary = fromCsv(session, "src/test/resources/pay_later/users.csv")
    val transaction = fromCsv(session, "src/test/resources/pay_later/transactions.csv")
    val repayment = fromCsv(session, "src/test/resources/pay_later/repayments.csv")

    val actualDf = UserSummaryCreator.create(userSummary, transaction, repayment)
    val expectedDf = fromCsv(session, "src/test/resources/pay_later/out/user_summary.csv")

    val isSameContent = compareContent(expectedDf, actualDf)
    assert(isSameContent)
  }

  after {
    kill
  }

}
