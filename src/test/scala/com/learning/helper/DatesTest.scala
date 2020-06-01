package com.learning.helper

import base.TestBootstrap
import base.TestSetup.{init, kill}
import com.learning.helper.DataFrameComparator.compareContent
import com.learning.helper.Dates.{currentEpoch, currentTimestamp, toDate, toEpoch, toTimestamp}
import com.learning.spark.SparkInstance.session.implicits._
import org.apache.spark.sql.functions.col

class DatesTest extends TestBootstrap {

  before {
    init("DatesTest", "local")
  }

  behavior of "Operations on Date"

  it should "convert string-date column into column of type epoch (number of seconds)" in {
    val dateStrings = Seq(("2019-07-01 12:01:19.000", "07-01-2019 12:01:19.000", "07-01-2019"))
      .toDF("c1", "c2", "c3")

    val actualEpochs = dateStrings
      .select(toEpoch(col("c1")),
        toEpoch(col("c2"), "MM-dd-yyyy HH:mm:ss"),
        toEpoch(col("c3"), "MM-dd-yyyy"),
        currentEpoch().as("curr_epoch")).drop("curr_epoch")

    val expectedEpochs = Seq(("1561962679", "1561962679", "1561919400"))
      .toDF("c1", "c2", "c3")

    assert(compareContent(expectedEpochs, actualEpochs))
  }

  it should "convert string-date column into column of type Date" in {
    val dateStrings = Seq("06-03-2019", "07-03-2020").toDF("c1")
    dateStrings.select(toDate(col("c1"), "MM-dd-yyyy"))

    val timestampStrings = Seq("2020-01-21 12:01:19").toDF("t1")
    timestampStrings.select(toDate(col("t1")))
  }

  it should "get current TimeStamp" in {
    val timestampStrings = Seq("2020-01-21 12:01:19").toDF("c1")
    timestampStrings.withColumn("c1", currentTimestamp)
  }

  it should "convert string-timestamp column into column of type TimeStamp" in {
    val tsStrings = Seq("2020-01-21 12:01:19", "2020-01-24", "2020-11-26 16:44:55.406").toDF("c1")
    tsStrings.select(toTimestamp(col("c1")))
  }

  after {
    kill
  }

}
