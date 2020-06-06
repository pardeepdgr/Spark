package com.learning.creator

import base.TestBootstrap
import base.TestSetup.{init, kill, session}
import com.learning.creator.RDDCreator.{fromCsv, fromRows}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

class RDDCreatorTest extends TestBootstrap {

  private val AIRLINES = "src/test/resources/airlines/airlines.csv"

  before {
    init("RDDCreatorTest", "local")
  }

  behavior of "RDD Creation"

  it should "create a RDD from given Rows" in {
    val data = Array(Row("1", "Name", "Address", "000-000-0000", 5, 1))
    val rdd: RDD[Row] = fromRows(session.sparkContext, data)

    assert(rdd.count() == 1, "RDD is empty.")
  }

  it should "create a RDD from given CSV file" in {
    val rdd: RDD[String] = fromCsv(session.sparkContext, AIRLINES)

    assert(rdd.count() == 1580)
  }

  after {
    kill
  }

}
