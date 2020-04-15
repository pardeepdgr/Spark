package com.learning.rdd

import base.TestBootstrap
import base.TestSetup.{init, kill, session}
import com.learning.helper.RDDCreator.{fromCsv, fromRows}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

class EmployeeAnalyzerTest extends TestBootstrap {

  private val AIRLINE = "src/main/resources/airlines/airlines.csv"

  before {
    init("EmployeeAnalyzerTest", "local")
  }

  it should "create a RDD from given Rows" in {
    val data = Array(Row("1", "Name", "Address", "000-000-0000", 5, 1))
    val rdd: RDD[Row] = fromRows(session.sparkContext, data)

    assert(rdd.count() == 1, "RDD is empty.")
  }

  it should "create a RDD from given CSV file" in {
    val rdd: RDD[String] = fromCsv(session.sparkContext, AIRLINE)

    assert(rdd.count() == 1580, "Partial RDD is created.")
  }

  after {
    kill
  }

}
