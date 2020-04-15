package com.learning.rdd

import base.TestBootstrap
import base.TestSetup.{init, kill, session}
import com.learning.helper.DataFrameCreator.fromRdd
import com.learning.helper.RDDCreator.{fromCsv, fromRows}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

class EmployeeAnalyzerTest extends TestBootstrap {

  private val AIRLINES = "src/test/resources/airlines/airlines.csv"

  private var airlines: RDD[String] = _

  before {
    init("EmployeeAnalyzerTest", "local")
    airlines = fromCsv(session.sparkContext, AIRLINES)
  }

  it should "create a RDD from given Rows" in {
    val data = Array(Row("1", "Name", "Address", "000-000-0000", 5, 1))
    val rdd: RDD[Row] = fromRows(session.sparkContext, data)

    assert(rdd.count() == 1, "RDD is empty.")
  }

  it should "create a RDD from given CSV file" in {
    val rdd: RDD[String] = fromCsv(session.sparkContext, AIRLINES)

    assert(rdd.count() == 1580, "Partial RDD is created.")
  }

  it should "print all data of an RDD" in {
    airlines.collect().foreach(println)
  }

  it should "find all unique records of an RDD" in {
    airlines.distinct()
  }

  it should "fetch header of an RDD" in {
    val header: String = airlines.first()
    println(header)
  }

  it should "fetch first record of an RDD" in {
    val header: String = airlines.first()
    val firstRecord: String = airlines.filter(row => row != header).first()
    println(firstRecord)
  }

  it should "fetch first 4 records with header of an RDD" in {
    val records: Array[String] = airlines.take(5)
    records.foreach(println)
  }

  it should "create a data frame from an RDD" in {
    val data = Array(Row("1", "Name", "Address", "000-000-0000", 5, 1))
    val schema = new StructType()
      .add(StructField("id", StringType, true))
      .add(StructField("name", StringType, true))
      .add(StructField("address", StringType, true))
      .add(StructField("contact", StringType, true))
      .add(StructField("empid", IntegerType, true))
      .add(StructField("deptid", IntegerType, true))

    val rdd: RDD[Row] = fromRows(session.sparkContext, data)

    val df: DataFrame = fromRdd(session, rdd, schema)
    assert(df.count() == 1, "DataFrame isn't get created")
  }

  after {
    kill
  }

}
