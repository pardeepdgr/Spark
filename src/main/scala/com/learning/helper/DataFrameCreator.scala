package com.learning.helper

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

object DataFrameCreator {

  def fromRdd(session: SparkSession, rdd: RDD[Row], schema: StructType): DataFrame = {
    session.createDataFrame(rdd, schema)
  }

  def fromCsv(session: SparkSession, path: String): DataFrame = {
    session.read
      .format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(path)
  }

  def fromRange(session: SparkSession, range: Long): Dataset[java.lang.Long] = {
    session.range(range)
  }

  //TODO: make it dynamic
  def fromStrings(session: SparkSession, records: ArrayBuffer[String], delimiter: String, schema: StructType): DataFrame = {
    var rows = Array.empty[Row]
    for (record <- records) {
      val cells = record.split(delimiter)
      rows = rows :+ Row(cells(0), cells(1), cells(2), cells(3))
    }

    val rdd = RDDCreator.fromRows(session.sparkContext, rows)
    fromRdd(session, rdd, schema)
  }

}
