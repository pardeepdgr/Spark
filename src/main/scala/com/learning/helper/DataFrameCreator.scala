package com.learning.helper

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

object DataFrameCreator {
  private val DELIMITER = ","

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

  def fromStrings(session: SparkSession, records: ArrayBuffer[String], schema: StructType): DataFrame = {
    var rows = ArrayBuffer.empty[Row]
    for (record <- records) {
      val cells = record.split(DELIMITER)
      rows = rows :+ Row(cells:_*)
    }

    val rdd = RDDCreator.fromRows(session.sparkContext, rows.toArray)
    fromRdd(session, rdd, schema)
  }

}
