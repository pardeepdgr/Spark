package com.learning.helper

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.StructType

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
}
