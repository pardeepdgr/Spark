package com.learning.df.helper

import org.apache.spark.sql.SparkSession

object DataFrameCreator {

  def fromCsv(session: SparkSession, path: String) = {
    session.read
      .format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(path)
  }

  def fromRange(session: SparkSession, range: Long) = {
    session.range(range)
  }
}
