package com.learning.creator

import org.apache.spark.SparkContext
import org.apache.spark.sql.Row

object RDDCreator {

  def fromCsv(sparkContext: SparkContext, path: String) = {
    sparkContext.textFile(path)
  }

  def fromRows(sparkContext: SparkContext, data: Array[Row]) = {
    sparkContext.parallelize(data)
  }
}
