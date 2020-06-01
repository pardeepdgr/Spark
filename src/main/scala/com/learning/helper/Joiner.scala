package com.learning.helper

import org.apache.spark.sql.DataFrame

object Joiner {

  /** If join type is not mentioned then Spark api calls inner join under the hood */
  def equiJoin(left: DataFrame, right: DataFrame, columnName: String): DataFrame = {
    left.join(right, left(columnName) === right(columnName))
  }

  def innerJoin(left: DataFrame, right: DataFrame, columnName: String): DataFrame = {
    left.join(right, left(columnName) === right(columnName), "inner")
  }

  /** Join column will only appear once in the output. Use aliasing while doing self-join */
  def naturalJoin(left: DataFrame, right: DataFrame, columnName: String): DataFrame = {
    left.join(right, columnName)
  }

  /** left, left_outer are alias */
  def leftJoin(left: DataFrame, right: DataFrame, columnName: String): DataFrame = {
    left.join(right, left(columnName) === right(columnName), "left")
  }

  /** right, right_outer are alias */
  def rightJoin(left: DataFrame, right: DataFrame, columnName: String): DataFrame = {
    left.join(right, left(columnName) === right(columnName), "right")
  }

  /** outer, full, full_outer are alias */
  def fullJoin(left: DataFrame, right: DataFrame, columnName: String): DataFrame = {
    left.join(right, left(columnName) === right(columnName), "full")
  }

  /** shows only records which are present in both left and right table */
  def leftSemiJoin(left: DataFrame, right: DataFrame, columnName: String): DataFrame = {
    left.join(right, left(columnName) === right(columnName), "left_semi")
  }

  /** shows only records which are in left table but NOT present in the right table */
  def leftAntiJoin(left: DataFrame, right: DataFrame, columnName: String): DataFrame = {
    left.join(right, left(columnName) === right(columnName), "left_anti")
  }

  def broadcastJoin(fatDf: DataFrame, leanDf: DataFrame, columnName: String): DataFrame = {
    if(fatDf.count() < leanDf.count())
      throw new IllegalArgumentException
    import org.apache.spark.sql.functions.broadcast
    fatDf.join(broadcast(leanDf), columnName)
  }

  /** spark.sql.crossJoin.enabled setting need to be set true */
  def crossJoin(left: DataFrame, right: DataFrame, columnName: String): DataFrame = {
    left.join(right)
  }

}
