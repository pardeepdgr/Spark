package com.learning.helper

import org.apache.spark.sql.DataFrame

object JoinHelper {

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

  /** Combines DataFrames with same number of columns regardless of column name and duplicate data.
   * If number of columns are not the same it returns an AnalysisException.
   * To enforce matching column name use unionByName but column name should be same otherwise AnalysisException.*/
  def union(left: DataFrame, right: DataFrame): DataFrame = {
    left.union(right)
  }

  /** spark.sql.crossJoin.enabled setting need to be set true */
  def crossJoin(left: DataFrame, right: DataFrame, columnName: String): DataFrame = {
    left.join(right)
  }

  def joinWithoutCommonColumn(left:DataFrame, right: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions.{col, monotonically_increasing_id}

    val df1 = left.withColumn("m_id", monotonically_increasing_id)
    val df2 = right.withColumn("m_id", monotonically_increasing_id)

    df1
      .join(df2, col("m_id"))
      .drop("m_id")
  }

}
