package com.learning.helper

import org.apache.spark.sql.DataFrame

object DataFrameComparator {

  def compareColumnsName(df1: DataFrame, df2: DataFrame): Boolean = {
    val c1: Array[String] = sortByColumn(df1).columns
    val c2: Array[String] = sortByColumn(df2).columns

    /** FYI: sameElements api won't work on nested arrays use deep for that
     * scala> Array(Array(11), Array(21, 22)).deep == Array(Array(11), Array(21, 22)).deep
     * res0: Boolean = true */
    c1.sameElements(c2)
  }

  def compareDataTypes(df1: DataFrame, df2: DataFrame): Boolean = {
    val d1: Array[String] = getDataTypes(sortByColumn(df1))
    val d2: Array[String] = getDataTypes(sortByColumn(df2))

    d1.sameElements(d2)
  }

  def compareContent(df1: DataFrame, df2: DataFrame): Boolean = {
    val d1: DataFrame = distinctWithCount(sortByColumn(df1))
    val d2: DataFrame = distinctWithCount(sortByColumn(df2))

    diff(d1, d2) == 0
  }

  private def diff(df1: DataFrame, df2: DataFrame): Long =
    df1.union(df2).except(df1.intersect(df2)).count()

  private def distinctWithCount(df: DataFrame): DataFrame = {
    val columnNames = df.columns
    df.groupBy(columnNames.head, columnNames.tail: _*).count() // : _* tells the compiler to pass each element as its own argument
  }

  private def sortByColumn(df: DataFrame): DataFrame = {
    val sortedColumnsName: Array[String] = df.columns.sorted
    df.select(sortedColumnsName.head, sortedColumnsName.tail: _*)
  }

  private def getDataTypes(df: DataFrame): Array[String] =
    df.schema.fields.map(x => x.dataType.toString)

}
