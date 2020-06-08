package com.learning.helper

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{array, array_contains}

object DataFrameAssistant {

  def showPhysicalPlan(dataFrame: DataFrame) = {
    dataFrame.explain()
  }

  def showPhysicalAndLogicalPlan(dataFrame: DataFrame) = {
    val isLogicalPlanEnabled = true
    dataFrame.explain(isLogicalPlanEnabled)
  }

  def castColumnTo(df: DataFrame, columnName: String, datatype: String): DataFrame = {
    df.withColumn(columnName, df(columnName).cast(datatype))
  }

  def doesValueExistsInArrayCol(col: Column, value: String): Column = {
    array_contains(col, value)
  }

  def combineTwoColumns(df: DataFrame, col: Array[String]): DataFrame = {
    df.withColumn("col_arr", array(df(col(0)), df(col(1)))) // or struct api can be used
  }

}
