package com.learning.helper

import org.apache.spark.sql.DataFrame

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

}
