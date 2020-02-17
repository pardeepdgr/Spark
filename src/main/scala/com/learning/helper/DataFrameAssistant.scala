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

  def equiJoin(firstDf: DataFrame, secondDf: DataFrame, columnName: String) = {
    firstDf.join(secondDf, firstDf(columnName) === secondDf(columnName))
  }

  def naturalJoin(firstDf: DataFrame, secondDf: DataFrame, columnName: String) = {
    firstDf.join(secondDf, columnName)
  }

  def broadcastJoin(fatDf: DataFrame, leanDf: DataFrame, columnName: String): DataFrame = {
    import org.apache.spark.sql.functions.broadcast
    fatDf.join(broadcast(leanDf), columnName)
  }
}
