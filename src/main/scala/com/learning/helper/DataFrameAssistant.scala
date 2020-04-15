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
    if(fatDf.count() < leanDf.count())
      throw new IllegalArgumentException
    import org.apache.spark.sql.functions.broadcast
    fatDf.join(broadcast(leanDf), columnName)
  }

  def castColumnTo(df: DataFrame, columnName: String, datatype: String): DataFrame = {
    df.withColumn(columnName, df(columnName).cast(datatype))
  }

  def registerDataFrameAsView(dataFrame: DataFrame, viewName: String): Unit = {
    dataFrame.createOrReplaceTempView(viewName)
  }

  def registerDataFrameAsViewForAllSparkSessionInCluster(dataFrame: DataFrame, viewName: String): Unit = {
    dataFrame.createGlobalTempView(viewName)
  }
}
