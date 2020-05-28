package com.learning.sql

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

case class SparkSqlAnalyzer(session: SparkSession) {

  def registerDataFrameAsViewForCurrentSession(dataFrame: DataFrame): DataFrame = {
    val VIEW_NAME = "flights_dist"
    val QUERY = s"select flight_number, destination, distance from $VIEW_NAME"

    dataFrame.createOrReplaceTempView(VIEW_NAME)

    session.sqlContext.sql(QUERY)
      .filter(col("distance") > lit(2500))
  }

  def registerDataFrameAsViewForAllSessions(dataFrame: DataFrame): DataFrame = {
    val GLOBAL_TEMP_DB = "global_temp"
    val VIEW_NAME = "flights_orig"
    val QUERY = s"select flight_number, origin, destination from $GLOBAL_TEMP_DB.$VIEW_NAME"

    dataFrame.createOrReplaceGlobalTempView(VIEW_NAME)

    session.sqlContext.sql(QUERY)
      .filter(col("origin") === lit("JFK"))
  }

}
