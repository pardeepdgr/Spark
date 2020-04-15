package com.learning.sql

import com.learning.helper.{DataFrameAssistant, DataFrameCreator}
import com.learning.spark.SparkInstance.{session, sqlContext}
import org.apache.spark.sql.DataFrame

object FlightAnalyzer {
  val FLIGHT_DATASET_PATH = "src/main/resources/airlines/flights.csv"

  def main(args: Array[String]): Unit = {
    val flights: DataFrame = DataFrameCreator.fromCsv(session, FLIGHT_DATASET_PATH)
    val viewName = "flights"

    DataFrameAssistant.registerDataFrameAsView(flights, viewName)

    val sqlMixedWithDf: DataFrame = sqlContext.sql("select flight_number, destination, distance from flights")
    sqlMixedWithDf.filter(sqlMixedWithDf("distance") > 2500)
  }
}
