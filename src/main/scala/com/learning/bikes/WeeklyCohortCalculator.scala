package com.learning.bikes

import com.learning.bikes.enumeration.Bike.CustomerNumber
import com.learning.bikes.enumeration.Bike.Derived.Week
import com.learning.helper.DataFrameCreator
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

class WeeklyCohortCalculator(session: SparkSession, bikes: DataFrame) {
  private val DELIMITER = ","
  private val NULL = ",null"

  def calculate(weekNumber: Int, numberOfWeeks: Int): DataFrame = {
    var records = ArrayBuffer[String]()
    val aggregator = new CustomerAverageAggregator()
    val bikesWithWeek = aggregator.findWeeklyAggregatedCustomers(bikes)

    getCohortAnalysis
    getLastRecordOfCohortAnalysis

    def getCohortAnalysis = {
      for (i <- 0 until numberOfWeeks - 1) {
        val currentWeekCustomers = bikesWithWeek
          .filter(bikesWithWeek(Week) === (weekNumber + i))
          .select(CustomerNumber)
          .distinct()
        val nextWeekCustomers = bikesWithWeek
          .filter(bikesWithWeek(Week) === (weekNumber + i + 1))
          .select(CustomerNumber)
          .distinct()

        val recurrentCustomers = nextWeekCustomers.except(currentWeekCustomers)
        val recurrentCustomersCount = nextWeekCustomers.count() - recurrentCustomers.count()

        var record = (weekNumber + i).toString
        for (_ <- 0 until i) record = record + NULL
        record = record + DELIMITER + currentWeekCustomers.count() + DELIMITER + recurrentCustomersCount

        if ((i + 2) < numberOfWeeks) {
          for (j <- (i + 2) to numberOfWeeks - 1) {
            val fartherWeekCustomers = bikesWithWeek
              .filter(bikesWithWeek(Week) === (weekNumber + j))
              .select(CustomerNumber)
              .distinct()

            val fartherRecurrentCustomers = fartherWeekCustomers.except(currentWeekCustomers)
            val fartherRecurrentCustomersCount = fartherWeekCustomers.count() - fartherRecurrentCustomers.count()

            record = record + DELIMITER + fartherRecurrentCustomersCount
          }
        }
        records += record
      }
    }

    def getLastRecordOfCohortAnalysis = {
      val lastWeekCustomers = bikesWithWeek
        .filter(bikesWithWeek(Week) === (weekNumber + numberOfWeeks - 1))
        .select(CustomerNumber)
        .distinct()

      var record = (weekNumber + numberOfWeeks - 1).toString
      for (_ <- 0 until numberOfWeeks - 1) record = record + NULL
      records += record + DELIMITER + lastWeekCustomers.count()
    }

    DataFrameCreator.fromStrings(session, records, DELIMITER, getDynamicSchema(numberOfWeeks))
  }

  //TODO: make it dynamic
  private def getDynamicSchema(numberOfWeeks: Int): StructType = {
    new StructType()
      .add(StructField("week_num", StringType, true))
      .add(StructField("week1", StringType, true))
      .add(StructField("week2", StringType, true))
      .add(StructField("week3", StringType, true))
  }

}
