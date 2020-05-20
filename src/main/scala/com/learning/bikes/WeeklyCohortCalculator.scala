package com.learning.bikes

import java.text.SimpleDateFormat
import java.util.Date

import com.learning.bikes.enumeration.Bike.CustomerNumber
import com.learning.bikes.enumeration.Bike.Derived.Week
import com.learning.helper.DataFrameCreator
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

class WeeklyCohortCalculator(session: SparkSession, bikes: DataFrame) {
  private val DELIMITER = ","
  private val NULL = ",null"

  def calculate(date: Date, numberOfWeeks: Int): DataFrame = {
    val weekNumber = getWeekNumber(date)
    this.calculate(weekNumber, numberOfWeeks)
  }

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

  private def getWeekNumber(date: Date): Int = {
    val yearFormat = new SimpleDateFormat("YYYY")
    val year: Int = Integer.parseInt(yearFormat.format(date))

    val weekFormat = new SimpleDateFormat("w")
    val week: Int = Integer.parseInt(weekFormat.format(date))

    Integer.parseInt(year + "" + week)
  }

  private def getDynamicSchema(numberOfWeeks: Int): StructType = {
    var fields = Array[StructField]()

    fields = fields :+(StructField("week_num", StringType, true))
    for (i <- 1 until numberOfWeeks + 1)
      fields = fields :+ StructField("week" + i, StringType, true)

    new StructType(fields)
  }

}
