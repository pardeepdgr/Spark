package com.learning.bikes

import java.text.SimpleDateFormat
import java.util.Date

import com.learning.bikes.enumeration.Bike.CustomerNumber
import com.learning.bikes.enumeration.Bike.Derived.{Week, WeekNumber}
import com.learning.helper.DataFrameCreator
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

class WeeklyCohortCalculator(session: SparkSession, bikes: DataFrame) {
  private val YEAR_PATTERN = "YYYY"
  private val WEEK_PATTERN = "w"
  private val DELIMITER = ","
  private val EMPTY_PLACEHOLDER = ",null"

  def calculate(date: Date, numberOfWeeks: Int): DataFrame = {
    val weekNumber = getWeekNumber(date)
    this.calculate(weekNumber, numberOfWeeks)
  }

  def calculate(weekNumber: Int, numberOfWeeks: Int): DataFrame = {
    val aggregator = new CustomerAverageAggregator()
    val bikesWithWeek = aggregator.findWeeklyAggregatedCustomers(bikes)

    var records = ArrayBuffer[String]()
    records = computeCohortAnalysis(weekNumber, numberOfWeeks, bikesWithWeek)
    records = records ++ computeLastRecord(weekNumber, numberOfWeeks, bikesWithWeek)

    DataFrameCreator.fromStrings(session, records, getDynamicSchema(numberOfWeeks))
  }

  private def getWeekNumber(date: Date): Int = {
    val yearFormat = new SimpleDateFormat(YEAR_PATTERN)
    val year: Int = Integer.parseInt(yearFormat.format(date))

    val weekFormat = new SimpleDateFormat(WEEK_PATTERN)
    val week: Int = Integer.parseInt(weekFormat.format(date))

    Integer.parseInt(year + "" + week)
  }

  private def computeCohortAnalysis(weekNumber: Int, numberOfWeeks: Int, bikesWithWeek: DataFrame): ArrayBuffer[String] = {
    var records = ArrayBuffer[String]()

    for (i <- 0 until numberOfWeeks - 1) {
      val currentWeekCustomers = bikesWithWeek
        .filter(bikesWithWeek(Week) === (weekNumber + i)).select(CustomerNumber).distinct()
      val nextWeekCustomers = bikesWithWeek
        .filter(bikesWithWeek(Week) === (weekNumber + i + 1)).select(CustomerNumber).distinct()

      val recurrentCustomers = nextWeekCustomers.except(currentWeekCustomers)
      val recurrentCustomersCount = nextWeekCustomers.count() - recurrentCustomers.count()

      var record = (weekNumber + i).toString
      for (_ <- 0 until i) record = record + EMPTY_PLACEHOLDER
      record = record + DELIMITER + currentWeekCustomers.count() + DELIMITER + recurrentCustomersCount

      if ((i + 2) < numberOfWeeks)
        for (j <- (i + 2) to numberOfWeeks - 1) {
          val fartherWeekCustomers = bikesWithWeek
            .filter(bikesWithWeek(Week) === (weekNumber + j)).select(CustomerNumber).distinct()

          val fartherRecurrentCustomers = fartherWeekCustomers.except(currentWeekCustomers)
          val fartherRecurrentCustomersCount = fartherWeekCustomers.count() - fartherRecurrentCustomers.count()

          record = record + DELIMITER + fartherRecurrentCustomersCount
        }
      records = records :+ record
    }
    records
  }

  private def computeLastRecord(weekNumber: Int, numberOfWeeks: Int, bikesWithWeek: DataFrame): ArrayBuffer[String] = {
    val lastWeekCustomers = bikesWithWeek
      .filter(bikesWithWeek(Week) === (weekNumber + numberOfWeeks - 1))
      .select(CustomerNumber)
      .distinct()

    var records = ArrayBuffer[String]()
    var record = (weekNumber + numberOfWeeks - 1).toString
    for (_ <- 0 until numberOfWeeks - 1) record = record + EMPTY_PLACEHOLDER
    records = records :+ (record + DELIMITER + lastWeekCustomers.count())
    records
  }

  private def getDynamicSchema(numberOfWeeks: Int): StructType = {
    var fields = ArrayBuffer[StructField]()

    fields = fields :+ StructField(WeekNumber, StringType, false)
    for (weekNumber <- 1 until numberOfWeeks + 1)
      fields = fields :+ StructField(Week + weekNumber, StringType, true)

    new StructType(fields.toArray)
  }

}
