package com.learning.bikes

import com.learning.bikes.enumeration.Bike.Derived._
import com.learning.bikes.enumeration.Bike.{CustomerNumber, Timestamp}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, dayofyear, hour, weekofyear}

class CustomerAverageAggregator {

  private val DATE = "date"

  def findHourlyAggregatedCustomers(bikes: DataFrame): DataFrame = {

    val windowSpec = Window
      .partitionBy(CustomerNumber, Hour)
      .orderBy(Timestamp)

    bikes
      .withColumn(Hour, hour(col(Timestamp)))
      .withColumn(HourlyAggCustomer, count(CustomerNumber).over(windowSpec))
  }

  def findDailyAggregatedCustomers(bikes: DataFrame): DataFrame = {

    val windowSpec = Window
      .partitionBy(CustomerNumber, Day)
      .orderBy(Timestamp)

    bikes
      .withColumn(Day, dayofyear(col(Timestamp)))
      .withColumn(DailyAggCustomer, count(CustomerNumber).over(windowSpec))
  }

  def findWeeklyAggregatedCustomers(bikes: DataFrame): DataFrame = {

    val windowSpec = Window
      .partitionBy(CustomerNumber, Week)
      .orderBy(Timestamp)

    bikes
      .withColumn(Week, weekofyear(col(Timestamp)))
      .withColumn(WeeklyAggCustomer, count(CustomerNumber).over(windowSpec))
  }

}
