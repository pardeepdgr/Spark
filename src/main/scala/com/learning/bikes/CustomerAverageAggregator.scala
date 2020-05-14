package com.learning.bikes

import com.learning.bikes.enumeration.Bike.Derived._
import com.learning.bikes.enumeration.Bike.{CustomerNumber, Timestamp}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{concat, col, count, dayofyear, hour, month, weekofyear, year}

class CustomerAverageAggregator {

  def findHourlyAggregatedCustomers(bikes: DataFrame): DataFrame = {

    val windowSpec = Window
      .partitionBy(CustomerNumber, Hour)
      .orderBy(Timestamp)

    bikes
      .withColumn(Hour, concat(year(col(Timestamp)), hour(col(Timestamp))))
      .withColumn(HourlyAggCustomer, count(CustomerNumber).over(windowSpec))
  }

  def findDailyAggregatedCustomers(bikes: DataFrame): DataFrame = {

    val windowSpec = Window
      .partitionBy(CustomerNumber, Day)
      .orderBy(Timestamp)

    bikes
      .withColumn(Day, concat(year(col(Timestamp)), dayofyear(col(Timestamp))))
      .withColumn(DailyAggCustomer, count(CustomerNumber).over(windowSpec))
  }

  def findWeeklyAggregatedCustomers(bikes: DataFrame): DataFrame = {

    val windowSpec = Window
      .partitionBy(CustomerNumber, Week)
      .orderBy(Timestamp)

    bikes
      .withColumn(Week, concat(year(col(Timestamp)), weekofyear(col(Timestamp))))
      .withColumn(WeeklyAggCustomer, count(CustomerNumber).over(windowSpec))
  }

  def findMonthlyAggregatedCustomers(bikes: DataFrame): DataFrame = {

    val windowSpec = Window
      .partitionBy(CustomerNumber, Month)
      .orderBy(Timestamp)

    bikes
      .withColumn(Month, concat(year(col(Timestamp)), month(col(Timestamp))))
      .withColumn(MonthlyAggCustomer, count(CustomerNumber).over(windowSpec))
  }

}
