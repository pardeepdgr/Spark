package com.learning.bikes

import com.learning.bikes.enumeration.Bike.{CustomerNumber, Hour, HourlyAggCustomer, Time}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, hour}

class CustomerAverageAggregator {

  def findHourlyAggregatedCustomers(bikes: DataFrame): DataFrame = {

    val windowSpec = Window
      .partitionBy(CustomerNumber, Hour)
      .orderBy(Time)

    bikes
      .withColumn(Hour, hour(col(Time)))
      .withColumn(HourlyAggCustomer, count(CustomerNumber).over(windowSpec))
  }

}
