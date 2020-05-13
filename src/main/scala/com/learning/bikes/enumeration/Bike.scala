package com.learning.bikes.enumeration

object Bike extends Enumeration {

  val CustomerNumber = Value("number").toString
  val Timestamp = Value("ts").toString
  val PickUpLatitude = Value("pick_lat").toString
  val PickUpLongitude = Value("pick_lng").toString
  val DropLatitude = Value("drop_lat").toString
  val DropLongitude = Value("drop_lng").toString

  object Derived extends Enumeration {
    val Hour = Value("hour").toString
    val HourlyAggCustomer = Value("hourly_agg_customer").toString
    val Day = Value("day").toString
    val DailyAggCustomer = Value("daily_agg_customer").toString
    val Week = Value("week").toString
    val WeeklyAggCustomer = Value("weekly_agg_customer").toString
  }
}
