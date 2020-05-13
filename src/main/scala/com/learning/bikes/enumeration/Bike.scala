package com.learning.bikes.enumeration

object Bike extends Enumeration {

  val CustomerNumber = Value("number").toString
  val Time = Value("ts").toString
  val PickUpLatitude = Value("pick_lat").toString
  val PickUpLongitude = Value("pick_lng").toString
  val DropLatitude = Value("drop_lat").toString
  val DropLongitude = Value("drop_lng").toString

  val Hour = Value("hour").toString
  val HourlyAggCustomer = Value("hourly_agg_customer").toString
}
