package com.learning.helper

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

object DateHelper {

  def currentEpoch(): Column = {
    unix_timestamp
  }

  def toEpoch(dateString: Column): Column = {
    toEpoch(dateString, "")
  }

  def toEpoch(dateString: Column, dateFormat: String): Column = {
    if (dateFormat != null && !dateFormat.isEmpty)
      unix_timestamp(dateString, dateFormat)
    else
      unix_timestamp(dateString)
  }

  def toDate(timestampString: Column): Column = {
      toDate(timestampString, "")
  }

  def toDate(dateString: Column, dateFormat: String): Column = {
    if (dateFormat != null && !dateFormat.isEmpty)
      to_date(dateString, dateFormat)
    else
      to_date(dateString)
  }

  def currentTimestamp(): Column = {
    current_timestamp
  }

  def toTimestamp(timestampString: Column): Column = {
    to_timestamp(timestampString)
  }

  /** date = 2021-04-17 from csv it automatically get converted to 2021-04-17 00:00:00 due to inferSchema option
   * time = 01:01:00 */
  def toTimestamp(date: Column, time: Column): Column = {
    to_timestamp(concat(substring(date, 1, 11), time))
  }

  /* timestamp format 2021-04-17 00:00:00 */
  def diffInHour(endTimeStamp: Column, startTimeStamp: Column): Column = {
    (endTimeStamp.cast(LongType) - startTimeStamp.cast(LongType)) / 3600
  }

}
