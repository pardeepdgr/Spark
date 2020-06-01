package com.learning.helper

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{current_timestamp, to_date, to_timestamp, unix_timestamp}

object Dates {

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

}
