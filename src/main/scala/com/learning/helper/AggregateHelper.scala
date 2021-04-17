package com.learning.helper

import com.learning.helper.DateHelper.{diffInHour, toTimestamp}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object AggregateHelper {

  def findAllActivityPoints(players: DataFrame): DataFrame = {
    players.groupBy().agg(sum(col("activity_point")))
    // or
    players.groupBy().agg(sum("activity_point"))
    // or
    players.agg(sum(col("activity_point"))) // as agg on a DataFrame/Dataset is simply a shortcut for groupBy().agg(…​)
  }

  def findActivityPointsOfEach(players:DataFrame):DataFrame = {
    players.groupBy(col("player_id")).agg(sum(col("activity_point")))
    // or
    players.groupBy("player_id").sum("activity_point")
  }

  def findNumberOfActivityOfEach(players:DataFrame):DataFrame = {
    players.groupBy("player_id").count()
  }

  def findHoursFromDateString_withTwoAgg(df: DataFrame): DataFrame = {
    df
      .withColumn("duration", diffInHour(
        toTimestamp(col("EndDate"), col("EndTime")),
        toTimestamp(col("StartDate"), col("StartTime")))
      )
      .groupBy("cpid")
      .agg(
        round(avg("duration"), 2).alias("avg_duration"),
        round(max("duration"), 2).alias("max_duration")
      )
      .withColumnRenamed("cpid", "chargepoint_id")
      .select("chargepoint_id", "avg_duration", "max_duration")
  }

}
