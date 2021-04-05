package com.learning.helper

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, sum}

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

}
