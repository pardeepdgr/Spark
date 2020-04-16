package com.learning.df

import org.apache.spark.sql.DataFrame

object SoccerTalentScout {

  def getAverageOfVitalStats(playerAttributes: DataFrame): DataFrame = {
    playerAttributes
      .groupBy("player_api_id")
      .avg("finishing", "shot_power", "acceleration")
  }

  def getWeightedStats(vitalStats: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions.round
    vitalStats
      .withColumn("striker_grade",
        round(vitalStats("avg(finishing)") * 2
          + vitalStats("avg(shot_power)") * 2
          + vitalStats("avg(acceleration)") * 1, 2))
      .drop("avg(finishing)", "avg(shot_power)", "avg(acceleration)")
  }

  def getTopStrikers(strikers: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions.desc
    strikers
      .filter(strikers("striker_grade") > 100)
      .orderBy(desc("striker_grade"))
  }

  def getTopStrikersDetail(players: DataFrame, topStrikers: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions.broadcast
    players
      .select("player_api_id", "player_name") // NOTE: for performance, first select what all column required then do join
      .join(broadcast(topStrikers), "player_api_id")
  }

}
