package com.learning.df

import com.learning.spark.SparkInstance.sparkContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.LongAccumulator

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

  //Use AccumulatorV2 to create user defined accumulators
  def getTallTopStrikers(topStrikers: DataFrame): LongAccumulator = {
    val shortCount = sparkContext.longAccumulator("Counter for short players")
    val mediumCount = sparkContext.longAccumulator("Counter for average height players")
    val tallCount = sparkContext.longAccumulator("Counter for tall players")

    topStrikers.foreach(record => countPlayersHeight(record, shortCount, mediumCount, tallCount))
    tallCount
  }

  private def countPlayersHeight(record: Row,
                                 shortCount: LongAccumulator,
                                 mediumCount: LongAccumulator,
                                 tallCount: LongAccumulator): Unit = {
    val height: Double = record.getAs[Double]("height")
    if (height <= 175)
      shortCount.add(1)
    else if (height > 175 & height < 195)
      mediumCount.add(1)
    else if (height > 195)
      tallCount.add(1)
  }

}
