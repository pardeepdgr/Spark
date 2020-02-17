package com.learning.df

import com.learning.helper.DataFrameAssistant
import com.learning.helper.DataFrameCreator
import com.learning.spark.SparkInstance.{session, sparkContext}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.LongAccumulator

object SoccerTalentScout {
  val PLAYER_ATTRIBUTE_DATASET_PATH = "src/main/resources/soccer/player_attributes.csv"
  val PLAYER_DATASET_PATH = "src/main/resources/soccer/player.csv"

  def main(args: Array[String]): Unit = {
    val players = DataFrameCreator.fromCsv(session, PLAYER_DATASET_PATH)
    val playerAttributes = DataFrameCreator.fromCsv(session, PLAYER_ATTRIBUTE_DATASET_PATH)

    val vitalStats: DataFrame = getAverageOfVitalStats(playerAttributes)
    val strikers: DataFrame = getWeightedStats(vitalStats)
    val topStrikers: DataFrame = filterTopStrikersInDescOrder(strikers)

    DataFrameAssistant.showPhysicalAndLogicalPlan(topStrikers)

    val topStrikerPlayersWithDuplicateId: DataFrame = DataFrameAssistant.equiJoin(players, topStrikers, "player_api_id")
    val topStrikerPlayersWithoutDuplicateId: DataFrame = DataFrameAssistant.naturalJoin(players, topStrikers, "player_api_id")

    topStrikerPlayersBroadcastJoin(players, topStrikers)
    playerHeadingAccuracyUsingAccumulator(topStrikerPlayersWithoutDuplicateId)

  }

  private def playerHeadingAccuracyUsingAccumulator(topStrikers: DataFrame) = {
    //Use AccumulatorV2 to create user defined accumulators
    val shortCount = sparkContext.longAccumulator("Counter for short players")
    val mediumCount = sparkContext.longAccumulator("Counter for average height players")
    val tallCount = sparkContext.longAccumulator("Counter for tall players")

    topStrikers.foreach(record => countPlayersHeight(record, shortCount, mediumCount, tallCount))
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

  private def topStrikerPlayersBroadcastJoin(players: DataFrame, topStrikers: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions.broadcast
    players
      .select("player_api_id", "player_name") // NOTE: while join, first select what all column required then do join
      .join(broadcast(topStrikers), "player_api_id")
  }

  private def filterTopStrikersInDescOrder(strikers: DataFrame) = {
    import org.apache.spark.sql.functions.desc
    strikers
      .filter(strikers("striker_grade") > 100)
      .orderBy(desc("striker_grade"))
  }

  private def getWeightedStats(vitalStats: DataFrame) = {
    import org.apache.spark.sql.functions.round
    vitalStats
      .withColumn("striker_grade",
        round(vitalStats("avg(finishing)") * 2
          + vitalStats("avg(shot_power)") * 2
          + vitalStats("avg(acceleration)") * 1, 2))
      .drop("avg(finishing)", "avg(shot_power)", "avg(acceleration)")
  }

  private def getAverageOfVitalStats(playerAttributes: DataFrame) = {
    playerAttributes
      .groupBy("player_api_id")
      .avg("finishing", "shot_power", "acceleration")
  }
}
