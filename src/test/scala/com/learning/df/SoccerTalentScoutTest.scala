package com.learning.df

import base.TestBootstrap
import base.TestSetup.{init, kill, session}
import com.learning.df.SoccerTalentScout._
import com.learning.helper.JoinHelper.naturalJoin
import com.learning.creator.DataFrameCreator.fromCsv
import org.apache.spark.sql.DataFrame

class SoccerTalentScoutTest extends TestBootstrap {

  private val PLAYER = "src/test/resources/soccer/player.csv"
  private val PLAYER_ATTRIBUTE = "src/test/resources/soccer/player_attributes.csv"

  private var players: DataFrame = _
  private var playerAttributes: DataFrame = _

  before {
    init("SoccerTalentScoutTest", "local")
    players = fromCsv(session, PLAYER)
    playerAttributes = fromCsv(session, PLAYER_ATTRIBUTE)
  }

  it should "calculate average of all vital stats of all players" in {
    val vitalStats: DataFrame = getAverageOfVitalStats(playerAttributes)
    vitalStats.show()
  }

  it should "calculate striker grade of all players" in {
    val vitalStats: DataFrame = getAverageOfVitalStats(playerAttributes)
    val strikers: DataFrame = getWeightedStats(vitalStats)
    strikers.show()
  }

  it should "find top strikers" in {
    val vitalStats: DataFrame = getAverageOfVitalStats(playerAttributes)
    val strikers: DataFrame = getWeightedStats(vitalStats)
    val topStrikers: DataFrame = getTopStrikers(strikers)
    topStrikers.show()
  }

  it should "get top strikers detail" in {
    val vitalStats: DataFrame = getAverageOfVitalStats(playerAttributes)
    val strikers: DataFrame = getWeightedStats(vitalStats)
    val topStrikers: DataFrame = getTopStrikers(strikers)
    val topStrikersDetail: DataFrame = naturalJoin(players, topStrikers, "player_api_id")
    topStrikersDetail.show()
  }

  it should "get top strikers detail with performance" in {
    val vitalStats: DataFrame = getAverageOfVitalStats(playerAttributes)
    val strikers: DataFrame = getWeightedStats(vitalStats)
    val topStrikers: DataFrame = getTopStrikers(strikers)
    val topStrikersDetail: DataFrame = getTopStrikersDetail(players, topStrikers)
    topStrikersDetail.show()
  }

  it should "get number of top striker who are tall" in {
    val vitalStats: DataFrame = getAverageOfVitalStats(playerAttributes)
    val strikers: DataFrame = getWeightedStats(vitalStats)
    val topStrikers: DataFrame = getTopStrikers(strikers)
    val topStrikersDetail: DataFrame = naturalJoin(players, topStrikers, "player_api_id")
    val tallStrikersCount = getTallTopStrikers(topStrikersDetail)
    assert(tallStrikersCount.value == 207, "Tall top strikers count mismatch")
  }

  after {
    kill
  }

}
