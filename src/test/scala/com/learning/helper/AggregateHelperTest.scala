package com.learning.helper

import base.TestBootstrap
import base.TestSetup.{init, session}
import com.learning.creator.DataFrameCreator.fromCsv
import org.apache.spark.sql.DataFrame

class AggregateHelperTest extends TestBootstrap {

  private val PLAYERS = "src/test/resources/soccer/player_activity.csv"

  private var players: DataFrame = _

  before {
    init("AggregateHelperTest", "local")
    players = fromCsv(session, PLAYERS)
  }

  behavior of "AggregateHelperTest"

  it should "should calculate all activity points of all users" in {
    AggregateHelper.findAllActivityPoints(players).show()
  }

  it should "should calculate all activity points of a users" in {
    AggregateHelper.findActivityPointsOfEach(players).show()
  }

  it should "should find number of activities of each player" in {
    AggregateHelper.findNumberOfActivityOfEach(players).show()
  }

}
