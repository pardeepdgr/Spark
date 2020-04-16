package com.learning.helper

import base.TestBootstrap
import org.apache.spark.sql.DataFrame
import base.TestSetup.{init, kill, session}
import com.learning.helper.DataFrameComparator._
import com.learning.helper.DataFrameCreator.fromCsv

class DataFrameComparatorTest extends TestBootstrap {
  private val AIRLINES = "src/test/resources/airlines/airlines.csv"
  private val AIRPORTS = "src/test/resources/airlines/airports.csv"
  private val PLAYERS = "src/test/resources/soccer/player.csv"

  private var airlines: DataFrame = _
  private var airports: DataFrame = _
  private var players: DataFrame = _

  before {
    init("DataFrameComparatorTest", "local")
    airlines = fromCsv(session, AIRLINES)
    airports = fromCsv(session, AIRPORTS)
    players = fromCsv(session, PLAYERS)
  }

  "DataFrameComparator" should "return true if columns name is same in both data frames" in {
    val areColumnsSame = compareColumnsName(airlines, airports)
    assert(areColumnsSame, "Columns name are not same")
  }

  it should "return false if columns name is not same in data frames" in {
    val areColumnsSame = !compareColumnsName(airlines, players)
    assert(areColumnsSame, "Columns name are same")
  }

  it should "return true if data types are same in both data frames" in {
    val isColumnsTypeSame = compareDataTypes(airlines, airlines)
    assert(isColumnsTypeSame, "Columns name are not same")
  }

  it should "return false if data types are not same in both data frames" in {
    val isColumnsTypeSame = !compareDataTypes(airlines, players)
    assert(isColumnsTypeSame, "Columns name are not same")
  }

  it should "return true if content is same in both data frames" in {
    val isColumnsTypeSame = compareContent(airlines, airlines)
    assert(isColumnsTypeSame, "Content of both data frames is same")
  }

  it should "return false if content is not same in data frames" in {
    val isColumnsTypeSame = !compareContent(airlines, airports)
    assert(isColumnsTypeSame, "Content of both data frames is same")
  }

  after {
    kill
  }

}
