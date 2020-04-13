package com.learning.helper

import base.TestBootstrap
import base.TestHelper.readCSV
import base.TestSetup.{init, kill, session}
import com.learning.helper.DataFrameComparator._

class DataFrameComparatorTest extends TestBootstrap {
  private val AIRLINES = "src/test/resources/airlines/airlines.csv"
  private val AIRPORTS = "src/test/resources/airlines/airports.csv"
  private val PLAYERS = "src/test/resources/soccer/player.csv"

  before {
    init("DataFrameComparatorTest", "local")
  }

  "DataFrameComparator" should "return true if columns name is same in both data frames" in {
    val areColumnsSame = compareColumnsName(readCSV(session, AIRLINES), readCSV(session, AIRPORTS))
    assert(areColumnsSame, "Columns name are not same")
  }

  it should "return false if columns name is not same in data frames" in {
    val areColumnsSame = !compareColumnsName(readCSV(session, AIRLINES), readCSV(session, PLAYERS))
    assert(areColumnsSame, "Columns name are same")
  }

  it should "return true if data types are same in both data frames" in {
    val isColumnsTypeSame = compareDataTypes(readCSV(session, AIRLINES), readCSV(session, AIRPORTS))
    assert(isColumnsTypeSame, "Columns name are not same")
  }

  it should "return false if data types are not same in both data frames" in {
    val isColumnsTypeSame = !compareDataTypes(readCSV(session, AIRLINES), readCSV(session, PLAYERS))
    assert(isColumnsTypeSame, "Columns name are not same")
  }

  it should "return true if content is same in both data frames" in {
    val isColumnsTypeSame = compareContent(readCSV(session, AIRLINES), readCSV(session, AIRLINES))
    assert(isColumnsTypeSame, "Content of both data frames is same")
  }

  it should "return false if content is not same in data frames" in {
    val isColumnsTypeSame = !compareContent(readCSV(session, AIRLINES), readCSV(session, AIRPORTS))
    assert(isColumnsTypeSame, "Content of both data frames is same")
  }

  after {
    kill
  }

}
