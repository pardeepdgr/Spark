package com.learning.window

import base.TestBootstrap
import base.TestSetup.{init, kill, session}
import com.learning.creator.DataFrameCreator.fromCsv
import org.apache.spark.sql.DataFrame

class WindowsTestOperation extends TestBootstrap {

  private val PRODUCTS = "src/test/resources/products/product.csv"
  private val BIKES = "src/test/resources/bikes/raw/bikes.csv"

  private var products: DataFrame = _
  private var bikes: DataFrame = _

  before {
    init("WindowsTestOperation", "local")
    products = fromCsv(session, PRODUCTS)
    bikes = fromCsv(session, BIKES)
  }

  it should "get category wise rank for all products in descending price" in {
    WindowOperation.getCategorizedRank(products)
  }

  it should "get moving average price for current and previous product" in {
    WindowOperation.getMovingAverage(products)
  }

  it should "find first two costliest product in the category" in {
    WindowOperation.findFirstTwoCostliestFromCategory(products).show(false)
  }

  it should "find how much product is cheaper from its costliest product in the category" in {
    WindowOperation.findPriceDifferenceFromCostliestInCategory(products)
  }

  it should "find duplicate products" in {
    WindowOperation.findDuplicateProducts(products)
  }

  it should "create time buckets for everyday with one hour sliding duration and window start after one minute" in {
    WindowOperation.timeWindowBucketing(bikes.filter(bikes("number") === "14626")).show(100, false)
  }

  after {
    kill
  }

}
