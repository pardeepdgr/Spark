package com.learning.window

import base.TestBootstrap
import base.TestSetup.{init, kill, session}
import com.learning.helper.DataFrameCreator.fromCsv
import org.apache.spark.sql.DataFrame
import window.CustomWindow

class CustomWindowsTest extends TestBootstrap {

  private val PRODUCTS = "src/test/resources/products/product.csv"
  private var products: DataFrame = _

  before {
    init("CustomWindowsTest", "local")
    products = fromCsv(session, PRODUCTS)
  }

  it should "get category wise rank for all products in descending price" in {
    CustomWindow.getCategorizedRank(products);
  }

  it should "get moving average price for current and previous product" in {
    CustomWindow.getMovingAverage(products);
  }

  it should "find how much product is cheaper from its costliest product in the category" in {
    CustomWindow.findPriceDifferenceFromCostliestInCategory(products);
  }

  after {
    kill
  }

}
