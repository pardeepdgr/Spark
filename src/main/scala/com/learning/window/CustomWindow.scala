package com.learning.window

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, desc, lag, max, rank}

object CustomWindow {

  private val CATEGORY = "Category"
  private val PRICE = "Price"
  private val PRODUCT = "Product"

  def getCategorizedRank(products: DataFrame): DataFrame = {

    val windowSpec = Window
      .partitionBy(CATEGORY)
      .orderBy(desc(PRICE))

    products
      .na.drop("all") // "all" means where every columns in a row is null remove it
      .withColumn("rank", rank.over(windowSpec))
  }

  private val CURRENT_ROW = 0
  private val PREVIOUS_ROW = -1

  def getMovingAverage(products: DataFrame): DataFrame = {

    val windowSpec = Window
      .partitionBy(CATEGORY)
      .orderBy(desc(PRICE))
      .rowsBetween(PREVIOUS_ROW, CURRENT_ROW)

    products
      .na.drop() // if no args passed then by default it's "any" which means wherever null is remove that row
      .withColumn("avg_price", avg(PRICE).over(windowSpec))
  }

  def findPriceDifferenceFromCostliestInCategory(products: DataFrame): DataFrame = {

    val cleansedProducts = products
      .na.fill("Mobile") // auto-infer column-type then replaces null for all string-columns with Mobile
      .na.fill(0) // replaces null for all number-columns with 0

    val windowSpec = Window
      .partitionBy(CATEGORY)
      .orderBy(desc(PRICE))
      .rangeBetween(Int.MinValue, Int.MaxValue)

    cleansedProducts
      .withColumn("cheaper_than_costliest_in_category", max(PRICE).over(windowSpec) - cleansedProducts(PRICE))
  }

  private val TEMP_COL = "prev_products"

  def findDuplicateProducts(products: DataFrame): DataFrame = {

    val windowSpec = Window
      .partitionBy(PRODUCT)
      .orderBy(desc(PRODUCT))

    products
      .withColumn(TEMP_COL, lag(PRODUCT, 1).over(windowSpec))
      .filter(col(PRODUCT) === col(TEMP_COL))
      .drop(col(TEMP_COL))
  }

}
