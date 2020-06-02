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
      .withColumn("avg_price", avg(PRICE).over(windowSpec))
  }

  def findPriceDifferenceFromCostliestInCategory(products: DataFrame): DataFrame = {
    val allProducts = products
      .na.fill("Mobile") // automatically infer column-type and replace null for all string type columns
      .na.fill(0) // replace null for all number type columns with 0

    val windowSpec = Window
      .partitionBy(CATEGORY)
      .orderBy(desc(PRICE))
      .rangeBetween(Int.MinValue, Int.MaxValue)

    allProducts
      .withColumn("cheaper_than_costliest_in_category", max(PRICE).over(windowSpec) - allProducts(PRICE))
  }

  def findDuplicateProducts(products: DataFrame): DataFrame = {

    val windowSpec = Window
      .partitionBy(PRODUCT)
      .orderBy(desc(PRODUCT))

    products
      .withColumn("prev_products", lag(PRODUCT, 1).over(windowSpec))
      .filter(col(PRODUCT) === col("prev_products"))
      .drop(col("prev_products"))
  }

}
