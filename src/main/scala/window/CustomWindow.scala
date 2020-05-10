package window

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.rank

object CustomWindow {

  private val CATEGORY = "Category"
  private val PRICE = "Price"

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

    val windowSpec = Window
      .partitionBy(CATEGORY)
      .orderBy(desc(PRICE))
      .rangeBetween(Int.MinValue, Int.MaxValue)

    products
      .withColumn("cheaper_than_costliest_in_category", max(PRICE).over(windowSpec) - products(PRICE))
  }

}
